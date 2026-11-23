"""Game Analytics: Unlocking Tennis Data with SportRadar API.

Role 1: Database & Competitions ETL Module
File: role1_database_and_competitions/etl_competitions.py

This module handles:
1. Extraction of competition and category data from Sportradar Tennis v3 API.
2. Exponential backoff and graceful error handling for HTTP 429 / 403 / network errors.
3. Local raw JSON caching in data/raw/competitions.json to preserve API quota.
4. Normalization into relational schemas (categories and competitions).
5. Topological sorting of parent competitions (parent_id IS NULL) ahead of sub-competitions
   to guarantee self-referencing foreign key integrity.
6. Idempotent database loading via SQLAlchemy and MySQL ON DUPLICATE KEY UPDATE.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ETL-Competitions")

# Resolve repository paths for standalone or integrated execution
CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent if CURRENT_DIR.name.startswith("role") else CURRENT_DIR
DATA_DIR = REPO_ROOT / "data" / "raw"
CACHE_FILE = DATA_DIR / "competitions.json"

# Load environment variables
load_dotenv(REPO_ROOT / ".env")
load_dotenv(CURRENT_DIR / ".env")

API_ENDPOINT = "https://api.sportradar.com/tennis/trial/v3/en/competitions.json"
DEFAULT_DB_URL = "mysql+pymysql://root:root@localhost:3306/sportradar_tennis"


def get_database_engine(db_url: Optional[str] = None) -> Engine:
    """Create and return a SQLAlchemy database engine.

    Args:
        db_url: Optional SQLAlchemy database connection string.

    Returns:
        SQLAlchemy Engine instance.
    """
    connection_string = db_url or os.getenv("DATABASE_URL", DEFAULT_DB_URL)
    return create_engine(connection_string, echo=False, pool_pre_ping=True)


def fetch_competitions_data(
    api_key: Optional[str] = None,
    max_retries: int = 4,
    backoff_factor: float = 2.0,
) -> Dict[str, Any]:
    """Fetch raw competitions payload from Sportradar API with exponential backoff.

    If the API key is missing or invalid, or if rate limits persist,
    this function falls back to locally cached raw JSON data.

    Args:
        api_key: Sportradar API key. Defaults to SPORTRADAR_API_KEY environment variable.
        max_retries: Maximum number of exponential backoff retry attempts for HTTP 429.
        backoff_factor: Multiplier for exponential backoff sleep delay.

    Returns:
        Parsed JSON dictionary response.
    """
    key = api_key or os.getenv("SPORTRADAR_API_KEY", "").strip()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Check if a valid API key is present
    if not key or key == "your_sportradar_api_key_here":
        logger.info("No active Sportradar API key found. Attempting to load cached data.")
        return load_cached_raw_data()

    headers = {
        "x-api-key": key,
        "accept": "application/json",
    }

    url = API_ENDPOINT
    delay = 1.5

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Requesting competitions data from Sportradar API (Attempt %d/%d)...", attempt, max_retries)
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 200:
                data = response.json()
                logger.info("Successfully fetched %d competitions from API.", len(data.get("competitions", [])))
                # Cache response locally to conserve quota
                try:
                    with open(CACHE_FILE, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2)
                    logger.info("Cached raw competitions payload to %s", CACHE_FILE)
                except OSError as write_err:
                    logger.warning("Failed to cache payload to disk: %s", write_err)
                return data

            if response.status_code == 429:
                logger.warning("HTTP 429 Rate Limit exceeded. Backing off for %.1f seconds...", delay)
                time.sleep(delay)
                delay *= backoff_factor
                continue

            if response.status_code in (401, 403):
                logger.error("Sportradar API Authentication failed (HTTP %d). Check your API key.", response.status_code)
                logger.info("Falling back to local raw cached JSON.")
                return load_cached_raw_data()

            response.raise_for_status()

        except requests.exceptions.RequestException as req_err:
            logger.warning("Network or request error: %s. Retrying in %.1f seconds...", req_err, delay)
            time.sleep(delay)
            delay *= backoff_factor

    logger.warning("Exhausted all API retry attempts. Falling back to cached payload.")
    return load_cached_raw_data()


def load_cached_raw_data() -> Dict[str, Any]:
    """Load cached raw competitions JSON from data/raw/competitions.json.

    Returns:
        Parsed JSON dictionary.

    Raises:
        FileNotFoundError: If the cache file does not exist.
    """
    if not CACHE_FILE.exists():
        msg = f"Raw cached data not found at {CACHE_FILE}. Please provide a valid SPORTRADAR_API_KEY."
        logger.error(msg)
        raise FileNotFoundError(msg)

    logger.info("Loading competitions data from local cache: %s", CACHE_FILE)
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def transform_competitions(
    raw_data: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize raw JSON data into clean records for categories and competitions.

    Sorts parent competitions (parent_id IS NULL) ahead of child sub-competitions
    so the self-referencing foreign key constraint never fails.

    Args:
        raw_data: Raw JSON dictionary returned from Sportradar or cache.

    Returns:
        Tuple of (categories_list, competitions_list).
    """
    competitions_raw = raw_data.get("competitions", [])
    categories_dict: Dict[str, Dict[str, Any]] = {}
    competitions_list: List[Dict[str, Any]] = []

    # First pass: collect categories and raw competition records
    valid_comp_ids = set()
    for item in competitions_raw:
        comp_id = item.get("id")
        if not comp_id:
            continue
        valid_comp_ids.add(comp_id)

        # Normalize category
        cat_info = item.get("category") or {}
        cat_id = cat_info.get("id") or "sr:category:unknown"
        cat_name = cat_info.get("name") or "General / Uncategorized"

        if cat_id not in categories_dict:
            categories_dict[cat_id] = {
                "category_id": str(cat_id)[:50],
                "category_name": str(cat_name)[:100],
            }

        # Normalize competition
        parent_id = item.get("parent_id")
        comp_name = item.get("name") or "Unnamed Competition"
        comp_type = item.get("type") or "singles"
        comp_gender = item.get("gender") or "mixed"

        competitions_list.append({
            "competition_id": str(comp_id)[:50],
            "competition_name": str(comp_name)[:100],
            "parent_id": str(parent_id)[:50] if parent_id else None,
            "type": str(comp_type)[:20],
            "gender": str(comp_gender)[:10],
            "category_id": str(cat_id)[:50],
        })

    # Validate parent_ids: if parent_id is not in the dataset, set to None
    for comp in competitions_list:
        if comp["parent_id"] and comp["parent_id"] not in valid_comp_ids:
            logger.debug(
                "Parent ID %s not found in current batch; setting to NULL for %s",
                comp["parent_id"],
                comp["competition_id"],
            )
            comp["parent_id"] = None

    # Sort competitions: parent competitions (parent_id IS None) come first
    # This guarantees self-referencing FK succeeds without constraint violation
    competitions_sorted = sorted(
        competitions_list,
        key=lambda x: (x["parent_id"] is not None, x["competition_name"]),
    )

    return list(categories_dict.values()), competitions_sorted


def load_competitions_to_db(
    engine: Engine,
    categories: List[Dict[str, Any]],
    competitions: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Perform idempotent upserts for categories and competitions into MySQL.

    Args:
        engine: SQLAlchemy Engine connected to MySQL.
        categories: Normalized category dicts.
        competitions: Normalized competition dicts (topologically sorted).

    Returns:
        Summary dict containing counts of processed categories and competitions.
    """
    summary = {"categories_processed": len(categories), "competitions_processed": len(competitions)}

    category_upsert_sql = text("""
        INSERT INTO categories (category_id, category_name)
        VALUES (:category_id, :category_name)
        ON DUPLICATE KEY UPDATE
            category_name = VALUES(category_name);
    """)

    competition_upsert_sql = text("""
        INSERT INTO competitions (competition_id, competition_name, parent_id, type, gender, category_id)
        VALUES (:competition_id, :competition_name, :parent_id, :type, :gender, :category_id)
        ON DUPLICATE KEY UPDATE
            competition_name = VALUES(competition_name),
            parent_id = VALUES(parent_id),
            type = VALUES(type),
            gender = VALUES(gender),
            category_id = VALUES(category_id);
    """)

    with engine.begin() as conn:
        logger.info("Upserting %d categories into MySQL...", len(categories))
        for cat in categories:
            conn.execute(category_upsert_sql, cat)

        logger.info("Upserting %d competitions into MySQL...", len(competitions))
        for comp in competitions:
            conn.execute(competition_upsert_sql, comp)

    logger.info("Successfully loaded categories and competitions into the database.")
    return summary


def run_competitions_etl(
    engine: Optional[Engine] = None,
    api_key: Optional[str] = None,
) -> Dict[str, int]:
    """Execute the end-to-end Competitions ETL pipeline.

    Args:
        engine: Optional pre-configured SQLAlchemy engine.
        api_key: Optional Sportradar API key.

    Returns:
        Summary dictionary with processing metrics.
    """
    logger.info("Starting Role 1 Competitions ETL pipeline...")
    db_engine = engine or get_database_engine()

    raw_data = fetch_competitions_data(api_key=api_key)
    categories, competitions = transform_competitions(raw_data)
    summary = load_competitions_to_db(db_engine, categories, competitions)

    logger.info("Role 1 Competitions ETL completed successfully: %s", summary)
    return summary


if __name__ == "__main__":
    try:
        run_competitions_etl()
    except Exception as exc:
        logger.exception("Competitions ETL failed: %s", exc)
        sys.exit(1)
