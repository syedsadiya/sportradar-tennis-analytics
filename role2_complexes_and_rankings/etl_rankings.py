"""Game Analytics: Unlocking Tennis Data with SportRadar API.

Role 2: Competitors & Rankings ETL Module
File: role2_complexes_and_rankings/etl_rankings.py

This module handles:
1. Extraction of double competitors rankings data from Sportradar Tennis v3 API.
2. Exponential backoff and graceful error handling for HTTP 429 / 403 / network errors.
3. Local raw JSON caching in data/raw/double_competitors_rankings.json to preserve API quota.
4. Normalization of nested rankings -> competitor_rankings -> competitor structures.
5. Idempotent database loading via SQLAlchemy and MySQL ON DUPLICATE KEY UPDATE.
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
logger = logging.getLogger("ETL-Rankings")

# Resolve repository paths for standalone or integrated execution
CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent if CURRENT_DIR.name.startswith("role") else CURRENT_DIR
DATA_DIR = REPO_ROOT / "data" / "raw"
CACHE_FILE = DATA_DIR / "double_competitors_rankings.json"

# Load environment variables
load_dotenv(REPO_ROOT / ".env")
load_dotenv(CURRENT_DIR / ".env")

API_ENDPOINT = "https://api.sportradar.com/tennis/trial/v3/en/double_competitors_rankings.json"
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


def fetch_rankings_data(
    api_key: Optional[str] = None,
    max_retries: int = 4,
    backoff_factor: float = 2.0,
) -> Dict[str, Any]:
    """Fetch raw rankings payload from Sportradar API with exponential backoff.

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

    if not key or key == "your_sportradar_api_key_here":
        logger.info("No active Sportradar API key found. Attempting to load cached rankings data.")
        return load_cached_raw_data()

    headers = {
        "x-api-key": key,
        "accept": "application/json",
    }

    url = API_ENDPOINT
    delay = 1.5

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Requesting rankings data from Sportradar API (Attempt %d/%d)...", attempt, max_retries)
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 200:
                data = response.json()
                logger.info("Successfully fetched rankings payload from API.")
                try:
                    with open(CACHE_FILE, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2)
                    logger.info("Cached raw rankings payload to %s", CACHE_FILE)
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
    """Load cached raw rankings JSON from data/raw/double_competitors_rankings.json.

    Returns:
        Parsed JSON dictionary.

    Raises:
        FileNotFoundError: If the cache file does not exist.
    """
    if not CACHE_FILE.exists():
        msg = f"Raw cached data not found at {CACHE_FILE}. Please provide a valid SPORTRADAR_API_KEY."
        logger.error(msg)
        raise FileNotFoundError(msg)

    logger.info("Loading rankings data from local cache: %s", CACHE_FILE)
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def transform_rankings(
    raw_data: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize raw JSON data into clean records for competitors and competitor_rankings.

    Extracts competitor attributes and their corresponding weekly ranking performance.

    Args:
        raw_data: Raw JSON dictionary returned from Sportradar or cache.

    Returns:
        Tuple of (competitors_list, rankings_list).
    """
    rankings_blocks = raw_data.get("rankings", [])
    competitors_dict: Dict[str, Dict[str, Any]] = {}
    rankings_dict: Dict[str, Dict[str, Any]] = {}

    for block in rankings_blocks:
        comp_rankings = block.get("competitor_rankings", [])
        for entry in comp_rankings:
            competitor = entry.get("competitor") or {}
            c_id = competitor.get("id")
            if not c_id:
                continue

            c_id_str = str(c_id)[:50]
            name = competitor.get("name") or "Unknown Competitor"
            country = competitor.get("country") or "Unknown Country"
            country_code = (competitor.get("country_code") or "UNK").upper()[:3]
            abbreviation = (competitor.get("abbreviation") or (name[:3].upper() if name else "UNK"))[:10]

            if c_id_str not in competitors_dict:
                competitors_dict[c_id_str] = {
                    "competitor_id": c_id_str,
                    "name": str(name)[:100],
                    "country": str(country)[:100],
                    "country_code": str(country_code).ljust(3, "X")[:3],
                    "abbreviation": str(abbreviation),
                }

            # Normalize ranking metrics
            try:
                rank_val = int(entry.get("rank", 999))
            except (ValueError, TypeError):
                rank_val = 999

            try:
                movement_val = int(entry.get("movement", 0))
            except (ValueError, TypeError):
                movement_val = 0

            try:
                points_val = int(entry.get("points", 0))
            except (ValueError, TypeError):
                points_val = 0

            try:
                competitions_played_val = int(entry.get("competitions_played", 0))
            except (ValueError, TypeError):
                competitions_played_val = 0

            rankings_dict[c_id_str] = {
                "competitor_id": c_id_str,
                "rank": rank_val,
                "movement": movement_val,
                "points": points_val,
                "competitions_played": competitions_played_val,
            }

    competitors_list = list(competitors_dict.values())
    rankings_list = list(rankings_dict.values())
    # Sort rankings by rank ascending
    rankings_list.sort(key=lambda r: r["rank"])

    return competitors_list, rankings_list


def load_rankings_to_db(
    engine: Engine,
    competitors: List[Dict[str, Any]],
    rankings: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Perform idempotent upserts for competitors and rankings into MySQL.

    Args:
        engine: SQLAlchemy Engine connected to MySQL.
        competitors: Normalized competitor dicts.
        rankings: Normalized ranking dicts referencing competitors.

    Returns:
        Summary dict containing counts of processed competitors and rankings.
    """
    summary = {"competitors_processed": len(competitors), "rankings_processed": len(rankings)}

    competitor_upsert_sql = text("""
        INSERT INTO competitors (competitor_id, name, country, country_code, abbreviation)
        VALUES (:competitor_id, :name, :country, :country_code, :abbreviation)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            country = VALUES(country),
            country_code = VALUES(country_code),
            abbreviation = VALUES(abbreviation);
    """)

    ranking_upsert_sql = text("""
        INSERT INTO competitor_rankings (`rank`, movement, points, competitions_played, competitor_id)
        VALUES (:rank, :movement, :points, :competitions_played, :competitor_id)
        ON DUPLICATE KEY UPDATE
            `rank` = VALUES(`rank`),
            movement = VALUES(movement),
            points = VALUES(points),
            competitions_played = VALUES(competitions_played);
    """)

    with engine.begin() as conn:
        logger.info("Upserting %d competitors into MySQL...", len(competitors))
        for comp in competitors:
            conn.execute(competitor_upsert_sql, comp)

        logger.info("Upserting %d competitor rankings into MySQL...", len(rankings))
        for rk in rankings:
            conn.execute(ranking_upsert_sql, rk)

    logger.info("Successfully loaded competitors and rankings into the database.")
    return summary


def run_rankings_etl(
    engine: Optional[Engine] = None,
    api_key: Optional[str] = None,
) -> Dict[str, int]:
    """Execute the end-to-end Competitor Rankings ETL pipeline.

    Args:
        engine: Optional pre-configured SQLAlchemy engine.
        api_key: Optional Sportradar API key.

    Returns:
        Summary dictionary with processing metrics.
    """
    logger.info("Starting Role 2 Competitor Rankings ETL pipeline...")
    db_engine = engine or get_database_engine()

    raw_data = fetch_rankings_data(api_key=api_key)
    competitors, rankings = transform_rankings(raw_data)
    summary = load_rankings_to_db(db_engine, competitors, rankings)

    logger.info("Role 2 Competitor Rankings ETL completed successfully: %s", summary)
    return summary


if __name__ == "__main__":
    try:
        run_rankings_etl()
    except Exception as exc:
        logger.exception("Rankings ETL failed: %s", exc)
        sys.exit(1)
