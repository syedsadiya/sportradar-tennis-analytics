"""Game Analytics: Unlocking Tennis Data with SportRadar API.

Role 2: Complexes & Venues ETL Module
File: role2_complexes_and_rankings/etl_complexes.py

This module handles:
1. Extraction of complexes and venues data from Sportradar Tennis v3 API.
2. Exponential backoff and graceful error handling for HTTP 429 / 403 / network errors.
3. Local raw JSON caching in data/raw/complexes.json to preserve API quota.
4. Normalization of nested complexes and venues hierarchies with clean default values.
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
logger = logging.getLogger("ETL-Complexes")

# Resolve repository paths for standalone or integrated execution
CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent if CURRENT_DIR.name.startswith("role") else CURRENT_DIR
DATA_DIR = REPO_ROOT / "data" / "raw"
CACHE_FILE = DATA_DIR / "complexes.json"

# Load environment variables
load_dotenv(REPO_ROOT / ".env")
load_dotenv(CURRENT_DIR / ".env")

API_ENDPOINT = "https://api.sportradar.com/tennis/trial/v3/en/complexes.json"
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


def fetch_complexes_data(
    api_key: Optional[str] = None,
    max_retries: int = 4,
    backoff_factor: float = 2.0,
) -> Dict[str, Any]:
    """Fetch raw complexes and venues payload from Sportradar API with exponential backoff.

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
        logger.info("No active Sportradar API key found. Attempting to load cached complexes data.")
        return load_cached_raw_data()

    headers = {
        "x-api-key": key,
        "accept": "application/json",
    }

    url = API_ENDPOINT
    delay = 1.5

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Requesting complexes data from Sportradar API (Attempt %d/%d)...", attempt, max_retries)
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code == 200:
                data = response.json()
                logger.info("Successfully fetched %d complexes from API.", len(data.get("complexes", [])))
                try:
                    with open(CACHE_FILE, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2)
                    logger.info("Cached raw complexes payload to %s", CACHE_FILE)
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
    """Load cached raw complexes JSON from data/raw/complexes.json.

    Returns:
        Parsed JSON dictionary.

    Raises:
        FileNotFoundError: If the cache file does not exist.
    """
    if not CACHE_FILE.exists():
        msg = f"Raw cached data not found at {CACHE_FILE}. Please provide a valid SPORTRADAR_API_KEY."
        logger.error(msg)
        raise FileNotFoundError(msg)

    logger.info("Loading complexes data from local cache: %s", CACHE_FILE)
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def transform_complexes(
    raw_data: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize raw JSON data into clean records for complexes and venues.

    Applies clean default values to optional or missing strings to ensure
    all NOT NULL relational constraints succeed without failure.

    Args:
        raw_data: Raw JSON dictionary returned from Sportradar or cache.

    Returns:
        Tuple of (complexes_list, venues_list).
    """
    complexes_raw = raw_data.get("complexes", [])
    complexes_list: List[Dict[str, Any]] = []
    venues_list: List[Dict[str, Any]] = []
    seen_complex_ids = set()
    seen_venue_ids = set()

    for comp in complexes_raw:
        c_id = comp.get("id")
        if not c_id or c_id in seen_complex_ids:
            continue
        seen_complex_ids.add(c_id)

        c_name = comp.get("name") or "Unnamed Complex"
        complexes_list.append({
            "complex_id": str(c_id)[:50],
            "complex_name": str(c_name)[:100],
        })

        # Process nested venues
        for v in comp.get("venues", []):
            v_id = v.get("id")
            if not v_id or v_id in seen_venue_ids:
                continue
            seen_venue_ids.add(v_id)

            v_name = v.get("name") or "Main Court"
            city = v.get("city_name") or "Unknown City"
            country = v.get("country_name") or "Unknown Country"
            country_code = (v.get("country_code") or "UNK").upper()[:3]
            tz = v.get("timezone") or "UTC"

            venues_list.append({
                "venue_id": str(v_id)[:50],
                "venue_name": str(v_name)[:100],
                "city_name": str(city)[:100],
                "country_name": str(country)[:100],
                "country_code": str(country_code).ljust(3, "X")[:3],
                "timezone": str(tz)[:100],
                "complex_id": str(c_id)[:50],
            })

    return complexes_list, venues_list


def load_complexes_to_db(
    engine: Engine,
    complexes: List[Dict[str, Any]],
    venues: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Perform idempotent upserts for complexes and venues into MySQL.

    Args:
        engine: SQLAlchemy Engine connected to MySQL.
        complexes: Normalized complex dicts.
        venues: Normalized venue dicts referencing complexes.

    Returns:
        Summary dict containing counts of processed complexes and venues.
    """
    summary = {"complexes_processed": len(complexes), "venues_processed": len(venues)}

    complex_upsert_sql = text("""
        INSERT INTO complexes (complex_id, complex_name)
        VALUES (:complex_id, :complex_name)
        ON DUPLICATE KEY UPDATE
            complex_name = VALUES(complex_name);
    """)

    venue_upsert_sql = text("""
        INSERT INTO venues (venue_id, venue_name, city_name, country_name, country_code, timezone, complex_id)
        VALUES (:venue_id, :venue_name, :city_name, :country_name, :country_code, :timezone, :complex_id)
        ON DUPLICATE KEY UPDATE
            venue_name = VALUES(venue_name),
            city_name = VALUES(city_name),
            country_name = VALUES(country_name),
            country_code = VALUES(country_code),
            timezone = VALUES(timezone),
            complex_id = VALUES(complex_id);
    """)

    with engine.begin() as conn:
        logger.info("Upserting %d complexes into MySQL...", len(complexes))
        for c in complexes:
            conn.execute(complex_upsert_sql, c)

        logger.info("Upserting %d venues into MySQL...", len(venues))
        for v in venues:
            conn.execute(venue_upsert_sql, v)

    logger.info("Successfully loaded complexes and venues into the database.")
    return summary


def run_complexes_etl(
    engine: Optional[Engine] = None,
    api_key: Optional[str] = None,
) -> Dict[str, int]:
    """Execute the end-to-end Complexes & Venues ETL pipeline.

    Args:
        engine: Optional pre-configured SQLAlchemy engine.
        api_key: Optional Sportradar API key.

    Returns:
        Summary dictionary with processing metrics.
    """
    logger.info("Starting Role 2 Complexes & Venues ETL pipeline...")
    db_engine = engine or get_database_engine()

    raw_data = fetch_complexes_data(api_key=api_key)
    complexes, venues = transform_complexes(raw_data)
    summary = load_complexes_to_db(db_engine, complexes, venues)

    logger.info("Role 2 Complexes & Venues ETL completed successfully: %s", summary)
    return summary


if __name__ == "__main__":
    try:
        run_complexes_etl()
    except Exception as exc:
        logger.exception("Complexes ETL failed: %s", exc)
        sys.exit(1)
