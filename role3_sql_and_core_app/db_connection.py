"""Game Analytics: Unlocking Tennis Data with SportRadar API.

Role 3: Database Connection & Query Execution Engine
File: role3_sql_and_core_app/db_connection.py

This module manages:
1. SQLAlchemy Engine lifecycle with Streamlit @st.cache_resource caching.
2. Query execution with @st.cache_data for high performance and low database load.
3. Health check / ping connectivity tests with descriptive diagnostic feedback.
4. Pre-loaded catalog of all 20 required analytical project queries for the interactive SQL Runner.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("DB-Connection")

# Resolve repository paths
CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent if CURRENT_DIR.name.startswith("role") else CURRENT_DIR

# Load environment configuration
load_dotenv(REPO_ROOT / ".env")
load_dotenv(CURRENT_DIR / ".env")

DEFAULT_DB_URL = "mysql+pymysql://root:root@localhost:3306/sportradar_tennis"


def _build_engine(db_url: Optional[str] = None) -> Engine:
    """Internal factory to create a SQLAlchemy engine."""
    connection_string = db_url or os.getenv("DATABASE_URL", DEFAULT_DB_URL)
    return create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
    )


if HAS_STREAMLIT:
    @st.cache_resource(show_spinner=False)
    def get_db_engine(db_url: Optional[str] = None) -> Engine:
        """Cached database engine provider for Streamlit sessions."""
        return _build_engine(db_url)
else:
    def get_db_engine(db_url: Optional[str] = None) -> Engine:
        """Database engine provider for non-Streamlit environments."""
        return _build_engine(db_url)


def test_connection(engine: Optional[Engine] = None) -> Tuple[bool, str]:
    """Test connectivity to the database and verify presence of key tables.

    Args:
        engine: Optional SQLAlchemy engine to test.

    Returns:
        Tuple of (is_connected: bool, message: str).
    """
    eng = engine or get_db_engine()
    try:
        with eng.connect() as conn:
            result = conn.execute(text("SELECT 1;")).scalar()
            if result == 1:
                # Check tables
                tables_res = conn.execute(text("SHOW TABLES;")).fetchall()
                tables = [t[0] for t in tables_res]
                return True, f"Connected to MySQL. Verified {len(tables)} tables: {', '.join(tables)}"
        return False, "Database responded with unexpected result."
    except Exception as exc:
        logger.error("Database connection check failed: %s", exc)
        return False, f"Connection Failed: {str(exc)}"


def _run_query(sql: str, params: Optional[Dict[str, Any]] = None, engine: Optional[Engine] = None) -> pd.DataFrame:
    """Execute raw SQL query and return a pandas DataFrame."""
    eng = engine or get_db_engine()
    with eng.connect() as conn:
        df = pd.read_sql_query(text(sql), conn, params=params or {})
    return df


if HAS_STREAMLIT:
    @st.cache_data(ttl=60, show_spinner=False)
    def execute_cached_query(sql: str, params_tuple: Optional[tuple] = None) -> pd.DataFrame:
        """Execute query with Streamlit data caching (60s TTL)."""
        params = dict(params_tuple) if params_tuple else None
        return _run_query(sql, params)
else:
    def execute_cached_query(sql: str, params_tuple: Optional[tuple] = None) -> pd.DataFrame:
        params = dict(params_tuple) if params_tuple else None
        return _run_query(sql, params)


def execute_query(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """Public helper to execute an SQL statement without cache or with immediate evaluation.

    Args:
        sql: SQL query text.
        params: Optional dictionary of query parameters.

    Returns:
        pandas DataFrame of results.
    """
    return _run_query(sql, params)


def get_all_20_queries() -> List[Dict[str, str]]:
    """Return catalog of all 20 analytical queries with titles and SQL text."""
    return [
        {
            "id": 1,
            "section": "Competitions & Categories",
            "title": "Query 1: List all competitions along with their category name",
            "description": "Lists tournaments and joins them with their governing category (ATP, WTA, ITF).",
            "sql": (
                "SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_name "
                "FROM competitions c "
                "INNER JOIN categories cat ON c.category_id = cat.category_id "
                "ORDER BY cat.category_name ASC, c.competition_name ASC;"
            ),
        },
        {
            "id": 2,
            "section": "Competitions & Categories",
            "title": "Query 2: Count the number of competitions in each category",
            "description": "Aggregates total tournaments hosted under each tennis circuit category.",
            "sql": (
                "SELECT cat.category_id, cat.category_name, COUNT(c.competition_id) AS total_competitions "
                "FROM categories cat "
                "LEFT JOIN competitions c ON cat.category_id = c.category_id "
                "GROUP BY cat.category_id, cat.category_name "
                "ORDER BY total_competitions DESC, cat.category_name ASC;"
            ),
        },
        {
            "id": 3,
            "section": "Competitions & Categories",
            "title": "Query 3: Find all competitions of type 'doubles'",
            "description": "Filters competitions formatted specifically for doubles tournament play.",
            "sql": (
                "SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_name "
                "FROM competitions c "
                "INNER JOIN categories cat ON c.category_id = cat.category_id "
                "WHERE LOWER(c.type) = 'doubles' "
                "ORDER BY c.competition_name ASC;"
            ),
        },
        {
            "id": 4,
            "section": "Competitions & Categories",
            "title": "Query 4: Get competitions that belong to a specific category ('ITF Men')",
            "description": "Filters competitions belonging strictly to the ITF Men circuit.",
            "sql": (
                "SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_name "
                "FROM competitions c "
                "INNER JOIN categories cat ON c.category_id = cat.category_id "
                "WHERE cat.category_name = 'ITF Men' "
                "ORDER BY c.competition_name ASC;"
            ),
        },
        {
            "id": 5,
            "section": "Competitions & Categories",
            "title": "Query 5: Identify parent competitions and their sub-competitions (self-join)",
            "description": "Uses self-referencing hierarchy on competitions to match main events with sub-events.",
            "sql": (
                "SELECT p.competition_id AS parent_competition_id, p.competition_name AS parent_competition_name, "
                "s.competition_id AS sub_competition_id, s.competition_name AS sub_competition_name, "
                "s.type AS sub_competition_type, s.gender AS sub_competition_gender "
                "FROM competitions s "
                "INNER JOIN competitions p ON s.parent_id = p.competition_id "
                "ORDER BY p.competition_name ASC, s.competition_name ASC;"
            ),
        },
        {
            "id": 6,
            "section": "Competitions & Categories",
            "title": "Query 6: Analyze the distribution of competition types by category",
            "description": "Cross-tabulates competition formats (singles/doubles/mixed) per circuit category.",
            "sql": (
                "SELECT cat.category_name, c.type AS competition_type, COUNT(c.competition_id) AS competition_count "
                "FROM categories cat "
                "INNER JOIN competitions c ON cat.category_id = c.category_id "
                "GROUP BY cat.category_name, c.type "
                "ORDER BY cat.category_name ASC, competition_count DESC;"
            ),
        },
        {
            "id": 7,
            "section": "Competitions & Categories",
            "title": "Query 7: List all competitions with no parent (top-level competitions)",
            "description": "Finds flagship tournaments that sit at the root level (parent_id IS NULL).",
            "sql": (
                "SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_name "
                "FROM competitions c "
                "INNER JOIN categories cat ON c.category_id = cat.category_id "
                "WHERE c.parent_id IS NULL "
                "ORDER BY cat.category_name ASC, c.competition_name ASC;"
            ),
        },
        {
            "id": 8,
            "section": "Complexes & Venues",
            "title": "Query 8: List all venues along with their associated complex name",
            "description": "Joins individual courts and stadiums with their parent facility.",
            "sql": (
                "SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, v.timezone, c.complex_name "
                "FROM venues v "
                "INNER JOIN complexes c ON v.complex_id = c.complex_id "
                "ORDER BY c.complex_name ASC, v.venue_name ASC;"
            ),
        },
        {
            "id": 9,
            "section": "Complexes & Venues",
            "title": "Query 9: Count the number of venues in each complex",
            "description": "Computes total court capacity for each tennis complex.",
            "sql": (
                "SELECT c.complex_id, c.complex_name, COUNT(v.venue_id) AS total_venues "
                "FROM complexes c "
                "LEFT JOIN venues v ON c.complex_id = v.complex_id "
                "GROUP BY c.complex_id, c.complex_name "
                "ORDER BY total_venues DESC, c.complex_name ASC;"
            ),
        },
        {
            "id": 10,
            "section": "Complexes & Venues",
            "title": "Query 10: Get details of venues in a specific country ('Chile')",
            "description": "Filters stadiums and venues located in Chile.",
            "sql": (
                "SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, v.country_code, v.timezone, c.complex_name "
                "FROM venues v "
                "INNER JOIN complexes c ON v.complex_id = c.complex_id "
                "WHERE v.country_name = 'Chile' "
                "ORDER BY v.city_name ASC, v.venue_name ASC;"
            ),
        },
        {
            "id": 11,
            "section": "Complexes & Venues",
            "title": "Query 11: Identify all venues and their timezones",
            "description": "Lists all stadiums along with their localized timezone offset.",
            "sql": (
                "SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, v.timezone "
                "FROM venues v "
                "ORDER BY v.timezone ASC, v.venue_name ASC;"
            ),
        },
        {
            "id": 12,
            "section": "Complexes & Venues",
            "title": "Query 12: Find complexes that have more than one venue (HAVING COUNT(*) > 1)",
            "description": "Identifies multi-court championship grounds with more than one venue.",
            "sql": (
                "SELECT c.complex_id, c.complex_name, COUNT(v.venue_id) AS venue_count "
                "FROM complexes c "
                "INNER JOIN venues v ON c.complex_id = v.complex_id "
                "GROUP BY c.complex_id, c.complex_name "
                "HAVING COUNT(v.venue_id) > 1 "
                "ORDER BY venue_count DESC, c.complex_name ASC;"
            ),
        },
        {
            "id": 13,
            "section": "Complexes & Venues",
            "title": "Query 13: List venues grouped by country",
            "description": "Summarizes infrastructure by country with a comma-delimited court list.",
            "sql": (
                "SELECT v.country_name, v.country_code, COUNT(v.venue_id) AS venue_count, "
                "GROUP_CONCAT(v.venue_name ORDER BY v.venue_name SEPARATOR ', ') AS venue_list "
                "FROM venues v "
                "GROUP BY v.country_name, v.country_code "
                "ORDER BY venue_count DESC, v.country_name ASC;"
            ),
        },
        {
            "id": 14,
            "section": "Complexes & Venues",
            "title": "Query 14: Find all venues for a specific complex ('Nacional')",
            "description": "Performs pattern matching on complex name for 'Nacional'.",
            "sql": (
                "SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, v.timezone, c.complex_name "
                "FROM venues v "
                "INNER JOIN complexes c ON v.complex_id = c.complex_id "
                "WHERE c.complex_name LIKE '%Nacional%' "
                "ORDER BY v.venue_name ASC;"
            ),
        },
        {
            "id": 15,
            "section": "Competitors & Rankings",
            "title": "Query 15: Get all competitors with their rank and points",
            "description": "Full world doubles rankings leaderboard.",
            "sql": (
                "SELECT r.rank, c.competitor_id, c.name AS competitor_name, c.country, c.country_code, "
                "c.abbreviation, r.points, r.movement, r.competitions_played "
                "FROM competitor_rankings r "
                "INNER JOIN competitors c ON r.competitor_id = c.competitor_id "
                "ORDER BY r.rank ASC;"
            ),
        },
        {
            "id": 16,
            "section": "Competitors & Rankings",
            "title": "Query 16: Find competitors ranked in the top 5",
            "description": "Filters the world top 5 elite doubles tennis players.",
            "sql": (
                "SELECT r.rank, c.name AS competitor_name, c.country, c.abbreviation, "
                "r.points, r.movement, r.competitions_played "
                "FROM competitor_rankings r "
                "INNER JOIN competitors c ON r.competitor_id = c.competitor_id "
                "WHERE r.rank <= 5 "
                "ORDER BY r.rank ASC;"
            ),
        },
        {
            "id": 17,
            "section": "Competitors & Rankings",
            "title": "Query 17: List competitors with no rank movement (movement = 0)",
            "description": "Identifies stable rankings with zero week-over-week position change.",
            "sql": (
                "SELECT r.rank, c.name AS competitor_name, c.country, r.points, r.movement, r.competitions_played "
                "FROM competitor_rankings r "
                "INNER JOIN competitors c ON r.competitor_id = c.competitor_id "
                "WHERE r.movement = 0 "
                "ORDER BY r.rank ASC;"
            ),
        },
        {
            "id": 18,
            "section": "Competitors & Rankings",
            "title": "Query 18: Get the total points of competitors from a specific country ('Croatia')",
            "description": "Aggregates total ranking points earned by Croatian competitors.",
            "sql": (
                "SELECT c.country, c.country_code, COUNT(c.competitor_id) AS total_players, "
                "SUM(r.points) AS total_points, ROUND(AVG(r.points), 2) AS average_points, "
                "MIN(r.rank) AS best_rank "
                "FROM competitors c "
                "INNER JOIN competitor_rankings r ON c.competitor_id = r.competitor_id "
                "WHERE c.country = 'Croatia' "
                "GROUP BY c.country, c.country_code;"
            ),
        },
        {
            "id": 19,
            "section": "Competitors & Rankings",
            "title": "Query 19: Count the number of competitors per country",
            "description": "Counts total players and aggregate ranking points by country.",
            "sql": (
                "SELECT c.country, c.country_code, COUNT(c.competitor_id) AS total_competitors, "
                "SUM(r.points) AS aggregate_points, ROUND(AVG(r.points), 2) AS average_points, "
                "MIN(r.rank) AS highest_rank "
                "FROM competitors c "
                "INNER JOIN competitor_rankings r ON c.competitor_id = r.competitor_id "
                "GROUP BY c.country, c.country_code "
                "ORDER BY total_competitors DESC, aggregate_points DESC;"
            ),
        },
        {
            "id": 20,
            "section": "Competitors & Rankings",
            "title": "Query 20: Find competitors with the highest points in the current week",
            "description": "Identifies competitor(s) possessing the maximum ranking points.",
            "sql": (
                "SELECT r.rank, c.name AS competitor_name, c.country, c.country_code, "
                "r.points, r.movement, r.competitions_played "
                "FROM competitor_rankings r "
                "INNER JOIN competitors c ON r.competitor_id = c.competitor_id "
                "WHERE r.points = (SELECT MAX(points) FROM competitor_rankings) "
                "ORDER BY r.rank ASC;"
            ),
        },
    ]
