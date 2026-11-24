"""Game Analytics: Unlocking Tennis Data with SportRadar API.

Master ETL Orchestration Module
File: role2_complexes_and_rankings/run_all_etl.py

This module orchestrates:
1. Sequential execution of all three ETL pipelines:
   - Competitions & Categories (Role 1)
   - Complexes & Venues (Role 2)
   - Double Competitor Rankings (Role 2)
2. Enforces a 1.5-second sleep between API calls to strictly respect Sportradar trial rate limits.
3. Consolidates execution metrics and logs an audit report of all records processed.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("Master-ETL-Orchestrator")

# Ensure role module directories are in python search path
CURRENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CURRENT_DIR.parent if CURRENT_DIR.name.startswith("role") else CURRENT_DIR

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Load environment configuration
load_dotenv(REPO_ROOT / ".env")

# Import individual ETL entrypoints
try:
    from role1_database_and_competitions.etl_competitions import run_competitions_etl
    from role2_complexes_and_rankings.etl_complexes import run_complexes_etl
    from role2_complexes_and_rankings.etl_rankings import run_rankings_etl
except ImportError:
    # Fallback if imported from within role directory
    sys.path.insert(0, str(REPO_ROOT / "role1_database_and_competitions"))
    sys.path.insert(0, str(REPO_ROOT / "role2_complexes_and_rankings"))
    from etl_competitions import run_competitions_etl  # type: ignore
    from etl_complexes import run_complexes_etl  # type: ignore
    from etl_rankings import run_rankings_etl  # type: ignore

RATE_LIMIT_SLEEP_SECONDS = float(os.getenv("RATE_LIMIT_SLEEP", "1.5"))
DEFAULT_DB_URL = "sqlite:///data/tennis.db"


def initialize_sqlite_schema(engine: Engine) -> None:
    """Create the SQLite tables before running any ETL stage."""
    if engine.dialect.name != "sqlite":
        return
    schema_path = REPO_ROOT / "role1_database_and_competitions" / "sqlite_schema.sql"
    if not schema_path.exists():
        raise FileNotFoundError(f"SQLite schema not found: {schema_path}")
    with engine.begin() as conn:
        for statement in schema_path.read_text(encoding="utf-8").split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))


def run_pipeline() -> Dict[str, Any]:
    """Execute all three Sportradar ETL pipelines with rate-limit pauses.

    Returns:
        Consolidated dictionary of results and metrics.
    """
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("INITIALIZING MASTER SPORTRADAR TENNIS ETL PIPELINE")
    logger.info("Rate limit throttle between calls: %.2f seconds", RATE_LIMIT_SLEEP_SECONDS)
    logger.info("=" * 70)

    db_url = os.getenv("DATABASE_URL", DEFAULT_DB_URL)
    shared_engine: Engine = create_engine(db_url, echo=False, pool_pre_ping=True)
    initialize_sqlite_schema(shared_engine)

    consolidated_results: Dict[str, Any] = {
        "status": "SUCCESS",
        "stages": {},
        "errors": [],
    }

    # -------------------------------------------------------------------------
    # Stage 1: Competitions & Categories ETL (Role 1)
    # -------------------------------------------------------------------------
    try:
        logger.info("[STAGE 1/3] Executing Competitions & Categories ETL...")
        comp_summary = run_competitions_etl(engine=shared_engine)
        consolidated_results["stages"]["competitions"] = comp_summary
        logger.info("[STAGE 1/3] Competitions ETL finished successfully.")
    except Exception as exc:
        logger.exception("Stage 1 failed: %s", exc)
        consolidated_results["status"] = "PARTIAL_ERROR"
        consolidated_results["errors"].append({"stage": "competitions", "error": str(exc)})

    # Sleep between API calls to respect Sportradar trial tier rate limits (1 request/sec limit)
    logger.info("Pausing %.2f seconds to respect trial API rate limits...", RATE_LIMIT_SLEEP_SECONDS)
    time.sleep(RATE_LIMIT_SLEEP_SECONDS)

    # -------------------------------------------------------------------------
    # Stage 2: Complexes & Venues ETL (Role 2)
    # -------------------------------------------------------------------------
    try:
        logger.info("[STAGE 2/3] Executing Complexes & Venues ETL...")
        complex_summary = run_complexes_etl(engine=shared_engine)
        consolidated_results["stages"]["complexes"] = complex_summary
        logger.info("[STAGE 2/3] Complexes ETL finished successfully.")
    except Exception as exc:
        logger.exception("Stage 2 failed: %s", exc)
        consolidated_results["status"] = "PARTIAL_ERROR"
        consolidated_results["errors"].append({"stage": "complexes", "error": str(exc)})

    # Sleep between API calls
    logger.info("Pausing %.2f seconds to respect trial API rate limits...", RATE_LIMIT_SLEEP_SECONDS)
    time.sleep(RATE_LIMIT_SLEEP_SECONDS)

    # -------------------------------------------------------------------------
    # Stage 3: Competitor Rankings ETL (Role 2)
    # -------------------------------------------------------------------------
    try:
        logger.info("[STAGE 3/3] Executing Competitor Rankings ETL...")
        rank_summary = run_rankings_etl(engine=shared_engine)
        consolidated_results["stages"]["rankings"] = rank_summary
        logger.info("[STAGE 3/3] Rankings ETL finished successfully.")
    except Exception as exc:
        logger.exception("Stage 3 failed: %s", exc)
        consolidated_results["status"] = "PARTIAL_ERROR"
        consolidated_results["errors"].append({"stage": "rankings", "error": str(exc)})

    elapsed = time.time() - start_time
    consolidated_results["duration_seconds"] = round(elapsed, 2)

    logger.info("=" * 70)
    logger.info("MASTER ETL PIPELINE FINISHED IN %.2f SECONDS", elapsed)
    logger.info("Overall Status: %s", consolidated_results["status"])
    logger.info("Metrics Summary: %s", consolidated_results["stages"])
    if consolidated_results["errors"]:
        logger.warning("Errors encountered during execution: %s", consolidated_results["errors"])
    logger.info("=" * 70)

    return consolidated_results


if __name__ == "__main__":
    result = run_pipeline()
    if result["status"] not in ("SUCCESS", "PARTIAL_ERROR"):
        sys.exit(1)
