#!/usr/bin/env python3
"""
Index all firms from Postgres into Elasticsearch.

Usage:
    docker compose run --rm api python /project/scripts/index_firms_to_es.py

Or from the project root (with DATABASE_URL + ELASTICSEARCH_URL in .env):
    python scripts/index_firms_to_es.py
"""
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "api"))

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BATCH_SIZE = 1000
LOG_EVERY = 5000


def fetch_firms_in_batches(conn, batch_size: int):
    """Yield lists of firm dicts from Postgres in batches."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT crd_number, legal_name, business_name,
                   main_street1, main_city, main_state, main_zip,
                   registration_status
            FROM firms
            ORDER BY crd_number
        """)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield [dict(r) for r in rows]


def main() -> None:
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        log.error("DATABASE_URL is not set.")
        sys.exit(1)

    dsn = database_url.replace("postgresql+psycopg2://", "postgresql://")

    # Import es_client after sys.path is set up
    from services.es_client import bulk_index_firms, create_index_if_not_exists

    log.info("Ensuring Elasticsearch index exists ...")
    create_index_if_not_exists()

    log.info("Connecting to Postgres ...")
    conn = psycopg2.connect(dsn)

    t0 = time.time()
    total_indexed = 0
    total_errors = 0

    for batch in fetch_firms_in_batches(conn, BATCH_SIZE):
        indexed = bulk_index_firms(batch)
        total_indexed += indexed
        total_errors += len(batch) - indexed

        if total_indexed % LOG_EVERY < BATCH_SIZE:
            log.info("Indexed %d firms so far ...", total_indexed)

    conn.close()
    elapsed = time.time() - t0
    log.info(
        "Done. Indexed %d firms in %.0fs (%d errors)",
        total_indexed, elapsed, total_errors,
    )


if __name__ == "__main__":
    main()
