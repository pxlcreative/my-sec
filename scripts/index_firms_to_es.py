#!/usr/bin/env python3
"""
Index all firms from Postgres into Elasticsearch.

Usage:
    docker compose run --rm api python /project/scripts/index_firms_to_es.py

Or from the project root (with DATABASE_URL + ELASTICSEARCH_URL in .env):
    python scripts/index_firms_to_es.py

Exits non-zero if the bulk-error rate exceeds ERROR_RATE_FAIL_THRESHOLD so
a corrupted index doesn't silently go live. Records a sync_manifest row on
completion so `/api/sync/manifest` shows when the last full reindex ran.
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
ERROR_RATE_FAIL_THRESHOLD = 0.01  # 1%

REINDEX_MANIFEST_FILE_TYPE = "es_reindex"


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


def _record_manifest(conn, status: str, records: int, error: str | None = None) -> None:
    """Upsert a sync_manifest row so the Sync dashboard surfaces this reindex run."""
    from datetime import datetime, timezone
    file_name = f"firms_index_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_manifest (
                file_type, file_name, year, status,
                processed_at, records_processed, error_message
            ) VALUES (%s, %s, %s, %s, NOW(), %s, %s)
            ON CONFLICT (file_type, file_name) DO UPDATE SET
                status = EXCLUDED.status,
                processed_at = NOW(),
                records_processed = EXCLUDED.records_processed,
                error_message = EXCLUDED.error_message
            """,
            (REINDEX_MANIFEST_FILE_TYPE, file_name, datetime.now(timezone.utc).year,
             status, records, error),
        )
    conn.commit()


def main() -> int:
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        log.error("DATABASE_URL is not set.")
        return 1

    dsn = database_url.replace("postgresql+psycopg2://", "postgresql://")

    from services.es_client import bulk_index_firms, create_index_if_not_exists

    log.info("Ensuring Elasticsearch index exists ...")
    create_index_if_not_exists()

    log.info("Connecting to Postgres ...")
    conn = psycopg2.connect(dsn)

    t0 = time.time()
    total_seen = 0
    total_indexed = 0
    total_errors = 0

    try:
        for batch in fetch_firms_in_batches(conn, BATCH_SIZE):
            indexed = bulk_index_firms(batch)
            total_seen += len(batch)
            total_indexed += indexed
            total_errors += len(batch) - indexed

            if total_indexed % LOG_EVERY < BATCH_SIZE:
                log.info("Indexed %d firms so far (errors=%d)", total_indexed, total_errors)

        elapsed = time.time() - t0
        error_rate = total_errors / total_seen if total_seen else 0.0
        log.info(
            "Done. Indexed %d/%d firms in %.0fs (errors=%d, rate=%.2f%%)",
            total_indexed, total_seen, elapsed, total_errors, error_rate * 100,
        )

        if error_rate > ERROR_RATE_FAIL_THRESHOLD:
            err_msg = (
                f"Error rate {error_rate * 100:.2f}% exceeds threshold "
                f"{ERROR_RATE_FAIL_THRESHOLD * 100:.2f}%"
            )
            log.error(err_msg)
            _record_manifest(conn, "failed", total_indexed, error=err_msg)
            return 2

        _record_manifest(conn, "complete", total_indexed)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
