#!/usr/bin/env python3
"""
Module A2: Load 2025+ monthly ADV filing data from reports.adviserinfo.sec.gov.

Complements load_bulk_csv.py (which covers 2000–2024).
Downloads and processes all pending advFilingData and advW files from the
SEC metadata feed. Idempotent — skips files already marked complete in sync_manifest.

Usage (from project root):
    docker compose exec api python scripts/load_filing_data.py
"""
from __future__ import annotations

import io
import logging
import os
import sys
import csv
import zipfile
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Bootstrap paths / config
# ---------------------------------------------------------------------------
SCRIPT_DIR   = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "api"))
sys.path.insert(0, str(SCRIPT_DIR))

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Reuse helpers from load_bulk_csv
from load_bulk_csv import (  # noqa: E402
    download_zip,
    parse_ia_main,
    upsert_firms,
    insert_aum_history,
    _int_or_none,
    _parse_date,
    DOWNLOAD_DIR,
)


# ---------------------------------------------------------------------------
# ADV-W parser
# ---------------------------------------------------------------------------

def parse_advw_csv(zip_path: Path) -> list[dict]:
    """Parse an ADVW ZIP and return [{crd, filing_date}] for each withdrawal row."""
    rows = []
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            log.warning("parse_advw_csv: no CSV found in %s", zip_path.name)
            return rows

        with zf.open(csv_names[0]) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
            reader = csv.DictReader(text)
            orig = reader.fieldnames or []
            upper_map = {f.strip().upper(): f for f in orig}

            crd_col = next(
                (upper_map[k] for k in ("CRD_NUMBER", "FIRM_CRD_NUMBER", "CRD") if k in upper_map),
                None,
            )
            date_col = next(
                (upper_map[k] for k in ("FILING_DATE", "ADV_FILING_DATE", "DATESUBMITTED") if k in upper_map),
                None,
            )

            if not crd_col:
                log.warning("parse_advw_csv: no CRD column in %s — skipping", zip_path.name)
                return rows

            for row in reader:
                crd = _int_or_none(row.get(crd_col, ""))
                filing_date = _parse_date(row.get(date_col, "")) if date_col else None
                if crd:
                    rows.append({"crd": crd, "filing_date": filing_date})

    log.info("parse_advw_csv: %d withdrawal rows from %s", len(rows), zip_path.name)
    return rows


def apply_withdrawals(withdrawals: list[dict], conn) -> int:
    """Mark firms as Withdrawn. Returns number of rows processed."""
    if not withdrawals:
        return 0
    sql = """
        UPDATE firms
        SET registration_status = 'Withdrawn',
            last_filing_date    = %(filing_date)s,
            updated_at          = NOW()
        WHERE crd_number = %(crd)s
          AND (last_filing_date IS NULL OR %(filing_date)s >= last_filing_date)
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, withdrawals, page_size=500)
    conn.commit()
    return len(withdrawals)


# ---------------------------------------------------------------------------
# Manifest helpers (SQLAlchemy)
# ---------------------------------------------------------------------------

def _get_session():
    from db import SessionLocal
    return SessionLocal()


def _get_pending(session, file_type: str) -> list:
    from sqlalchemy import select
    from models.sync_manifest import SyncManifestEntry
    return list(
        session.scalars(
            select(SyncManifestEntry)
            .where(
                SyncManifestEntry.file_type == file_type,
                SyncManifestEntry.status == "pending",
            )
            .order_by(SyncManifestEntry.year, SyncManifestEntry.uploaded_on)
        ).all()
    )


def _mark_complete(entry, records: int, session) -> None:
    from datetime import datetime, timezone
    entry.status = "complete"
    entry.processed_at = datetime.now(timezone.utc)
    entry.records_processed = records
    session.commit()


def _mark_failed(entry, error: str, session) -> None:
    from datetime import datetime, timezone
    entry.status = "failed"
    entry.processed_at = datetime.now(timezone.utc)
    entry.error_message = error[:500]
    session.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        log.error("DATABASE_URL is not set. Copy .env.example → .env and fill it in.")
        sys.exit(1)

    dsn = database_url.replace("postgresql+psycopg2://", "postgresql://")

    log.info("Fetching metadata from reports.adviserinfo.sec.gov…")
    from services.metadata_service import fetch_metadata, get_file_url, refresh_manifest
    metadata = fetch_metadata()

    session = _get_session()
    try:
        new_entries = refresh_manifest(metadata, session)
        log.info("Manifest refreshed: %d new entries", len(new_entries))

        # ----------------------------------------------------------------
        # Process advFilingData
        # ----------------------------------------------------------------
        filing_pending = _get_pending(session, "advFilingData")
        log.info("advFilingData: %d pending file(s)", len(filing_pending))

        for entry in filing_pending:
            url = get_file_url(entry.file_type, entry.year, entry.file_name)
            log.info("Processing %s…", entry.file_name)
            try:
                zip_path = download_zip(url, DOWNLOAD_DIR)
                rows = parse_ia_main(zip_path)
                conn = psycopg2.connect(dsn)
                try:
                    n = upsert_firms(rows, conn)
                    insert_aum_history(rows, conn, source_tag="monthly_csv")
                finally:
                    conn.close()
                log.info("  %s: %d firms upserted from %d rows", entry.file_name, n, len(rows))
                _mark_complete(entry, n, session)
            except Exception as exc:
                log.error("  FAILED %s: %s", entry.file_name, exc)
                _mark_failed(entry, str(exc), session)

        # ----------------------------------------------------------------
        # Process advW
        # ----------------------------------------------------------------
        advw_pending = _get_pending(session, "advW")
        log.info("advW: %d pending file(s)", len(advw_pending))

        for entry in advw_pending:
            url = get_file_url(entry.file_type, entry.year, entry.file_name)
            log.info("Processing %s…", entry.file_name)
            try:
                zip_path = download_zip(url, DOWNLOAD_DIR)
                withdrawals = parse_advw_csv(zip_path)
                conn = psycopg2.connect(dsn)
                try:
                    n = apply_withdrawals(withdrawals, conn)
                finally:
                    conn.close()
                log.info("  %s: %d withdrawal(s) applied", entry.file_name, n)
                _mark_complete(entry, n, session)
            except Exception as exc:
                log.error("  FAILED %s: %s", entry.file_name, exc)
                _mark_failed(entry, str(exc), session)

    finally:
        session.close()

    log.info("=== load_filing_data.py complete ===")


if __name__ == "__main__":
    main()
