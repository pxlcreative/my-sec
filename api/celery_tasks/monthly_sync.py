"""
Celery task for the monthly data sync (Module B1).

Driven by reports_metadata.json — no hardcoded URLs or month probing.

Phase 1 (fast):  advFilingData + advW  — CSV-based firm updates
Phase 2 (slow):  advBrochures          — PDF download and storage
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from celery_tasks.app import app

log = logging.getLogger(__name__)


@app.task(bind=True, name="monthly_sync.monthly_data_sync", max_retries=1)
def monthly_data_sync(self, job_id: int | None = None) -> dict:
    """
    Full monthly data sync driven by reports_metadata.json.

    If *job_id* is given (manual trigger), updates that pending record to running.
    Otherwise creates a new SyncJob (Beat-scheduled runs).
    """
    from db import SessionLocal
    from models.sync_job import SyncJob
    from models.sync_manifest import SyncManifestEntry
    from services.metadata_service import fetch_metadata, get_file_url, refresh_manifest, get_pending_files
    from sqlalchemy.orm.attributes import flag_modified

    log.info("monthly_data_sync starting")

    with SessionLocal() as session:
        # ----------------------------------------------------------------
        # Set up SyncJob
        # ----------------------------------------------------------------
        if job_id:
            job = session.get(SyncJob, job_id)
            job.status = "running"
            job.started_at = datetime.now(timezone.utc)
            session.commit()
        else:
            job = SyncJob(
                job_type="monthly_data",
                status="running",
                source_url="reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json",
                started_at=datetime.now(timezone.utc),
            )
            session.add(job)
            session.commit()
            session.refresh(job)
        job_id = job.id

        def _log_event(msg: str) -> None:
            j = session.get(SyncJob, job_id)
            if j:
                entry = {"ts": datetime.now(timezone.utc).isoformat(), "msg": msg}
                current = dict(j.results) if j.results else {}
                current.setdefault("log", []).append(entry)
                j.results = current
                flag_modified(j, "results")
                session.commit()

        try:
            # ----------------------------------------------------------------
            # Refresh manifest from metadata feed
            # ----------------------------------------------------------------
            _log_event("Fetching reports_metadata.json from SEC…")
            metadata = fetch_metadata()
            new_entries = refresh_manifest(metadata, session)
            _log_event(
                f"Manifest refreshed: {len(new_entries)} new file(s) discovered"
            )

            total_filing_firms = 0
            total_brochures_stored = 0
            new_brochure_crds: set[int] = set()

            # ----------------------------------------------------------------
            # Phase 1a: advFilingData — update firms / AUM from monthly CSVs
            # ----------------------------------------------------------------
            filing_pending = get_pending_files(session, "advFilingData")
            if filing_pending:
                _log_event(f"Phase 1: processing {len(filing_pending)} pending advFilingData file(s)…")
                total_filing_firms = _process_filing_data(
                    filing_pending, session, job_id, _log_event
                )
            else:
                _log_event("Phase 1: no pending advFilingData files")

            # ----------------------------------------------------------------
            # Phase 1b: advW — mark withdrawn firms
            # ----------------------------------------------------------------
            advw_pending = get_pending_files(session, "advW")
            if advw_pending:
                _log_event(f"Phase 1b: processing {len(advw_pending)} pending advW file(s)…")
                _process_advw(advw_pending, session, job_id, _log_event)
            else:
                _log_event("Phase 1b: no pending advW files")

            # ----------------------------------------------------------------
            # Phase 2: advBrochures — download PDFs
            # ----------------------------------------------------------------
            brochure_pending = get_pending_files(session, "advBrochures")
            if brochure_pending:
                _log_event(f"Phase 2: processing {len(brochure_pending)} pending advBrochures file(s)…")
                total_brochures_stored, new_brochure_crds = _process_brochures(
                    brochure_pending, session, job_id, _log_event
                )
            else:
                _log_event("Phase 2: no pending advBrochures files")

            # ----------------------------------------------------------------
            # Finalize job
            # ----------------------------------------------------------------
            job = session.get(SyncJob, job_id)
            job.status = "complete"
            job.completed_at = datetime.now(timezone.utc)
            job.firms_updated = total_filing_firms + total_brochures_stored
            session.commit()

            result = {
                "status": "complete",
                "filing_firms_updated": total_filing_firms,
                "brochures_stored": total_brochures_stored,
                "refreshes_enqueued": len(new_brochure_crds),
            }
            _log_event(
                f"Sync complete: {total_filing_firms} firm records updated, "
                f"{total_brochures_stored} brochures stored, "
                f"{len(new_brochure_crds)} firms enqueued for IAPD refresh"
            )
            log.info("monthly_data_sync complete: %s", result)

            if new_brochure_crds:
                from celery_tasks.refresh_tasks import refresh_firms_with_new_brochures
                refresh_firms_with_new_brochures.delay(list(new_brochure_crds))

            return result

        except Exception as exc:
            log.exception("monthly_data_sync failed")
            try:
                job = session.get(SyncJob, job_id)
                if job:
                    job.status = "failed"
                    job.error_message = str(exc)
                    job.completed_at = datetime.now(timezone.utc)
                    session.commit()
            except Exception:
                log.exception("monthly_data_sync: could not update SyncJob to failed")
            raise self.retry(exc=exc, countdown=900) if self.request.retries < self.max_retries else exc


# ---------------------------------------------------------------------------
# Phase helpers
# ---------------------------------------------------------------------------

def _process_filing_data(pending, session, job_id: int, _log_event) -> int:
    """Download and ingest each pending advFilingData ZIP. Returns total firms upserted."""
    import sys
    from pathlib import Path
    # Import bulk-load helpers from load_bulk_csv
    _scripts = Path(__file__).parent.parent.parent / "scripts"
    if str(_scripts) not in sys.path:
        sys.path.insert(0, str(_scripts))

    import psycopg2
    from load_bulk_csv import (
        download_zip,
        parse_ia_main,
        upsert_firms,
        insert_aum_history,
        DOWNLOAD_DIR,
    )
    from services.metadata_service import get_file_url
    from models.sync_manifest import SyncManifestEntry
    from datetime import datetime, timezone

    database_url = os.environ.get("DATABASE_URL", "")
    dsn = database_url.replace("postgresql+psycopg2://", "postgresql://")

    total_firms = 0

    for entry in pending:
        _mark_processing(entry, job_id, session)
        url = get_file_url(entry.file_type, entry.year, entry.file_name)
        _log_event(f"Downloading {entry.file_name}…")
        try:
            zip_path = download_zip(url, DOWNLOAD_DIR)
            rows = parse_ia_main(zip_path)
            conn = psycopg2.connect(dsn)
            try:
                n = upsert_firms(rows, conn)
                insert_aum_history(rows, conn, source_tag="monthly_csv")
            finally:
                conn.close()
            total_firms += n
            _log_event(f"{entry.file_name}: {n} firms upserted from {len(rows)} rows")
            _mark_complete(entry, n, session)
        except Exception as exc:
            log.error("_process_filing_data: failed %s: %s", entry.file_name, exc)
            _log_event(f"Failed {entry.file_name}: {exc}")
            _mark_failed(entry, str(exc), session)

    return total_firms


def _process_advw(pending, session, job_id: int, _log_event) -> None:
    """Download and ingest each pending advW ZIP, marking withdrawn firms."""
    import csv
    import io
    import zipfile as _zipfile
    import sys
    from pathlib import Path

    _scripts = Path(__file__).parent.parent.parent / "scripts"
    if str(_scripts) not in sys.path:
        sys.path.insert(0, str(_scripts))

    from load_bulk_csv import download_zip, DOWNLOAD_DIR, _int_or_none, _parse_date
    from services.metadata_service import get_file_url
    import psycopg2

    database_url = os.environ.get("DATABASE_URL", "")
    dsn = database_url.replace("postgresql+psycopg2://", "postgresql://")

    for entry in pending:
        _mark_processing(entry, job_id, session)
        url = get_file_url(entry.file_type, entry.year, entry.file_name)
        _log_event(f"Downloading {entry.file_name}…")
        try:
            zip_path = download_zip(url, DOWNLOAD_DIR)
            withdrawals = _parse_advw_csv(zip_path)
            if withdrawals:
                conn = psycopg2.connect(dsn)
                try:
                    _apply_withdrawals(withdrawals, conn)
                finally:
                    conn.close()
            _log_event(f"{entry.file_name}: {len(withdrawals)} withdrawal(s) applied")
            _mark_complete(entry, len(withdrawals), session)
        except Exception as exc:
            log.error("_process_advw: failed %s: %s", entry.file_name, exc)
            _log_event(f"Failed {entry.file_name}: {exc}")
            _mark_failed(entry, str(exc), session)


def _parse_advw_csv(zip_path) -> list[dict]:
    """Parse ADVW ZIP and return [{crd, filing_date}] for each withdrawal row."""
    import csv
    import io
    import zipfile as _zipfile
    from pathlib import Path

    rows = []
    with _zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            return rows
        with zf.open(csv_names[0]) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
            reader = csv.DictReader(text)
            fieldnames = [f.strip().upper() for f in (reader.fieldnames or [])]
            # Map original fieldnames to upper for lookup
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
                log.warning("_parse_advw_csv: no CRD column found in %s", zip_path.name)
                return rows

            import sys
            from pathlib import Path as _Path
            _scripts = _Path(__file__).parent.parent.parent / "scripts"
            if str(_scripts) not in sys.path:
                sys.path.insert(0, str(_scripts))
            from load_bulk_csv import _int_or_none, _parse_date

            for row in reader:
                crd = _int_or_none(row.get(crd_col, ""))
                filing_date = _parse_date(row.get(date_col, "")) if date_col else None
                if crd:
                    rows.append({"crd": crd, "filing_date": filing_date})
    return rows


def _apply_withdrawals(withdrawals: list[dict], conn) -> None:
    import psycopg2.extras

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


def _process_brochures(pending, session, job_id: int, _log_event) -> tuple[int, set[int]]:
    """Download and ingest each pending advBrochures ZIP. Returns (total_stored, new_crd_set)."""
    from services.pdf_sync_service import sync_brochure_file

    total_stored = 0
    all_new_crds: set[int] = set()

    for entry in pending:
        _mark_processing(entry, job_id, session)
        _log_event(f"Processing brochures ZIP: {entry.file_name}…")
        try:
            new_crds = sync_brochure_file(entry, session, job_id)
            total_stored += len(new_crds)  # approximate: crds ≤ PDFs stored
            all_new_crds.update(new_crds)
            _mark_complete(entry, len(new_crds), session)
        except Exception as exc:
            log.error("_process_brochures: failed %s: %s", entry.file_name, exc)
            _log_event(f"Failed {entry.file_name}: {exc}")
            _mark_failed(entry, str(exc), session)

    return total_stored, all_new_crds


# ---------------------------------------------------------------------------
# Manifest status helpers
# ---------------------------------------------------------------------------

def _mark_processing(entry, job_id: int, session) -> None:
    entry.status = "processing"
    entry.sync_job_id = job_id
    session.commit()


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
    entry.error_message = error
    session.commit()
