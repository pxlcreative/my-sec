"""
Module B1 – Monthly ADV Part 2 PDF download and storage pipeline.

Public entry points:
    sync_brochure_file(manifest_entry, db_session, sync_job_id) -> set[int]
    sync_month(zip_url, source_month, db_session, sync_job_id) -> set[int]

PDF filenames in the new brochure ZIPs encode all metadata:
    {CRD}_{BROCHURE_VERSION_ID}_{seq}_{YYYYMMDD}.pdf
No mapping CSV is present; parse_pdf_filename() extracts (crd, version_id, date_str).
"""
from __future__ import annotations

import logging
import re
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from config import settings

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BROCHURES_DIR = Path(settings.data_dir) / "brochures"
ZIPS_DIR      = Path(settings.data_dir) / "zips" / "brochures"

_HTTP_TIMEOUT  = 30
_RETRY_BACKOFF = 0.5
_MAX_RETRIES   = 5
_HEADERS = {"User-Agent": "MySEC/1.0 (self-hosted; research use)"}


# ---------------------------------------------------------------------------
# 1. PDF filename parser
# ---------------------------------------------------------------------------

def parse_pdf_filename(filename: str) -> tuple[int, int, str] | None:
    """
    Parse '{CRD}_{VERSION_ID}_{seq}_{YYYYMMDD}.pdf' → (crd, version_id, date_str).
    date_str is in 'YYYYMMDD' format.
    Returns None if filename doesn't match the expected pattern.
    """
    name = Path(filename).name
    m = re.match(r'^(\d+)_(\d+)_\d+_(\d{8})\.pdf$', name, re.IGNORECASE)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), m.group(3)


# ---------------------------------------------------------------------------
# 2. Streaming file download
# ---------------------------------------------------------------------------

def download_file(url: str, dest_dir: Path) -> Path:
    """
    Stream-download *url* into *dest_dir*.

    Skips if a file of the same name already exists with a non-zero size.
    Retries up to _MAX_RETRIES times with exponential backoff.
    Returns the local Path.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1].split("?")[0]
    dest = dest_dir / filename

    if dest.exists() and dest.stat().st_size > 0:
        log.info("download_file: already exists, skipping %s (%d bytes)", dest, dest.stat().st_size)
        return dest

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            log.info("Downloading %s (attempt %d)", url, attempt)
            with requests.get(url, stream=True, headers=_HEADERS, timeout=_HTTP_TIMEOUT) as r:
                r.raise_for_status()
                tmp = dest.with_suffix(".tmp")
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):  # 1 MiB
                        fh.write(chunk)
            tmp.rename(dest)
            log.info("Downloaded %s → %s (%d bytes)", url, dest, dest.stat().st_size)
            return dest
        except Exception as exc:
            log.warning("Download attempt %d failed for %s: %s", attempt, url, exc)
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BACKOFF * attempt)

    raise RuntimeError(f"Failed to download {url} after {_MAX_RETRIES} attempts")


# ---------------------------------------------------------------------------
# 3. PDF extraction and storage
# ---------------------------------------------------------------------------

def extract_and_store_pdf(
    zip_path: Path,
    zip_entry_name: str,
    version_id: int,
    crd: int,
    date_tag: str,
    backend,  # StorageBackend — typed loosely to avoid circular import
) -> tuple[str, int]:
    """
    Read the PDF at *zip_entry_name* from *zip_path* and store via *backend*.
    Returns (uri, file_size_bytes). Returns (uri, 0) if already stored.
    """
    from services.storage_backends import make_brochure_key

    key = make_brochure_key(crd, version_id, date_tag)

    if backend.exists(key):
        return backend.uri_for(key), 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        data = zf.read(zip_entry_name)

    backend.put(key, data)
    return backend.uri_for(key), len(data)


# ---------------------------------------------------------------------------
# 4. Orchestrator — one ZIP file
# ---------------------------------------------------------------------------

def sync_month(
    zip_url: str,
    source_month: str,
    db_session: Session,
    sync_job_id: int,
) -> set[int]:
    """
    Download *zip_url* and store all new brochure PDFs found inside.

    source_month: 'YYYY-MM' string used for AdvBrochure.source_month.

    Returns the set of CRD numbers for which at least one new brochure was stored.
    Updates SyncJob throughout.
    """
    from models.brochure import AdvBrochure
    from models.firm import Firm
    from models.sync_job import SyncJob
    from sqlalchemy.orm.attributes import flag_modified
    from services.storage_backends import get_active_backend

    job: SyncJob | None = db_session.get(SyncJob, sync_job_id)
    if job is None:
        raise ValueError(f"SyncJob {sync_job_id} not found")

    backend = get_active_backend(db_session)

    def _log_event(msg: str) -> None:
        j = db_session.get(SyncJob, sync_job_id)
        if j:
            entry = {"ts": datetime.now(timezone.utc).isoformat(), "msg": msg}
            current = dict(j.results) if j.results else {}
            current.setdefault("log", []).append(entry)
            j.results = current
            flag_modified(j, "results")
            db_session.commit()

    def _commit_progress(processed: int, stored: int, message: str | None = None) -> None:
        j = db_session.get(SyncJob, sync_job_id)
        if j:
            j.firms_processed = processed
            j.firms_updated   = stored
            if message:
                j.error_message = message
            db_session.commit()

    zip_name = zip_url.split("/")[-1]
    _log_event(f"Downloading {zip_name}…")

    try:
        zip_path = download_file(zip_url, ZIPS_DIR)
    except RuntimeError as exc:
        log.error("sync_month: download failed for %s: %s", zip_url, exc)
        _log_event(f"Download failed for {zip_name}: {exc}")
        raise

    size_mb = zip_path.stat().st_size // (1024 * 1024)
    _log_event(f"Downloaded {zip_name} ({size_mb} MB) — scanning PDFs…")

    # Pre-load known CRDs to skip orphaned brochures
    known_crds: set[int] = set(
        db_session.scalars(select(Firm.crd_number)).all()
    )

    # Pre-load already-stored version IDs for fast dedup
    stored_vids: set[int] = set(
        db_session.scalars(select(AdvBrochure.brochure_version_id)).all()
    )

    total_processed = 0
    total_stored    = 0
    new_brochure_crds: set[int] = set()

    with zipfile.ZipFile(zip_path, "r") as zf:
        all_entries = zf.namelist()

    pdf_entries = [n for n in all_entries if n.lower().endswith(".pdf")]
    _log_event(f"Found {len(pdf_entries):,} PDFs in {zip_name}")

    for entry_name in pdf_entries:
        parsed = parse_pdf_filename(entry_name)
        if parsed is None:
            log.debug("sync_month: skipping unrecognised entry %s", entry_name)
            continue

        crd, version_id, date_tag = parsed
        total_processed += 1

        if crd not in known_crds:
            log.debug("sync_month: CRD %d not in firms table, skipping version %d", crd, version_id)
            continue

        if version_id in stored_vids:
            continue

        try:
            uri, size_bytes = extract_and_store_pdf(
                zip_path, entry_name, version_id, crd, date_tag, backend
            )
        except Exception as exc:
            log.warning(
                "sync_month: could not extract version_id=%d crd=%d: %s",
                version_id, crd, exc,
            )
            continue

        # Parse date_tag YYYYMMDD → date
        submit_date_obj = None
        try:
            submit_date_obj = datetime.strptime(date_tag, "%Y%m%d").date()
        except ValueError:
            pass

        brochure = AdvBrochure(
            crd_number=crd,
            brochure_version_id=version_id,
            brochure_name=None,
            date_submitted=submit_date_obj,
            source_month=source_month,
            file_path=uri,
            file_size_bytes=size_bytes,
            downloaded_at=datetime.now(timezone.utc),
        )
        db_session.add(brochure)

        try:
            db_session.commit()
        except Exception as exc:
            db_session.rollback()
            log.warning("sync_month: DB insert failed version_id=%d: %s", version_id, exc)
            continue

        stored_vids.add(version_id)
        total_stored += 1
        new_brochure_crds.add(crd)
        log.info(
            "sync_month: stored version_id=%d crd=%d uri=%s size=%d bytes",
            version_id, crd, uri, size_bytes,
        )

        if total_processed % 500 == 0:
            _commit_progress(total_processed, total_stored)
            _log_event(f"Progress: {total_processed:,} scanned, {total_stored:,} stored")

    _commit_progress(total_processed, total_stored)
    _log_event(
        f"{zip_name} complete: {total_processed:,} scanned, {total_stored:,} stored, "
        f"{len(new_brochure_crds):,} firms updated"
    )
    log.info(
        "sync_month %s complete: processed=%d stored=%d new_crds=%d",
        zip_url, total_processed, total_stored, len(new_brochure_crds),
    )
    return new_brochure_crds


# ---------------------------------------------------------------------------
# 5. Entry point via manifest
# ---------------------------------------------------------------------------

def sync_brochure_file(manifest_entry, db_session: Session, sync_job_id: int) -> set[int]:
    """
    Sync a single brochure ZIP identified by a SyncManifestEntry.
    Constructs the download URL and delegates to sync_month().
    """
    from services.metadata_service import get_file_url

    zip_url = get_file_url(
        manifest_entry.file_type,
        manifest_entry.year,
        manifest_entry.file_name,
    )

    # Derive source_month from the manifest display_name or file_name
    # file_name e.g. "ADV_Brochures_2026_March_1_of_2.zip" → "2026-03"
    source_month = _derive_source_month(manifest_entry.file_name, manifest_entry.year)

    return sync_month(zip_url, source_month, db_session, sync_job_id)


_MONTH_TO_NUM = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def _derive_source_month(file_name: str, year: int) -> str:
    """
    Derive 'YYYY-MM' from a brochure zip filename.
    e.g. 'ADV_Brochures_2026_March_1_of_2.zip' → '2026-03'
         'ADV_Brochures_2025_January.zip' → '2025-01'
    """
    lower = file_name.lower()
    for month_name, month_num in _MONTH_TO_NUM.items():
        if month_name in lower:
            return f"{year}-{month_num}"
    return f"{year}-01"
