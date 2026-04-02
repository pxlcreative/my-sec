"""
Metadata service — reads reports_metadata.json from reports.adviserinfo.sec.gov
and synchronises the sync_manifest table.

Public API:
    fetch_metadata() -> dict
    get_file_url(file_type, year, file_name) -> str
    refresh_manifest(metadata, db_session) -> list[SyncManifestEntry]
    get_pending_files(db_session, file_type) -> list[SyncManifestEntry]
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

METADATA_URL = "https://reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json"
REPORTS_BASE_URL = "https://reports.adviserinfo.sec.gov/reports/foia"

# File types we care about (skip advFirmCRS and advFirmCRSDocs)
_TRACKED_FILE_TYPES = ("advFilingData", "advBrochures", "advW")

_HEADERS = {"User-Agent": "MySEC/1.0 (self-hosted; research use)"}


def fetch_metadata() -> dict:
    """Fetch and return the parsed reports_metadata.json."""
    log.info("Fetching metadata from %s", METADATA_URL)
    r = requests.get(METADATA_URL, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def get_file_url(file_type: str, year: str | int, file_name: str) -> str:
    """Construct the download URL for a file from the metadata feed."""
    return f"{REPORTS_BASE_URL}/{file_type}/{year}/{file_name}"


def refresh_manifest(metadata: dict, db_session: Session) -> list:
    """
    Upsert SyncManifestEntry rows for all tracked file types.
    Skips entries that are already complete or processing.
    Returns the list of newly-inserted (pending) entries.
    """
    from models.sync_manifest import SyncManifestEntry

    new_entries: list[SyncManifestEntry] = []

    for file_type in _TRACKED_FILE_TYPES:
        section = metadata.get(file_type, {})
        for year_str, year_data in section.items():
            if year_str in ("sectionDisplayName", "sectionDisplayOrder"):
                continue
            try:
                year = int(year_str)
            except ValueError:
                continue

            for file_info in year_data.get("files", []):
                file_name = file_info.get("fileName", "").strip()
                if not file_name:
                    continue

                uploaded_on: datetime | None = None
                raw_ts = file_info.get("uploadedOn", "")
                if raw_ts:
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                        try:
                            uploaded_on = datetime.strptime(raw_ts, fmt).replace(
                                tzinfo=timezone.utc
                            )
                            break
                        except ValueError:
                            continue

                existing = db_session.scalar(
                    select(SyncManifestEntry).where(
                        SyncManifestEntry.file_type == file_type,
                        SyncManifestEntry.file_name == file_name,
                    )
                )
                if existing is not None:
                    continue  # already tracked

                entry = SyncManifestEntry(
                    file_type=file_type,
                    file_name=file_name,
                    year=year,
                    display_name=file_info.get("displayName"),
                    file_size_bytes=file_info.get("size"),
                    uploaded_on=uploaded_on,
                    status="pending",
                )
                db_session.add(entry)
                new_entries.append(entry)

    if new_entries:
        db_session.commit()
        for e in new_entries:
            db_session.refresh(e)
        log.info("refresh_manifest: added %d new entries", len(new_entries))
    else:
        log.info("refresh_manifest: no new files found")

    return new_entries


def get_pending_files(db_session: Session, file_type: str) -> list:
    """Return manifest entries with status='pending' for the given file_type, oldest first."""
    from models.sync_manifest import SyncManifestEntry

    return list(
        db_session.scalars(
            select(SyncManifestEntry)
            .where(
                SyncManifestEntry.file_type == file_type,
                SyncManifestEntry.status == "pending",
            )
            .order_by(SyncManifestEntry.year, SyncManifestEntry.uploaded_on)
        ).all()
    )
