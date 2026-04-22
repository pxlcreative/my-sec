"""
Tests for services.metadata_service.

Covers:
- fetch_metadata() returns parsed JSON (HTTP mocked)
- get_file_url() builds correct download URLs
- refresh_manifest():
    - inserts one SyncManifestEntry per file
    - skips entries already tracked
    - parses uploadedOn timestamps
    - ignores sectionDisplayName / sectionDisplayOrder year keys
- get_pending_files() returns only pending, ordered by year + uploaded_on
"""
from __future__ import annotations

import datetime
import json

from services.metadata_service import (
    fetch_metadata,
    get_file_url,
    get_pending_files,
    refresh_manifest,
)


# ── fetch_metadata ──────────────────────────────────────────────────────────

class TestFetchMetadata:
    def test_returns_parsed_json(self, mock_sec_requests):
        data = fetch_metadata()
        assert "advFilingData" in data
        assert "advW" in data


# ── get_file_url ────────────────────────────────────────────────────────────

class TestGetFileUrl:
    def test_builds_full_url(self):
        url = get_file_url("advFilingData", 2026, "advFilingData_2026_01.zip")
        assert url == (
            "https://reports.adviserinfo.sec.gov/reports/foia/advFilingData/2026/"
            "advFilingData_2026_01.zip"
        )

    def test_accepts_year_as_string(self):
        url = get_file_url("advW", "2025", "advW_2025_01.zip")
        assert "2025" in url


# ── refresh_manifest ────────────────────────────────────────────────────────

class TestRefreshManifest:
    def test_inserts_one_entry_per_file(self, db):
        from models.sync_manifest import SyncManifestEntry
        from sqlalchemy import select

        metadata = _sample_metadata()
        new_entries = refresh_manifest(metadata, db)

        assert len(new_entries) == 3  # one advFilingData + one advBrochures + one advW

        rows = list(db.scalars(select(SyncManifestEntry)).all())
        by_type = {r.file_type for r in rows}
        assert by_type == {"advFilingData", "advBrochures", "advW"}

    def test_skips_already_tracked_files(self, db):
        metadata = _sample_metadata()

        # First run inserts 3 entries.
        first = refresh_manifest(metadata, db)
        assert len(first) == 3

        # Second run should insert nothing — manifest already knows about them.
        second = refresh_manifest(metadata, db)
        assert second == []

    def test_parses_uploaded_on_timestamp(self, db):
        from models.sync_manifest import SyncManifestEntry
        from sqlalchemy import select

        refresh_manifest(_sample_metadata(), db)
        entry = db.scalars(
            select(SyncManifestEntry)
            .where(SyncManifestEntry.file_type == "advFilingData")
        ).first()
        assert entry.uploaded_on is not None
        assert entry.uploaded_on.year == 2026

    def test_ignores_section_metadata_keys(self, db):
        """sectionDisplayName / sectionDisplayOrder are NOT year entries."""
        metadata = {
            "advFilingData": {
                "sectionDisplayName": "ADV Filing Data",
                "sectionDisplayOrder": 1,
                "2026": {"files": [_file("advFilingData_2026_01.zip")]},
            },
            "advBrochures": {},
            "advW": {},
        }
        entries = refresh_manifest(metadata, db)
        assert len(entries) == 1
        assert entries[0].file_name == "advFilingData_2026_01.zip"

    def test_skips_entries_with_empty_filename(self, db):
        metadata = {
            "advFilingData": {
                "2026": {"files": [
                    _file(""),  # empty filename — should be skipped
                    _file("real_file.zip"),
                ]},
            },
            "advBrochures": {},
            "advW": {},
        }
        entries = refresh_manifest(metadata, db)
        assert len(entries) == 1
        assert entries[0].file_name == "real_file.zip"


# ── get_pending_files ───────────────────────────────────────────────────────

class TestGetPendingFiles:
    def test_returns_only_pending(self, db):
        from models.sync_manifest import SyncManifestEntry

        pending = SyncManifestEntry(
            file_type="advFilingData", file_name="p.zip", year=2026,
            status="pending",
        )
        complete = SyncManifestEntry(
            file_type="advFilingData", file_name="c.zip", year=2026,
            status="complete",
        )
        db.add_all([pending, complete])
        db.flush()

        results = get_pending_files(db, "advFilingData")
        names = [e.file_name for e in results]
        assert names == ["p.zip"]

    def test_filters_by_file_type(self, db):
        from models.sync_manifest import SyncManifestEntry

        a = SyncManifestEntry(
            file_type="advFilingData", file_name="a.zip", year=2026,
            status="pending",
        )
        b = SyncManifestEntry(
            file_type="advW", file_name="b.zip", year=2026,
            status="pending",
        )
        db.add_all([a, b])
        db.flush()

        results = get_pending_files(db, "advW")
        names = [e.file_name for e in results]
        assert names == ["b.zip"]

    def test_orders_by_year_then_uploaded_on(self, db):
        from models.sync_manifest import SyncManifestEntry

        old_2026 = SyncManifestEntry(
            file_type="advFilingData", file_name="old26.zip", year=2026,
            status="pending",
            uploaded_on=datetime.datetime(2026, 1, 15, tzinfo=datetime.timezone.utc),
        )
        new_2026 = SyncManifestEntry(
            file_type="advFilingData", file_name="new26.zip", year=2026,
            status="pending",
            uploaded_on=datetime.datetime(2026, 4, 1, tzinfo=datetime.timezone.utc),
        )
        old_2025 = SyncManifestEntry(
            file_type="advFilingData", file_name="old25.zip", year=2025,
            status="pending",
            uploaded_on=datetime.datetime(2025, 3, 1, tzinfo=datetime.timezone.utc),
        )
        db.add_all([new_2026, old_2026, old_2025])
        db.flush()

        results = get_pending_files(db, "advFilingData")
        assert [e.file_name for e in results] == ["old25.zip", "old26.zip", "new26.zip"]


# ── helpers ─────────────────────────────────────────────────────────────────

def _file(name: str) -> dict:
    return {
        "fileName": name,
        "displayName": f"Display for {name}",
        "size": 1024,
        "uploadedOn": "2026-02-01 06:00:00",
    }


def _sample_metadata() -> dict:
    """Tiny but realistic metadata with one file per tracked type."""
    return {
        "advFilingData": {"2026": {"files": [_file("advFilingData_2026_01.zip")]}},
        "advBrochures": {"2026": {"files": [_file("advBrochures_2026_01.zip")]}},
        "advW":         {"2026": {"files": [_file("advW_2026_01.zip")]}},
    }
