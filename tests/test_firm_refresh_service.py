"""
Tests for services.firm_refresh_service.refresh_firm.

Covers:
- Happy path: fetch → extract → diff → persist snapshot + changes + ES reindex
- No-change path: hash identical, last_iapd_refresh_at still updated, no diff rows
- EDGAR-format response (missing registration_status): Inactive inferred for old firms
- ES reindex failure enqueues celery reindex retry (Phase 1 hardening)
- AUM history row inserted on every refresh (deduped per filing_date)
"""
from __future__ import annotations

import datetime
from unittest.mock import patch

from services.firm_refresh_service import refresh_firm


# ── helpers ─────────────────────────────────────────────────────────────────

def _seed_firm(db, crd: int, **overrides):
    from models.firm import Firm
    defaults = dict(
        crd_number=crd,
        legal_name="Seed Firm",
        registration_status="Registered",
        main_city="NYC", main_state="NY",
        last_filing_date=datetime.date(2025, 1, 1),
    )
    defaults.update(overrides)
    firm = Firm(**defaults)
    db.add(firm)
    db.flush()
    return firm


def _iacontent(crd: int, **overrides):
    """Minimal IAPD iacontent payload extract_firm_fields can parse."""
    doc = {
        "basicInformation": {
            "firmId": crd, "firmName": "Test Firm LLC",
            "iaSECNumber": "801-test", "advFilingDate": "2026-01-15",
        },
        "iaFirmAddressDetails": {
            "officeAddress": {
                "street1": "1 Main", "city": "NYC", "state": "NY", "postalCode": "10001",
            },
        },
        "registrationStatus": [{"secJurisdiction": "SEC", "status": "Approved"}],
    }
    for k, v in overrides.items():
        doc[k] = v
    return doc


# ── Happy path ──────────────────────────────────────────────────────────────

class TestRefreshFirmHappyPath:
    def test_fetch_extract_diff_persist(self, db, mock_es):
        """First refresh of a firm: no prior snapshot → all fields diff."""
        from models.firm import FirmChange, FirmSnapshot
        from sqlalchemy import select

        firm = _seed_firm(db, 900_001, legal_name="Old Name LLC")
        iacontent = _iacontent(900_001)
        iacontent["basicInformation"]["firmName"] = "New Name LLC"

        with patch("services.iapd_client.fetch_firm", return_value=iacontent):
            diffs = refresh_firm(900_001, db)

        assert any(d["field_path"] == "legal_name" for d in diffs)

        snap_rows = db.scalars(
            select(FirmSnapshot).where(FirmSnapshot.crd_number == 900_001)
        ).all()
        assert len(snap_rows) == 1

        change_rows = db.scalars(
            select(FirmChange).where(FirmChange.crd_number == 900_001)
        ).all()
        assert any(c.field_path == "legal_name" for c in change_rows)

        db.expire_all()
        firm_after = db.get(type(firm), 900_001)
        assert firm_after.legal_name == "New Name LLC"
        assert firm_after.last_iapd_refresh_at is not None

    def test_reindexes_to_es_on_change(self, db, mock_es):
        """ES index must receive the updated firm document after a change."""
        _seed_firm(db, 900_002, legal_name="Old Name")
        iacontent = _iacontent(900_002)
        iacontent["basicInformation"]["firmName"] = "Updated Name"

        with patch("services.iapd_client.fetch_firm", return_value=iacontent):
            refresh_firm(900_002, db)

        assert "900002" in mock_es.docs
        assert mock_es.docs["900002"]["legal_name"] == "Updated Name"

    def test_no_change_still_updates_last_iapd_refresh_at(self, db, mock_es):
        """Hash matches prior snapshot → no diffs, but refresh timestamp bumps."""
        from models.firm import Firm, FirmSnapshot
        from services.change_detector import canonical_json, sha256_hash

        _seed_firm(db, 900_003)

        iacontent = _iacontent(900_003, basicInformation={
            "firmId": 900_003, "firmName": "Test Firm LLC",
            "iaSECNumber": "801-test", "advFilingDate": "2026-01-15",
        })

        # Pre-seed a snapshot with the exact hash of what IAPD will return.
        from services.iapd_client import extract_firm_fields
        extracted = extract_firm_fields(iacontent)
        prior_hash = sha256_hash(canonical_json(extracted))
        db.add(FirmSnapshot(
            crd_number=900_003, snapshot_hash=prior_hash, raw_json=extracted,
            synced_at=datetime.datetime(2026, 4, 1, tzinfo=datetime.timezone.utc),
        ))
        db.flush()

        with patch("services.iapd_client.fetch_firm", return_value=iacontent):
            diffs = refresh_firm(900_003, db)

        assert diffs == []
        db.expire_all()
        firm = db.get(Firm, 900_003)
        assert firm.last_iapd_refresh_at is not None


# ── EDGAR-format inactive inference ────────────────────────────────────────

class TestEdgarFormat:
    def test_old_firm_with_no_status_becomes_inactive(self, db, mock_es):
        """IAPD returns EDGAR format (no registrationStatus) + last filing > 1yr old
        → firm auto-classified Inactive."""
        from models.firm import Firm

        _seed_firm(
            db, 900_010, registration_status="Registered",
            last_filing_date=datetime.date(2019, 6, 30),
        )
        # EDGAR-format response: old advFilingDate, no registrationStatus.
        iacontent = _iacontent(900_010)
        iacontent["basicInformation"]["advFilingDate"] = "2019-06-30"
        del iacontent["registrationStatus"]

        with patch("services.iapd_client.fetch_firm", return_value=iacontent):
            refresh_firm(900_010, db)

        db.expire_all()
        firm = db.get(Firm, 900_010)
        assert firm.registration_status == "Inactive"

    def test_recent_firm_with_no_status_stays_registered(self, db, mock_es):
        """Recent filings (< 1yr) should NOT be auto-marked Inactive."""
        from models.firm import Firm

        _seed_firm(
            db, 900_011, registration_status="Registered",
            last_filing_date=datetime.date.today() - datetime.timedelta(days=60),
        )
        iacontent = _iacontent(900_011)
        del iacontent["registrationStatus"]

        with patch("services.iapd_client.fetch_firm", return_value=iacontent):
            refresh_firm(900_011, db)

        db.expire_all()
        firm = db.get(Firm, 900_011)
        assert firm.registration_status == "Registered"


# ── ES reindex retry (Phase 1) ──────────────────────────────────────────────

class TestEsReindexRetry:
    def test_reindex_failure_enqueues_retry_task(self, db):
        """When bulk_index_firms raises, reindex_firm.apply_async must be called."""
        _seed_firm(db, 900_020)

        iacontent = _iacontent(900_020)
        iacontent["basicInformation"]["firmName"] = "Changed"

        with patch(
            "services.iapd_client.fetch_firm", return_value=iacontent,
        ), patch(
            "services.es_client.bulk_index_firms",
            side_effect=ConnectionError("ES down"),
        ), patch(
            "celery_tasks.refresh_tasks.reindex_firm.apply_async",
        ) as mock_enqueue:
            refresh_firm(900_020, db)

        mock_enqueue.assert_called_once()
        args_kw = mock_enqueue.call_args.kwargs
        assert args_kw["args"] == [900_020]
        assert args_kw["countdown"] == 60


# ── AUM history ─────────────────────────────────────────────────────────────

class TestAumHistory:
    def test_inserts_iapd_live_row_on_refresh(self, db, mock_es):
        from models.aum import FirmAumHistory
        from sqlalchemy import select

        _seed_firm(db, 900_030)
        with patch("services.iapd_client.fetch_firm", return_value=_iacontent(900_030)):
            refresh_firm(900_030, db)

        rows = db.scalars(
            select(FirmAumHistory).where(
                FirmAumHistory.crd_number == 900_030,
                FirmAumHistory.source == "iapd_live",
            )
        ).all()
        assert len(rows) == 1

    def test_second_refresh_same_day_dedupes_aum_row(self, db, mock_es):
        """(crd, filing_date=today, source='iapd_live') is unique — second
        refresh on the same day must not insert a duplicate."""
        from models.aum import FirmAumHistory
        from sqlalchemy import select

        _seed_firm(db, 900_031)
        with patch("services.iapd_client.fetch_firm", return_value=_iacontent(900_031)):
            refresh_firm(900_031, db)
            refresh_firm(900_031, db)

        rows = db.scalars(
            select(FirmAumHistory).where(
                FirmAumHistory.crd_number == 900_031,
                FirmAumHistory.source == "iapd_live",
            )
        ).all()
        assert len(rows) == 1
