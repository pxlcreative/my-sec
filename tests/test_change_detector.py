"""
Unit tests for services.change_detector.

Covers:
- canonical_json determinism (key order does not affect output)
- sha256_hash stability
- get_current_snapshot returns most recent, None if absent
- compute_diffs: added / removed / changed / unchanged / multi-field
- save_snapshot_and_changes persists snapshot + per-field changes
- First-refresh edge case (no prior snapshot → diffs from empty baseline)
"""
from __future__ import annotations

import datetime


from services.change_detector import (
    canonical_json,
    compute_diffs,
    get_current_snapshot,
    save_snapshot_and_changes,
    sha256_hash,
)


# ── canonical_json ──────────────────────────────────────────────────────────

class TestCanonicalJson:
    def test_same_keys_in_different_order_produce_same_output(self):
        a = {"b": 1, "a": 2, "c": 3}
        b = {"c": 3, "a": 2, "b": 1}
        assert canonical_json(a) == canonical_json(b)

    def test_compact_no_whitespace(self):
        s = canonical_json({"a": 1, "b": 2})
        assert s == '{"a":1,"b":2}'

    def test_handles_date_via_default_str(self):
        # date objects serialize via str() per the `default=str` fallback.
        s = canonical_json({"d": datetime.date(2026, 1, 1)})
        assert "2026-01-01" in s


# ── sha256_hash ─────────────────────────────────────────────────────────────

class TestSha256Hash:
    def test_deterministic(self):
        assert sha256_hash("hello") == sha256_hash("hello")

    def test_different_inputs_different_outputs(self):
        assert sha256_hash("hello") != sha256_hash("hello ")

    def test_hex_length_64(self):
        assert len(sha256_hash("x")) == 64


# ── compute_diffs ───────────────────────────────────────────────────────────

class TestComputeDiffs:
    def test_no_change_returns_empty(self):
        fields = {"aum_total": 100, "registration_status": "Registered"}
        assert compute_diffs(1, fields, fields) == []

    def test_single_field_change(self):
        old = {"aum_total": 100}
        new = {"aum_total": 150}
        diffs = compute_diffs(1, old, new)
        assert len(diffs) == 1
        assert diffs[0]["field_path"] == "aum_total"
        assert diffs[0]["old_value"] == "100"
        assert diffs[0]["new_value"] == "150"

    def test_multi_field_change(self):
        old = {"aum_total": 100, "main_state": "NY", "registration_status": "Registered"}
        new = {"aum_total": 150, "main_state": "CA", "registration_status": "Withdrawn"}
        diffs = compute_diffs(1, old, new)
        assert {d["field_path"] for d in diffs} == {"aum_total", "main_state", "registration_status"}

    def test_none_to_value(self):
        """None → value is a change."""
        diffs = compute_diffs(1, {"aum_total": None}, {"aum_total": 100})
        assert len(diffs) == 1
        assert diffs[0]["old_value"] is None
        assert diffs[0]["new_value"] == "100"

    def test_value_to_none(self):
        """Value → None is a change."""
        diffs = compute_diffs(1, {"aum_total": 100}, {"aum_total": None})
        assert len(diffs) == 1
        assert diffs[0]["new_value"] is None

    def test_no_prior_snapshot_treats_old_as_empty(self):
        """First refresh: no prior snapshot → diffs computed against {}."""
        diffs = compute_diffs(1, None, {"aum_total": 100, "main_state": "NY"})
        fields = {d["field_path"] for d in diffs}
        assert "aum_total" in fields
        assert "main_state" in fields

    def test_non_diff_fields_ignored(self):
        """Changes to fields outside DIFF_FIELDS are not reported."""
        old = {"sec_number": "801-1"}
        new = {"sec_number": "801-2"}
        assert compute_diffs(1, old, new) == []


# ── get_current_snapshot ────────────────────────────────────────────────────

class TestGetCurrentSnapshot:
    def test_returns_none_when_no_snapshots(self, db):
        assert get_current_snapshot(999_999, db) is None

    def test_returns_most_recent(self, db):
        from models.firm import Firm, FirmSnapshot

        firm = Firm(
            crd_number=700_001, legal_name="X",
            registration_status="Registered",
            last_filing_date=datetime.date(2025, 1, 1),
        )
        db.add(firm)
        db.flush()

        older = FirmSnapshot(
            crd_number=700_001, snapshot_hash="a" * 64, raw_json={},
            synced_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        )
        newer = FirmSnapshot(
            crd_number=700_001, snapshot_hash="b" * 64, raw_json={},
            synced_at=datetime.datetime(2026, 4, 1, tzinfo=datetime.timezone.utc),
        )
        db.add_all([older, newer])
        db.flush()

        result = get_current_snapshot(700_001, db)
        assert result is not None
        assert result.snapshot_hash == "b" * 64


# ── save_snapshot_and_changes ───────────────────────────────────────────────

class TestSaveSnapshotAndChanges:
    def test_persists_snapshot_and_change_rows(self, db):
        from models.firm import Firm, FirmChange, FirmSnapshot

        firm = Firm(
            crd_number=700_002, legal_name="Y",
            registration_status="Registered",
            last_filing_date=datetime.date(2025, 1, 1),
        )
        db.add(firm)
        db.flush()

        diffs = [
            {"field_path": "aum_total", "old_value": "100", "new_value": "200"},
            {"field_path": "main_state", "old_value": "NY", "new_value": "CA"},
        ]
        snap_id = save_snapshot_and_changes(
            crd=700_002, new_hash="c" * 64,
            raw_json={"aum_total": 200, "main_state": "CA"},
            diffs=diffs, db=db,
        )
        db.flush()

        assert snap_id is not None
        snap = db.get(FirmSnapshot, snap_id)
        assert snap is not None
        assert snap.snapshot_hash == "c" * 64

        from sqlalchemy import select
        change_rows = db.scalars(
            select(FirmChange).where(FirmChange.crd_number == 700_002)
        ).all()
        assert len(change_rows) == 2
        assert {c.field_path for c in change_rows} == {"aum_total", "main_state"}
        for c in change_rows:
            assert c.snapshot_to == snap_id

    def test_links_prev_snapshot_id(self, db):
        from models.firm import Firm, FirmChange, FirmSnapshot

        firm = Firm(
            crd_number=700_003, legal_name="Z",
            registration_status="Registered",
            last_filing_date=datetime.date(2025, 1, 1),
        )
        db.add(firm)
        db.flush()

        # Create a prior snapshot so the FK target actually exists.
        prev = FirmSnapshot(
            crd_number=700_003, snapshot_hash="e" * 64, raw_json={},
            synced_at=datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc),
        )
        db.add(prev)
        db.flush()

        snap_id = save_snapshot_and_changes(
            crd=700_003, new_hash="d" * 64, raw_json={}, diffs=[
                {"field_path": "aum_total", "old_value": "100", "new_value": "200"},
            ],
            db=db, prev_snapshot_id=prev.id,
        )
        db.flush()

        from sqlalchemy import select
        change = db.scalars(
            select(FirmChange).where(FirmChange.crd_number == 700_003)
        ).first()
        assert change.snapshot_from == prev.id
        assert change.snapshot_to == snap_id
