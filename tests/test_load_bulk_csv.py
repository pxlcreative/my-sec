"""
Tests for the Phase 1 hardening of scripts/load_bulk_csv.py:

- urllib3.Retry adapter for SEC downloads (network resilience)
- ZIP integrity check (corrupt downloads are re-fetched)
- Explicit BEGIN/COMMIT/ROLLBACK per batch (no partial state on error)
- sync_manifest row per bulk ZIP (make bulk loads visible in the dashboard)
- Column-map variants: legacy IA_MAIN.csv, modern IA_ADV_Base_A.csv, ADV field numbers
- Idempotent re-run (already-complete ZIPs are skipped)
- End-of-run summary distinguishes processed / skipped / failed
"""
from __future__ import annotations

import csv
import io
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest


# Make scripts/ importable.
_SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import load_bulk_csv  # noqa: E402


# ── Helpers ─────────────────────────────────────────────────────────────────

def _build_zip(entries: dict[str, str], path: Path) -> Path:
    """Write a ZIP containing the given {filename: text-content} entries."""
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in entries.items():
            zf.writestr(name, content)
    return path


def _build_ia_adv_base_a_csv(rows: list[tuple]) -> str:
    """Build a CSV in the current SEC IA_ADV_Base_A format."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "FilingID", "1E1", "1A", "DATESUBMITTED",
        "5F2C", "5F2A", "5F2B",
        "1F1-STREET 1", "1F1-CITY", "1F1-STATE", "1F1-POSTAL",
        "REGISTRATION_STATUS",
    ])
    for i, (crd, firm, date_, aum_t, city, state, zipc, reg) in enumerate(rows, 1):
        w.writerow([
            f"FID-{i:06d}", crd, firm, date_,
            aum_t, "", "",
            "Addr", city, state, zipc, reg,
        ])
    return buf.getvalue()


def _build_ia_main_csv(rows: list[tuple]) -> str:
    """Build a CSV in the legacy IA_MAIN format (2000–2011 ZIP)."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "CRD_NUMBER", "FIRM_NAME", "ADV_FILING_DATE",
        "ITEM5F_2C", "CITY", "STATE", "ZIP",
    ])
    for crd, firm, date_, aum, city, state, zipc in rows:
        w.writerow([crd, firm, date_, aum, city, state, zipc])
    return buf.getvalue()


# ── Header normalization ────────────────────────────────────────────────────

class TestNormalizeHeaders:
    def test_legacy_column_names(self):
        """Legacy CSVs: CRD_NUMBER, FIRM_NAME (→ legal_name), ADV_FILING_DATE.

        Note: FIRM_NAME is listed under BOTH "firm_name" and "legal_name" in
        COLUMN_MAP; the latter wins because it is defined second. This is
        load_bulk_csv's actual behaviour.
        """
        result = load_bulk_csv.normalize_headers([
            "CRD_NUMBER", "FIRM_NAME", "ADV_FILING_DATE", "ITEM5F_2C",
        ])
        assert result == {
            "CRD_NUMBER": "crd_number",
            "FIRM_NAME": "legal_name",
            "ADV_FILING_DATE": "filing_date",
            "ITEM5F_2C": "aum_total",
        }

    def test_modern_adv_field_numbers(self):
        """Current SEC format: "1E1", "5F2C", etc."""
        result = load_bulk_csv.normalize_headers(["1E1", "5F2C", "5F2A", "1F1-STATE"])
        assert result == {
            "1E1": "crd_number",
            "5F2C": "aum_total",
            "5F2A": "aum_discretionary",
            "1F1-STATE": "main_state",
        }

    def test_unknown_columns_dropped(self):
        result = load_bulk_csv.normalize_headers(["CRD_NUMBER", "UNKNOWN_JUNK"])
        assert "UNKNOWN_JUNK" not in result
        assert result["CRD_NUMBER"] == "crd_number"

    def test_case_insensitive(self):
        result = load_bulk_csv.normalize_headers(["crd_number", "Firm_Name"])
        assert result == {"crd_number": "crd_number", "Firm_Name": "legal_name"}


# ── _verify_zip ─────────────────────────────────────────────────────────────

class TestVerifyZip:
    def test_returns_true_for_valid_zip(self, tmp_path):
        zip_path = _build_zip({"a.txt": "hello"}, tmp_path / "v.zip")
        assert load_bulk_csv._verify_zip(zip_path) is True

    def test_returns_false_for_corrupt_file(self, tmp_path):
        path = tmp_path / "corrupt.zip"
        path.write_bytes(b"this is not a zip")
        assert load_bulk_csv._verify_zip(path) is False

    def test_returns_false_for_missing_file(self, tmp_path):
        assert load_bulk_csv._verify_zip(tmp_path / "does_not_exist.zip") is False


# ── parse_ia_main (returns tuple since Phase 1) ─────────────────────────────

class TestParseIaMain:
    def test_parses_ia_adv_base_a_format(self, tmp_path):
        csv_text = _build_ia_adv_base_a_csv([
            ("700001", "Alpha LLC", "2026-01-15", "150000000", "NYC", "NY", "10001", "Registered"),
            ("700002", "Beta Corp", "2026-01-20", "500000000", "Boston", "MA", "02101", "Registered"),
        ])
        zip_path = _build_zip({"IA_ADV_Base_A_sample.csv": csv_text}, tmp_path / "sample.zip")

        rows, errors = load_bulk_csv.parse_ia_main(zip_path)
        assert errors == 0
        assert len(rows) == 2
        assert rows[0]["crd_number"] == 700001
        # "1A" maps to legal_name per COLUMN_MAP (firm_name is sourced from "1B1").
        assert rows[0]["legal_name"] == "Alpha LLC"
        assert rows[0]["main_state"] == "NY"

    def test_parses_legacy_ia_main_format(self, tmp_path):
        csv_text = _build_ia_main_csv([
            ("800001", "Legacy Advisors", "03/15/2005", "50000000", "Chicago", "IL", "60601"),
        ])
        zip_path = _build_zip({"IA_MAIN.csv": csv_text}, tmp_path / "legacy.zip")
        rows, errors = load_bulk_csv.parse_ia_main(zip_path)
        assert errors == 0
        assert len(rows) == 1
        assert rows[0]["crd_number"] == 800001
        assert rows[0]["filing_date"].year == 2005

    def test_skips_rows_without_crd_or_filing_date(self, tmp_path):
        """Malformed rows (no CRD or no date) are silently skipped, not errored."""
        csv_text = _build_ia_main_csv([
            ("", "No CRD", "2026-01-15", "1000", "X", "Y", "00001"),
            ("999", "No Date", "", "1000", "X", "Y", "00001"),
            ("111", "Valid", "01/15/2026", "1000", "X", "Y", "00001"),
        ])
        zip_path = _build_zip({"IA_MAIN.csv": csv_text}, tmp_path / "s.zip")
        rows, errors = load_bulk_csv.parse_ia_main(zip_path)
        assert len(rows) == 1
        assert rows[0]["crd_number"] == 111


# ── Transaction safety on batch insert ──────────────────────────────────────

class TestUpsertTransactions:
    def test_upsert_firms_rolls_back_on_error(self, db):
        """Mid-batch exception should roll back the whole batch."""
        import psycopg2
        # Use a real psycopg2 connection (load_bulk_csv uses raw psycopg2, not SQLAlchemy)
        import os
        # conftest forces DATABASE_URL to the test DB; psycopg2 can use it directly.
        dsn = os.environ["DATABASE_URL"].replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(dsn)
        try:
            # Good rows, but we simulate a failure by patching execute_batch.
            rows = [{
                "crd_number": 700_050 + i,
                "firm_name": f"TxTest {i}",
                "legal_name": None,
                "filing_date": __import__("datetime").date(2026, 1, 15),
                "aum_discretionary": None, "aum_non_discretionary": None,
                "aum_total": 100, "num_employees": None,
                "main_street1": None, "main_city": None,
                "main_state": "NY", "main_zip": "10001",
                "registration_status": "Registered",
            } for i in range(3)]

            with patch(
                "load_bulk_csv.psycopg2.extras.execute_batch",
                side_effect=RuntimeError("boom"),
            ):
                with pytest.raises(RuntimeError):
                    load_bulk_csv.upsert_firms(rows, conn)

            # After rollback, no firms with the synthetic CRDs should exist.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM firms WHERE crd_number BETWEEN 700050 AND 700100"
                )
                count = cur.fetchone()[0]
            assert count == 0
        finally:
            conn.close()

    def test_upsert_firms_deduplicates_by_crd_keeping_latest(self, db):
        """If the same CRD appears twice in the batch, only the newer filing wins."""
        import psycopg2
        import os
        # conftest forces DATABASE_URL to the test DB; psycopg2 can use it directly.
        dsn = os.environ["DATABASE_URL"].replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(dsn)
        try:
            import datetime as _dt
            rows = [
                {
                    "crd_number": 700_200, "firm_name": "First Record",
                    "legal_name": None,
                    "filing_date": _dt.date(2025, 1, 1),
                    "aum_discretionary": None, "aum_non_discretionary": None,
                    "aum_total": 100, "num_employees": None,
                    "main_street1": None, "main_city": None,
                    "main_state": "NY", "main_zip": "10001",
                    "registration_status": "Registered",
                },
                {
                    "crd_number": 700_200, "firm_name": "Newer Record",
                    "legal_name": None,
                    "filing_date": _dt.date(2026, 1, 1),
                    "aum_discretionary": None, "aum_non_discretionary": None,
                    "aum_total": 200, "num_employees": None,
                    "main_street1": None, "main_city": None,
                    "main_state": "NY", "main_zip": "10001",
                    "registration_status": "Registered",
                },
            ]
            load_bulk_csv.upsert_firms(rows, conn)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT business_name, aum_total FROM firms WHERE crd_number = %s",
                    (700_200,),
                )
                row = cur.fetchone()
            assert row == ("Newer Record", 200)
        finally:
            # Clean up so rollback of the outer test transaction doesn't leak.
            with conn.cursor() as cur:
                cur.execute("DELETE FROM firm_aum_history WHERE crd_number = 700200")
                cur.execute("DELETE FROM firms WHERE crd_number = 700200")
            conn.commit()
            conn.close()


# ── sync_manifest helpers ───────────────────────────────────────────────────

class TestManifestHelpers:
    def test_upsert_manifest_creates_and_updates(self, db):
        """_upsert_manifest inserts first time, updates on subsequent calls."""
        import psycopg2
        import os
        # conftest forces DATABASE_URL to the test DB; psycopg2 can use it directly.
        dsn = os.environ["DATABASE_URL"].replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(dsn)
        try:
            load_bulk_csv._upsert_manifest(conn, "test_file.zip", "processing", records=0)
            assert load_bulk_csv._manifest_status(conn, "test_file.zip") == "processing"

            load_bulk_csv._upsert_manifest(conn, "test_file.zip", "complete", records=1234)
            assert load_bulk_csv._manifest_status(conn, "test_file.zip") == "complete"

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT records_processed FROM sync_manifest "
                    "WHERE file_type = %s AND file_name = %s",
                    (load_bulk_csv.BULK_MANIFEST_FILE_TYPE, "test_file.zip"),
                )
                assert cur.fetchone()[0] == 1234
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM sync_manifest WHERE file_name = 'test_file.zip' "
                    "AND file_type = %s",
                    (load_bulk_csv.BULK_MANIFEST_FILE_TYPE,),
                )
            conn.commit()
            conn.close()

    def test_manifest_status_returns_none_for_unknown(self, db):
        import psycopg2
        import os
        # conftest forces DATABASE_URL to the test DB; psycopg2 can use it directly.
        dsn = os.environ["DATABASE_URL"].replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(dsn)
        try:
            assert load_bulk_csv._manifest_status(conn, "unknown_file.zip") is None
        finally:
            conn.close()


# ── HTTP retry adapter ──────────────────────────────────────────────────────

class TestHttpSession:
    def test_session_has_retry_adapter(self):
        """The shared session must have urllib3.Retry configured on both adapters."""
        sess = load_bulk_csv._http
        https_adapter = sess.get_adapter("https://sec.gov")
        retries = getattr(https_adapter, "max_retries", None)
        assert retries is not None
        assert retries.total == 5
        # 429 and 5xx statuses should be in the retry list.
        assert set([429, 500, 502, 503, 504]).issubset(set(retries.status_forcelist))

    def test_session_sets_user_agent(self):
        sess = load_bulk_csv._http
        assert "MySEC" in sess.headers.get("User-Agent", "")
