"""
Smoke test for every test-infrastructure fixture added in Phase 2.

If any of these fail, every other test that relies on these fixtures will
also fail, so this file runs first alphabetically by design (fixtures_).
Tests here should be small and direct: prove the fixture does what its
docstring claims, nothing more.
"""
from __future__ import annotations

import datetime
import zipfile

import pytest
import requests

from tests.fixtures.firms import (
    firm_factory, inactive_firm, make_firms, registered_firm, withdrawn_firm,
)
from tests.fixtures.rules import (
    aum_decline_rule, deregistration_rule, field_change_rule,
)


# ── Core DB isolation ───────────────────────────────────────────────────────

class TestDbIsolation:
    def test_db_is_test_db(self):
        """Confirm we're talking to the isolated test DB, not production."""
        import os
        from sqlalchemy.engine.url import make_url
        url = make_url(os.environ["DATABASE_URL"])
        assert "test" in (url.database or "")

    def test_rollback_isolates_tests(self, db):
        """Writes in one test must not be visible in another."""
        from models.firm import Firm
        f = Firm(
            crd_number=999_001, legal_name="Rollback Check",
            registration_status="Registered",
            last_filing_date=datetime.date(2025, 1, 1),
        )
        db.add(f)
        db.flush()
        assert db.get(Firm, 999_001) is not None
        # Teardown rolls back; next test below confirms it's gone.

    def test_rollback_actually_happened(self, db):
        from models.firm import Firm
        assert db.get(Firm, 999_001) is None


# ── Firm factories ──────────────────────────────────────────────────────────

class TestFirmFactories:
    def test_firm_factory_returns_persisted_firm(self, db):
        firm = firm_factory(db, legal_name="Factory Made LLC", main_state="CA")
        assert firm.crd_number is not None
        assert firm.legal_name == "Factory Made LLC"
        assert firm.main_state == "CA"

    def test_registered_firm_defaults(self, db):
        firm = registered_firm(db)
        assert firm.registration_status == "Registered"
        assert firm.last_filing_date and firm.last_filing_date.year == 2025

    def test_withdrawn_firm_defaults(self, db):
        firm = withdrawn_firm(db)
        assert firm.registration_status == "Withdrawn"
        assert firm.aum_total is None

    def test_inactive_firm_defaults(self, db):
        firm = inactive_firm(db)
        assert firm.registration_status == "Inactive"
        assert firm.last_filing_date and firm.last_filing_date.year < 2025

    def test_make_firms_bulk(self, db):
        firms = make_firms(db, 3, main_state="TX")
        assert len(firms) == 3
        assert {f.main_state for f in firms} == {"TX"}
        assert len({f.crd_number for f in firms}) == 3  # unique CRDs


# ── Alert-rule factories ────────────────────────────────────────────────────

class TestRuleFactories:
    def test_deregistration_rule(self, db):
        rule = deregistration_rule(db, label="Test dereg")
        assert rule.id is not None
        assert rule.rule_type == "deregistration"
        assert rule.delivery == "in_app"

    def test_aum_decline_rule(self, db):
        rule = aum_decline_rule(db, threshold_pct=-30.0, operator="lt")
        assert rule.rule_type == "aum_decline_pct"
        assert float(rule.threshold_pct) == -30.0

    def test_field_change_rule(self, db):
        rule = field_change_rule(db, field_path="main_city", match_new_value="Austin")
        assert rule.rule_type == "field_change"
        assert rule.field_path == "main_city"


# ── mock_iapd ───────────────────────────────────────────────────────────────

class TestMockIapd:
    def test_returns_canned_response_for_known_crd(self, mock_iapd):
        resp = mock_iapd.fetch(100001)
        assert resp["basicInformation"]["firmId"] == 100001
        assert resp["basicInformation"]["firmName"].startswith("Acme")

    def test_raises_for_missing_crd(self, mock_iapd):
        with pytest.raises(ValueError, match="No IAPD"):
            mock_iapd.fetch(999_999)

    def test_raises_for_registered_error(self, mock_iapd):
        with pytest.raises(RuntimeError, match="rate limit"):
            mock_iapd.fetch(100429)

    def test_records_calls(self, mock_iapd):
        mock_iapd.fetch(100001)
        mock_iapd.fetch(100002)
        assert mock_iapd.calls == [100001, 100002]

    def test_register_adds_mapping(self, mock_iapd):
        mock_iapd.register(777, {"basicInformation": {"firmId": 777, "firmName": "Injected"}})
        resp = mock_iapd.fetch(777)
        assert resp["basicInformation"]["firmName"] == "Injected"

    def test_patches_iapd_client_module(self, mock_iapd):
        """The fixture must patch services.iapd_client.fetch_firm itself."""
        from services.iapd_client import fetch_firm
        resp = fetch_firm(100001)  # should hit mock, not real SEC
        assert resp["basicInformation"]["firmId"] == 100001


# ── mock_es ─────────────────────────────────────────────────────────────────

class TestMockEs:
    def test_seed_and_search(self, mock_es):
        mock_es.seed({
            "crd_number": 1, "legal_name": "Alpha LLC",
            "main_city": "Boston", "main_state": "MA",
        })
        hits = mock_es.search_calls  # baseline
        from services.es_client import search_firms
        results = search_firms("alpha")
        assert len(results) == 1
        assert results[0]["crd_number"] == 1
        assert mock_es.search_calls == hits + 1

    def test_state_filter(self, mock_es):
        mock_es.seed_many([
            {"crd_number": 1, "legal_name": "Firm A", "main_state": "CA"},
            {"crd_number": 2, "legal_name": "Firm B", "main_state": "NY"},
        ])
        from services.es_client import search_firms
        results = search_firms("firm", state="CA")
        assert [r["crd_number"] for r in results] == [1]

    def test_bulk_index_records_count(self, mock_es):
        from services.es_client import bulk_index_firms
        docs = [
            {"crd_number": 10, "legal_name": "Ten"},
            {"crd_number": 11, "legal_name": "Eleven"},
        ]
        assert bulk_index_firms(docs) == 2
        assert len(mock_es.docs) == 2


# ── mock_smtp ───────────────────────────────────────────────────────────────

class TestMockSmtp:
    def test_captures_outbound_mail(self, mock_smtp):
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText("Body")
        msg["Subject"] = "Hello"
        msg["From"] = "a@x.com"
        msg["To"] = "b@x.com"
        with smtplib.SMTP("mail.local", 587) as s:
            s.starttls()
            s.login("user", "pass")
            s.send_message(msg)
        assert len(mock_smtp.messages) == 1
        assert mock_smtp.starttls_called is True
        assert mock_smtp.login_calls == [("user", "pass")]
        assert mock_smtp.connections == [("mail.local", 587)]


# ── mock_sec_requests ───────────────────────────────────────────────────────

class TestMockSecRequests:
    def test_serves_metadata(self, mock_sec_requests):
        r = requests.get(
            "https://reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json"
        )
        assert r.status_code == 200
        body = r.json()
        # Real SEC structure: top-level keys are file types, each with year → files.
        assert "advFilingData" in body
        assert "advW" in body

    def test_serves_advfilingdata_zip(self, tmp_path, mock_sec_requests):
        r = requests.get(
            "https://reports.adviserinfo.sec.gov/reports/foia/advFilingData/2026/advFilingData_2026_01.zip"
        )
        assert r.status_code == 200
        # Content is a valid ZIP containing a CSV
        zpath = tmp_path / "t.zip"
        zpath.write_bytes(r.content)
        with zipfile.ZipFile(zpath) as zf:
            csvs = [n for n in zf.namelist() if n.endswith(".csv")]
            assert csvs, "no CSV in fixture ZIP"

    def test_serves_advw_zip(self, mock_sec_requests):
        r = requests.get(
            "https://reports.adviserinfo.sec.gov/reports/foia/advW/2026/advW_2026_01.zip"
        )
        assert r.status_code == 200
        assert r.content[:2] == b"PK"  # ZIP magic bytes


# ── celery_eager ────────────────────────────────────────────────────────────

class TestCeleryEager:
    def test_delay_runs_inline(self, celery_eager):
        from celery_tasks.app import app

        @app.task(name="tests.smoke.echo")
        def echo(x):
            return x * 2

        result = echo.delay(21)
        assert result.get(timeout=5) == 42

    def test_restores_config_after(self, celery_eager):
        """Inside the fixture, always_eager is True."""
        assert celery_eager.conf.task_always_eager is True


def test_celery_eager_config_restored_after_fixture():
    """Outside the fixture, always_eager is False again."""
    from celery_tasks.app import app
    assert app.conf.task_always_eager is False


# ── tmp_data_dir ────────────────────────────────────────────────────────────

class TestTmpDataDir:
    def test_points_settings_at_tmp_path(self, tmp_data_dir):
        from config import settings
        assert settings.data_dir == str(tmp_data_dir)

    def test_subdirs_exist(self, tmp_data_dir):
        assert (tmp_data_dir / "raw" / "csv").is_dir()
        assert (tmp_data_dir / "exports").is_dir()
        assert (tmp_data_dir / "brochures").is_dir()


# ── frozen_time ─────────────────────────────────────────────────────────────

class TestFrozenTime:
    def test_date_today_is_frozen(self, frozen_time):
        assert datetime.date.today() == datetime.date(2026, 4, 21)

    def test_datetime_now_is_frozen(self, frozen_time):
        assert datetime.datetime.now().date() == datetime.date(2026, 4, 21)

    def test_move_to_jumps_forward(self, frozen_time):
        frozen_time.move_to("2027-01-01")
        assert datetime.date.today() == datetime.date(2027, 1, 1)


# ── migration_engine ────────────────────────────────────────────────────────

class TestMigrationEngine:
    def test_migration_db_has_view(self, migration_engine):
        """The migration-provisioned DB has the firm_aum_annual view."""
        from sqlalchemy import text
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM information_schema.views "
                    "WHERE table_name = 'firm_aum_annual'"
                )
            ).first()
        assert row is not None, "firm_aum_annual view missing from migration-backed DB"
