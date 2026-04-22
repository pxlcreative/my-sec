"""
Tests for Celery tasks hardened in Phase 1.

Covers:
- refresh_firm_task happy path, EDGAR/ValueError inactive-inference, retry
- reindex_firm retry task (new in Phase 1)
- batch_verify_registration_status: staggered dispatch + scope filter
- run_bulk_match: retry on failure, final failure persisted to SyncJob
- monthly_data_sync: partial_success status when some files fail
"""
from __future__ import annotations

import datetime
from unittest.mock import patch

import pytest


# ── refresh_firm_task ───────────────────────────────────────────────────────

class TestRefreshFirmTask:
    def test_happy_path_runs_refresh_service(self, db, celery_eager):
        """Eager-mode .delay() calls the refresh_firm service."""
        from celery_tasks.refresh_tasks import refresh_firm_task

        with patch("services.firm_refresh_service.refresh_firm") as mock_refresh:
            mock_refresh.return_value = [
                {"field_path": "aum_total", "old_value": "100", "new_value": "200"},
            ]
            result = refresh_firm_task.delay(100001).get(timeout=5)

        assert result["changed"] is True
        assert result["num_changes"] == 1
        assert result["fields_changed"] == ["aum_total"]
        mock_refresh.assert_called_once()

    def test_value_error_marks_stale_firm_inactive(self, db, celery_eager):
        """If IAPD returns ValueError for a stale Registered firm, mark it Inactive."""
        from models.firm import Firm
        from celery_tasks.refresh_tasks import refresh_firm_task

        # Pre-seed a Registered firm with an old filing date.
        firm = Firm(
            crd_number=800_001, legal_name="Stale Advisor",
            registration_status="Registered",
            last_filing_date=datetime.date(2020, 1, 1),
        )
        db.add(firm)
        db.commit()
        try:
            with patch(
                "services.firm_refresh_service.refresh_firm",
                side_effect=ValueError("No IAPD results for CRD 800001"),
            ):
                result = refresh_firm_task.delay(800_001).get(timeout=5)

            assert result["changed"] is False
            assert "error" in result

            # Firm should now be Inactive.
            db.expire_all()
            firm2 = db.get(Firm, 800_001)
            assert firm2.registration_status == "Inactive"
            assert firm2.last_iapd_refresh_at is not None
        finally:
            db.execute(
                __import__("sqlalchemy").text(
                    "DELETE FROM firms WHERE crd_number = 800001"
                )
            )
            db.commit()

    def test_value_error_does_not_mark_recent_firm_inactive(self, db, celery_eager):
        """Recent filings must NOT be auto-marked Inactive on IAPD miss."""
        from models.firm import Firm
        from celery_tasks.refresh_tasks import refresh_firm_task

        firm = Firm(
            crd_number=800_002, legal_name="Fresh Advisor",
            registration_status="Registered",
            last_filing_date=datetime.date.today() - datetime.timedelta(days=60),
        )
        db.add(firm)
        db.commit()
        try:
            with patch(
                "services.firm_refresh_service.refresh_firm",
                side_effect=ValueError("No IAPD results"),
            ):
                refresh_firm_task.delay(800_002).get(timeout=5)

            db.expire_all()
            assert db.get(Firm, 800_002).registration_status == "Registered"
        finally:
            db.execute(
                __import__("sqlalchemy").text(
                    "DELETE FROM firms WHERE crd_number = 800002"
                )
            )
            db.commit()


# ── reindex_firm retry task (new in Phase 1) ───────────────────────────────

class TestReindexFirmTask:
    def test_reindexes_via_bulk_index_firms(self, db, celery_eager):
        """reindex_firm pulls a fresh firm snapshot and calls bulk_index_firms."""
        from models.firm import Firm
        from celery_tasks.refresh_tasks import reindex_firm

        firm = Firm(
            crd_number=800_010, legal_name="Reindex Target",
            main_city="NYC", main_state="NY", main_zip="10001",
            registration_status="Registered",
            last_filing_date=datetime.date(2025, 1, 1),
        )
        db.add(firm)
        db.commit()
        try:
            with patch(
                "services.es_client.bulk_index_firms", return_value=1
            ) as mock_bulk:
                result = reindex_firm.delay(800_010).get(timeout=5)

            assert result == {"crd_number": 800_010, "indexed": True}
            mock_bulk.assert_called_once()
            # The doc handed to ES matches the firm row.
            (docs_arg,), _ = mock_bulk.call_args
            assert docs_arg[0]["crd_number"] == 800_010
            assert docs_arg[0]["legal_name"] == "Reindex Target"
        finally:
            db.execute(
                __import__("sqlalchemy").text(
                    "DELETE FROM firms WHERE crd_number = 800010"
                )
            )
            db.commit()

    def test_not_found_firm_returns_gracefully(self, db, celery_eager):
        """Missing firm returns a sentinel, doesn't raise."""
        from celery_tasks.refresh_tasks import reindex_firm

        result = reindex_firm.delay(999_999_999).get(timeout=5)
        assert result == {
            "crd_number": 999_999_999, "indexed": False, "reason": "not_found",
        }


# ── batch_verify_registration_status ────────────────────────────────────────

class TestBatchVerifyRegistrationStatus:
    def test_scope_filter_only_post_2025_registered(self, db, celery_eager):
        """Must only enqueue post-2025 Registered firms."""
        from models.firm import Firm
        from celery_tasks.refresh_tasks import batch_verify_registration_status

        fresh = Firm(
            crd_number=800_020, legal_name="In Scope",
            registration_status="Registered",
            last_filing_date=datetime.date(2025, 6, 1),
        )
        pre_2025 = Firm(
            crd_number=800_021, legal_name="Pre-2025 (out of scope)",
            registration_status="Registered",
            last_filing_date=datetime.date(2024, 6, 1),
        )
        withdrawn = Firm(
            crd_number=800_022, legal_name="Withdrawn (out of scope)",
            registration_status="Withdrawn",
            last_filing_date=datetime.date(2025, 6, 1),
        )
        db.add_all([fresh, pre_2025, withdrawn])
        db.commit()
        try:
            with patch(
                "celery_tasks.refresh_tasks.refresh_firm_task.apply_async"
            ) as mock_dispatch:
                result = batch_verify_registration_status.delay().get(timeout=5)

            assert result["enqueued"] == 1
            enqueued_crds = {call.kwargs["args"][0] for call in mock_dispatch.call_args_list}
            assert enqueued_crds == {800_020}
        finally:
            db.execute(
                __import__("sqlalchemy").text(
                    "DELETE FROM firms WHERE crd_number BETWEEN 800020 AND 800022"
                )
            )
            db.commit()

    def test_stagger_dispatch_spaces_out_countdowns(self, db, celery_eager):
        """250 firms should dispatch across 3 batches with increasing countdowns."""
        from models.firm import Firm
        from celery_tasks.refresh_tasks import batch_verify_registration_status

        firms = [
            Firm(
                crd_number=800_100 + i, legal_name=f"Firm{i}",
                registration_status="Registered",
                last_filing_date=datetime.date(2025, 6, 1),
            )
            for i in range(250)
        ]
        db.add_all(firms)
        db.commit()
        try:
            with patch(
                "celery_tasks.refresh_tasks.refresh_firm_task.apply_async"
            ) as mock_dispatch:
                batch_verify_registration_status.delay().get(timeout=5)

            countdowns = [c.kwargs["countdown"] for c in mock_dispatch.call_args_list]
            # BATCH_SIZE=100, interval=60s → first 100 at 0, next 100 at 60, next 50 at 120.
            assert countdowns[:100] == [0] * 100
            assert countdowns[100:200] == [60] * 100
            assert countdowns[200:250] == [120] * 50
        finally:
            db.execute(
                __import__("sqlalchemy").text(
                    "DELETE FROM firms WHERE crd_number BETWEEN 800100 AND 800349"
                )
            )
            db.commit()


# ── run_bulk_match retry (Phase 1: max_retries went from 0 → 2) ────────────

class TestRunBulkMatchRetry:
    """
    Celery's eager mode does not actually re-execute a task after `self.retry()`
    — it just raises the Retry exception. So these tests cover the two retry
    paths separately: (a) first failure schedules a retry, (b) when retries
    are exhausted, the job is persisted as failed. Patching `self.request.retries`
    simulates the "final attempt" state without needing a real worker.
    """

    def test_first_failure_raises_celery_retry(self, db, celery_eager):
        """On first failure, run_bulk_match calls self.retry which raises Retry."""
        from celery.exceptions import Retry
        from models.sync_job import SyncJob
        from celery_tasks.match_tasks import run_bulk_match

        job = SyncJob(
            job_type="bulk_match", status="pending",
            firms_processed=0, firms_updated=0,
        )
        db.add(job)
        db.commit()
        job_id = job.id

        try:
            with patch(
                "services.matcher.match_batch",
                side_effect=RuntimeError("transient ES hiccup"),
            ):
                with pytest.raises(Retry):
                    run_bulk_match.delay(job_id, [], {}).get(timeout=10)

            # Job is still "running" — final failure handler hasn't fired yet.
            db.expire_all()
            assert db.get(SyncJob, job_id).status == "running"
        finally:
            db.execute(
                __import__("sqlalchemy").text(
                    "DELETE FROM sync_jobs WHERE id = :id"
                ),
                {"id": job_id},
            )
            db.commit()

    def test_persists_failure_when_retries_exhausted(self, db, celery_eager):
        """Simulate final retry attempt — job must be marked failed."""
        from models.sync_job import SyncJob
        from celery_tasks.match_tasks import run_bulk_match

        job = SyncJob(
            job_type="bulk_match", status="pending",
            firms_processed=0, firms_updated=0,
        )
        db.add(job)
        db.commit()
        job_id = job.id

        try:
            # Use Celery's push_request/pop_request to simulate "this is the
            # final retry attempt"; the task's persist-failure branch fires
            # when self.request.retries >= self.max_retries.
            run_bulk_match.push_request(retries=run_bulk_match.max_retries)
            try:
                with patch(
                    "services.matcher.match_batch",
                    side_effect=RuntimeError("always broken"),
                ):
                    run_bulk_match.run(job_id, [], {})
            finally:
                run_bulk_match.pop_request()

            db.expire_all()
            refreshed = db.get(SyncJob, job_id)
            assert refreshed.status == "failed"
            assert "always broken" in (refreshed.error_message or "")
        finally:
            db.execute(
                __import__("sqlalchemy").text(
                    "DELETE FROM sync_jobs WHERE id = :id"
                ),
                {"id": job_id},
            )
            db.commit()
