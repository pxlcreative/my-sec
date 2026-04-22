"""
Route-level tests for /api/sync.

Covers:
- GET  /api/sync/status  (most recent job per job_type)
- GET  /api/sync/jobs    (history, limit)
- GET  /api/sync/jobs/{id}  (detail + 404)
- POST /api/sync/jobs/{id}/cancel (success, wrong-state 400, 404)
- POST /api/sync/trigger (creates SyncJob + enqueues)
- POST /api/sync/reindex (BackgroundTasks)
"""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pytest


def _make_job(db, **overrides):
    from models.sync_job import SyncJob

    defaults = dict(
        job_type="monthly_data", status="complete",
        firms_processed=0, firms_updated=0,
        started_at=datetime.datetime(2026, 4, 1, tzinfo=datetime.timezone.utc),
        completed_at=datetime.datetime(2026, 4, 1, 0, 10, tzinfo=datetime.timezone.utc),
    )
    defaults.update(overrides)
    job = SyncJob(**defaults)
    db.add(job)
    db.flush()
    return job


# ── /api/sync/status ────────────────────────────────────────────────────────

class TestSyncStatus:
    def test_empty_returns_empty_list(self, client):
        r = client.get("/api/sync/status")
        assert r.status_code == 200
        assert r.json() == []

    def test_returns_latest_per_job_type(self, client, db):
        """The route orders by created_at DESC and takes the first per job_type."""
        # Explicit created_at so the ordering is deterministic even within
        # a single test transaction.
        _make_job(
            db, job_type="monthly_data",
            created_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            started_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        )
        newer = _make_job(
            db, job_type="monthly_data",
            created_at=datetime.datetime(2026, 4, 1, tzinfo=datetime.timezone.utc),
            started_at=datetime.datetime(2026, 4, 1, tzinfo=datetime.timezone.utc),
        )
        other = _make_job(
            db, job_type="bulk_csv",
            created_at=datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc),
        )

        r = client.get("/api/sync/status")
        assert r.status_code == 200
        rows = r.json()
        ids = {row["id"] for row in rows}
        assert newer.id in ids
        assert other.id in ids


# ── /api/sync/jobs ──────────────────────────────────────────────────────────

class TestSyncJobs:
    def test_list_returns_empty(self, client):
        r = client.get("/api/sync/jobs")
        assert r.status_code == 200
        assert r.json() == []

    def test_list_respects_limit(self, client, db):
        for i in range(5):
            _make_job(db, job_type="monthly_data")

        r = client.get("/api/sync/jobs?limit=2")
        assert r.status_code == 200
        assert len(r.json()) == 2

    def test_get_single_job(self, client, db):
        job = _make_job(db, job_type="monthly_data", firms_updated=42)

        r = client.get(f"/api/sync/jobs/{job.id}")
        assert r.status_code == 200
        body = r.json()
        assert body["id"] == job.id
        assert body["firms_updated"] == 42

    def test_missing_job_returns_404(self, client):
        r = client.get("/api/sync/jobs/999999")
        assert r.status_code == 404


# ── /api/sync/jobs/{id}/cancel ──────────────────────────────────────────────

class TestCancelSyncJob:
    def test_cancel_pending_job(self, client, db):
        job = _make_job(db, job_type="monthly_data", status="pending")

        with patch("celery_tasks.app.app.control") as mock_control:
            r = client.post(f"/api/sync/jobs/{job.id}/cancel")

        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "cancelled"

    def test_cannot_cancel_complete_job(self, client, db):
        job = _make_job(db, job_type="monthly_data", status="complete")
        r = client.post(f"/api/sync/jobs/{job.id}/cancel")
        assert r.status_code == 400
        assert "complete" in r.json()["detail"]

    def test_cancel_missing_job_returns_404(self, client):
        r = client.post("/api/sync/jobs/999999/cancel")
        assert r.status_code == 404


# ── /api/sync/trigger ───────────────────────────────────────────────────────

class TestTriggerMonthlySync:
    def test_creates_job_and_enqueues(self, client, db):
        from models.sync_job import SyncJob
        from sqlalchemy import select

        fake_task = MagicMock()
        fake_task.id = "task-xyz"
        with patch(
            "celery_tasks.monthly_sync.monthly_data_sync.delay",
            return_value=fake_task,
        ) as mock_delay:
            r = client.post("/api/sync/trigger")

        assert r.status_code == 202
        body = r.json()
        assert body["status"] == "accepted"
        assert body["task_id"] == "task-xyz"

        # SyncJob row should be created with status "pending" and the task_id.
        job = db.get(SyncJob, body["job_id"])
        assert job is not None
        assert job.job_type == "monthly_data"
        assert (job.results or {}).get("task_id") == "task-xyz"
        mock_delay.assert_called_once_with(job_id=job.id)


# ── /api/sync/reindex ───────────────────────────────────────────────────────

class TestTriggerReindex:
    def test_returns_accepted(self, client):
        """Reindex uses BackgroundTasks — we just assert the 202 + message."""
        with patch("routes.sync._run_reindex") as mock_run:
            r = client.post("/api/sync/reindex")
        assert r.status_code == 202
        assert "Reindex" in r.json()["message"]
        # FastAPI runs the background task after the response. Give it a chance.
        mock_run.assert_called()
