"""
Route-level tests for /api/schedules.

Covers:
- GET   /api/schedules (list)
- PATCH /api/schedules/{id} (enable/disable, cron fields)
- POST  /api/schedules/{id}/trigger (send_task invocation)
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch



def _make_schedule(db, **overrides):
    from models.cron_schedule import CronSchedule

    defaults = dict(
        name="test-schedule",
        task="some_task.dummy",
        minute="0", hour="0", day_of_month="*",
        month_of_year="*", day_of_week="*",
        enabled=True,
    )
    defaults.update(overrides)
    sched = CronSchedule(**defaults)
    db.add(sched)
    db.flush()
    return sched


class TestListSchedules:
    def test_empty(self, client):
        r = client.get("/api/schedules")
        assert r.status_code == 200
        assert r.json() == []

    def test_returns_all(self, client, db):
        _make_schedule(db, name="a")
        _make_schedule(db, name="b", enabled=False)

        r = client.get("/api/schedules")
        assert r.status_code == 200
        names = [s["name"] for s in r.json()]
        assert set(names) == {"a", "b"}


class TestPatchSchedule:
    def test_disable_schedule(self, client, db):
        sched = _make_schedule(db, name="to-disable", enabled=True)
        r = client.patch(f"/api/schedules/{sched.id}", json={"enabled": False})
        assert r.status_code == 200
        assert r.json()["enabled"] is False

    def test_update_cron_fields(self, client, db):
        sched = _make_schedule(db, name="cron")
        r = client.patch(f"/api/schedules/{sched.id}", json={
            "minute": "30", "hour": "4",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["minute"] == "30"
        assert body["hour"] == "4"

    def test_missing_schedule_returns_404(self, client):
        r = client.patch("/api/schedules/999999", json={"enabled": False})
        assert r.status_code == 404


class TestTriggerSchedule:
    def test_sends_celery_task(self, client, db):
        sched = _make_schedule(db, task="monthly_sync.monthly_data_sync")

        fake_result = MagicMock()
        fake_result.id = "task-abc123"
        with patch(
            "celery_tasks.app.app.send_task", return_value=fake_result,
        ) as mock_send:
            r = client.post(f"/api/schedules/{sched.id}/trigger")

        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "accepted"
        assert body["task_id"] == "task-abc123"
        mock_send.assert_called_once_with("monthly_sync.monthly_data_sync")

    def test_missing_schedule_returns_404(self, client):
        r = client.post("/api/schedules/999999/trigger")
        assert r.status_code == 404
