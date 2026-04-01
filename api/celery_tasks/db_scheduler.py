from __future__ import annotations

import logging
import time

from celery.beat import PersistentScheduler
from celery.schedules import crontab
from sqlalchemy import create_engine, text

from config import settings

logger = logging.getLogger(__name__)

# Own engine — Beat runs in a separate process from the FastAPI app
_engine = create_engine(settings.database_url, pool_pre_ping=True)


class DatabaseScheduler(PersistentScheduler):
    """Celery Beat scheduler that reads schedules from the cron_schedules DB table.

    Reloads every _db_check_interval seconds so that edits made via the API
    take effect without restarting the Beat process.
    """

    _db_check_interval: float = 60.0
    _last_db_check: float | None = None

    def setup_schedule(self) -> None:
        super().setup_schedule()
        self._sync_from_db()

    def tick(self, event_t=..., **kwargs):  # type: ignore[override]
        now = time.monotonic()
        if self._last_db_check is None or now - self._last_db_check >= self._db_check_interval:
            self._sync_from_db()
            self._last_db_check = now
        return super().tick(event_t=event_t, **kwargs)

    def _sync_from_db(self) -> None:
        try:
            with _engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT name, task, minute, hour, day_of_month, "
                        "month_of_year, day_of_week "
                        "FROM cron_schedules WHERE enabled = true"
                    )
                ).fetchall()
        except Exception:
            logger.exception("DatabaseScheduler: failed to load schedules from DB — keeping existing schedule")
            return

        active_names: set[str] = set()

        for row in rows:
            entry = self.Entry.from_entry(
                row.name,
                entry={
                    "task": row.task,
                    "schedule": crontab(
                        minute=row.minute,
                        hour=row.hour,
                        day_of_month=row.day_of_month,
                        month_of_year=row.month_of_year,
                        day_of_week=row.day_of_week,
                    ),
                },
                app=self.app,
            )
            self.data[row.name] = entry
            active_names.add(row.name)

        # Remove schedules that have been disabled or deleted in the DB
        for name in [k for k in list(self.data) if k not in active_names and not k.startswith("celery.")]:
            logger.info("DatabaseScheduler: removing disabled/deleted schedule %r", name)
            del self.data[name]

        logger.debug("DatabaseScheduler: loaded %d schedule(s) from DB", len(active_names))
