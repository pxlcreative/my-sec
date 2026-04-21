"""
Celery task for async bulk name+address → CRD matching.
"""
import logging
from datetime import datetime, timezone

from celery_tasks.app import app

log = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="celery_tasks.match_tasks.run_bulk_match",
    max_retries=2,
    default_retry_delay=60,
)
def run_bulk_match(self, job_id: int, records: list[dict], options: dict) -> None:
    """
    Runs match_batch for an async job, writing status and results back to sync_jobs.

    Retries up to 2 times on transient failures (ES unreachable, broker reconnect).
    """
    from db import SessionLocal
    from models.sync_job import SyncJob
    from services.matcher import match_batch

    db = SessionLocal()
    try:
        job: SyncJob | None = db.get(SyncJob, job_id)
        if job is None:
            log.error("run_bulk_match: job %d not found", job_id)
            return
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        db.commit()

        min_score = float(options.get("min_score", 50.0))
        max_candidates = int(options.get("max_candidates", 3))

        output = match_batch(records, min_score=min_score, max_candidates=max_candidates)

        job = db.get(SyncJob, job_id)  # re-fetch after potential commit staleness
        job.status = "complete"
        job.completed_at = datetime.now(timezone.utc)
        job.firms_processed = output["stats"]["total"]
        job.firms_updated = output["stats"]["confirmed"] + output["stats"]["probable"]
        job.results = output
        db.commit()

        log.info(
            "run_bulk_match job=%d complete: total=%d confirmed=%d probable=%d",
            job_id,
            output["stats"]["total"],
            output["stats"]["confirmed"],
            output["stats"]["probable"],
        )

    except Exception as exc:
        log.exception("run_bulk_match job=%d failed (attempt %d)", job_id, self.request.retries + 1)

        # Retry transient failures. If we've exhausted retries, persist the
        # failure to the SyncJob so the UI shows a clear error.
        if self.request.retries < self.max_retries:
            raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))

        try:
            db.rollback()
            job = db.get(SyncJob, job_id)
            if job:
                job.status = "failed"
                job.error_message = str(exc)
                job.completed_at = datetime.now(timezone.utc)
                db.commit()
        except Exception:
            log.exception("run_bulk_match: could not update job status to failed")
    finally:
        db.close()
