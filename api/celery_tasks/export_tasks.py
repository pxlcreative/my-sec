"""
Celery tasks for async export jobs and expired-file cleanup.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

from celery_tasks.app import app

log = logging.getLogger(__name__)


@app.task(bind=True, name="export_tasks.run_export_job", max_retries=2)
def run_export_job(self, job_id: str) -> dict:
    """Execute a single export job: query → format → write file → update status."""
    from db import SessionLocal
    from services.export_service import run_export_job as _run

    with SessionLocal() as session:
        try:
            _run(job_id, session)
            return {"job_id": job_id, "status": "complete"}
        except Exception as exc:
            log.exception("Export job %s failed (attempt %d)", job_id, self.request.retries + 1)
            raise self.retry(exc=exc, countdown=15) if self.request.retries < self.max_retries else exc


@app.task(name="export_tasks.cleanup_expired_exports")
def cleanup_expired_exports() -> dict:
    """
    Celery beat task (runs periodically).
    Deletes expired export files from disk and marks their ExportJob records as 'expired'.
    """
    from db import SessionLocal
    from models.export_job import ExportJob
    from sqlalchemy import select

    deleted_files = 0
    marked_expired = 0

    with SessionLocal() as session:
        now = datetime.now(timezone.utc)
        stmt = select(ExportJob).where(
            ExportJob.status == "complete",
            ExportJob.expires_at <= now,
        )
        jobs = list(session.scalars(stmt).all())

        for job in jobs:
            if job.file_path:
                try:
                    p = Path(job.file_path)
                    if p.exists():
                        p.unlink()
                        deleted_files += 1
                except OSError:
                    log.warning("Could not delete export file %s", job.file_path)
            job.status = "expired"
            job.file_path = None
            marked_expired += 1

        session.commit()

    log.info(
        "cleanup_expired_exports: marked %d expired, deleted %d files",
        marked_expired,
        deleted_files,
    )
    return {"marked_expired": marked_expired, "deleted_files": deleted_files}
