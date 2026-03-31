"""
Celery task for the monthly ADV Part 2 PDF sync (Module B1).
"""
import logging
from datetime import datetime, timezone

from celery_tasks.app import app

log = logging.getLogger(__name__)


def _prev_month_str() -> str:
    """Return the previous calendar month as 'YYYY-MM'."""
    now = datetime.now(timezone.utc)
    if now.month == 1:
        return f"{now.year - 1}-12"
    return f"{now.year}-{now.month - 1:02d}"


@app.task(bind=True, name="monthly_sync.monthly_pdf_sync", max_retries=1)
def monthly_pdf_sync(self, month_str: str | None = None) -> dict:
    """
    Download and store ADV Part 2 PDFs for *month_str* (defaults to previous month).
    Creates a SyncJob record, delegates to pdf_sync_service.sync_month(), updates status.
    """
    from db import SessionLocal
    from models.sync_job import SyncJob
    from services.pdf_sync_service import sync_month

    target_month = month_str or _prev_month_str()
    log.info("monthly_pdf_sync starting for month=%s", target_month)

    with SessionLocal() as session:
        job = SyncJob(
            job_type="monthly_pdf",
            status="running",
            source_url=f"sec.gov/foia adv-brochures-{target_month}",
            started_at=datetime.now(timezone.utc),
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        job_id = job.id

        try:
            new_crds = sync_month(target_month, session, job_id)

            job = session.get(SyncJob, job_id)
            job.status = "complete"
            job.completed_at = datetime.now(timezone.utc)
            session.commit()

            result = {
                "month": target_month,
                "status": "complete",
                "processed": job.firms_processed,
                "stored": job.firms_updated,
                "refreshes_enqueued": len(new_crds),
            }
            log.info("monthly_pdf_sync %s: %s", target_month, result)

            # Enqueue live IAPD refresh for every firm that received a new brochure
            if new_crds:
                from celery_tasks.refresh_tasks import refresh_firms_with_new_brochures
                refresh_firms_with_new_brochures.delay(list(new_crds))

            return result

        except Exception as exc:
            log.exception("monthly_pdf_sync failed for month=%s", target_month)
            try:
                job = session.get(SyncJob, job_id)
                if job:
                    job.status = "failed"
                    job.error_message = str(exc)
                    job.completed_at = datetime.now(timezone.utc)
                    session.commit()
            except Exception:
                log.exception("monthly_pdf_sync: could not update SyncJob to failed")
            raise self.retry(exc=exc, countdown=300) if self.request.retries < self.max_retries else exc
