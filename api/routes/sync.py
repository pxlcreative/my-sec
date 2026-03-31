import logging
import traceback

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from db import SessionLocal, get_db
from models.firm import Firm
from models.sync_job import SyncJob
from schemas.firm import SyncStatusEntry

log = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["sync"])

_JOB_TYPES = ["bulk_csv", "monthly_pdf", "live_incremental", "aum_history"]


# ---------------------------------------------------------------------------
# GET /api/sync/status
# ---------------------------------------------------------------------------

@router.get("/status", response_model=list[SyncStatusEntry])
def sync_status(db: Session = Depends(get_db)) -> list[SyncStatusEntry]:
    """Return the most recent SyncJob record per job_type."""
    try:
        results = []
        for job_type in _JOB_TYPES:
            job = db.scalars(
                select(SyncJob)
                .where(SyncJob.job_type == job_type)
                .order_by(desc(SyncJob.created_at))
                .limit(1)
            ).first()
            if job:
                results.append(SyncStatusEntry.model_validate(job))
        return results
    except Exception:
        log.error("sync_status error\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# POST /api/sync/reindex   (admin; no auth for now)
# ---------------------------------------------------------------------------

def _run_reindex() -> None:
    """Background task: stream all firms from Postgres and bulk-index into ES."""
    from services.es_client import bulk_index_firms, create_index_if_not_exists

    log.info("Reindex task started")
    try:
        create_index_if_not_exists()
    except Exception as exc:
        log.error("reindex: could not create ES index: %s", exc)
        return

    db = SessionLocal()
    try:
        BATCH = 1000
        offset = 0
        total = 0
        while True:
            firms = db.scalars(
                select(Firm).order_by(Firm.crd_number).offset(offset).limit(BATCH)
            ).all()
            if not firms:
                break
            docs = [
                {
                    "crd_number":          f.crd_number,
                    "legal_name":          f.legal_name,
                    "business_name":       f.business_name,
                    "main_street1":        f.main_street1,
                    "main_city":           f.main_city,
                    "main_state":          f.main_state,
                    "main_zip":            f.main_zip,
                    "registration_status": f.registration_status,
                }
                for f in firms
            ]
            indexed = bulk_index_firms(docs)
            total += indexed
            offset += BATCH
            if total % 5000 < BATCH:
                log.info("Reindex progress: %d firms indexed", total)
        log.info("Reindex complete: %d firms indexed", total)
    except Exception:
        log.error("Reindex task failed\n%s", traceback.format_exc())
    finally:
        db.close()


@router.post("/trigger", status_code=202)
def trigger_monthly_sync(month_str: str | None = None) -> dict:
    """
    Enqueue an immediate monthly PDF sync Celery task.
    Optional *month_str* query param (format: "YYYY-MM"); defaults to the previous month.
    """
    from celery_tasks.monthly_sync import monthly_pdf_sync

    task = monthly_pdf_sync.delay(month_str)
    return {
        "status": "accepted",
        "task_id": task.id,
        "month": month_str or "previous month",
        "message": "Monthly PDF sync enqueued. Check /api/sync/status for progress.",
    }


@router.post("/reindex", status_code=202)
def trigger_reindex(background_tasks: BackgroundTasks) -> dict:
    """
    Trigger a full re-index of all firms into Elasticsearch.
    Runs as a background task; returns immediately with 202 Accepted.
    """
    background_tasks.add_task(_run_reindex)
    return {"status": "accepted", "message": "Reindex started in background"}
