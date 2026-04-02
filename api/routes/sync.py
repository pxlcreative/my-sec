import logging
import traceback
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from db import SessionLocal, get_db
from models.firm import Firm
from models.sync_job import SyncJob
from schemas.firm import SyncStatusEntry

log = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["sync"])

_JOB_TYPES = ["bulk_csv", "monthly_data", "brochure_sync", "live_incremental", "aum_history"]


# ---------------------------------------------------------------------------
# GET /api/sync/status
# ---------------------------------------------------------------------------

@router.get(
    "/status",
    response_model=list[SyncStatusEntry],
    summary="Sync job status",
    description="Returns the most recent SyncJob record per job type. Returns [] when no jobs have run yet.",
)
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


# ---------------------------------------------------------------------------
# GET /api/sync/jobs  — full job history
# GET /api/sync/jobs/{job_id}  — single job detail (includes results/log)
# ---------------------------------------------------------------------------

@router.get(
    "/jobs",
    response_model=list[SyncStatusEntry],
    summary="Sync job history",
    description="Returns all sync jobs ordered by creation time descending.",
)
def list_sync_jobs(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
) -> list[SyncStatusEntry]:
    try:
        jobs = db.scalars(
            select(SyncJob).order_by(desc(SyncJob.created_at)).limit(limit)
        ).all()
        return [SyncStatusEntry.model_validate(j) for j in jobs]
    except Exception:
        log.error("list_sync_jobs error\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/jobs/{job_id}",
    response_model=SyncStatusEntry,
    summary="Sync job detail",
    description="Returns full detail for a single sync job, including the results/log field.",
)
def get_sync_job(job_id: int, db: Session = Depends(get_db)) -> SyncStatusEntry:
    try:
        job = db.get(SyncJob, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return SyncStatusEntry.model_validate(job)
    except HTTPException:
        raise
    except Exception:
        log.error("get_sync_job(%s) error\n%s", job_id, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post(
    "/jobs/{job_id}/cancel",
    summary="Cancel a sync job",
    description="Revokes the Celery task (if still queued or running) and marks the job cancelled.",
)
def cancel_sync_job(job_id: int, db: Session = Depends(get_db)) -> dict:
    try:
        job = db.get(SyncJob, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status not in ("pending", "running"):
            raise HTTPException(
                status_code=400,
                detail=f"Job is already {job.status} and cannot be cancelled",
            )

        # Revoke the Celery task if we have the task ID
        task_id = (job.results or {}).get("task_id")
        if task_id:
            from celery_tasks.app import app as celery_app
            celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
            log.info("cancel_sync_job: revoked task %s for job %d", task_id, job_id)

        job.status = "cancelled"
        job.completed_at = datetime.now(timezone.utc)
        db.commit()

        return {"job_id": job_id, "status": "cancelled"}
    except HTTPException:
        raise
    except Exception:
        log.error("cancel_sync_job(%s) error\n%s", job_id, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/trigger", status_code=202)
def trigger_monthly_sync(
    db: Session = Depends(get_db),
) -> dict:
    """
    Enqueue an immediate monthly data sync Celery task.
    Creates a pending SyncJob immediately so the UI shows the job before the worker starts.
    The sync discovers all pending files from reports_metadata.json automatically.
    """
    from celery_tasks.monthly_sync import monthly_data_sync
    from sqlalchemy.orm.attributes import flag_modified

    job = SyncJob(
        job_type="monthly_data",
        status="pending",
        source_url="reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json",
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    task = monthly_data_sync.delay(job_id=job.id)

    job.results = {"task_id": task.id}
    flag_modified(job, "results")
    db.commit()

    return {
        "status": "accepted",
        "task_id": task.id,
        "job_id": job.id,
        "message": "Monthly data sync enqueued. Check /api/sync/status for progress.",
    }


@router.post("/reindex", status_code=202)
def trigger_reindex(background_tasks: BackgroundTasks) -> dict:
    """
    Trigger a full re-index of all firms into Elasticsearch.
    Runs as a background task; returns immediately with 202 Accepted.
    """
    background_tasks.add_task(_run_reindex)
    return {"status": "accepted", "message": "Reindex started in background"}
