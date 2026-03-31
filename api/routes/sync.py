import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from db import get_db
from models.sync_job import SyncJob
from schemas.firm import SyncStatusEntry

log = logging.getLogger(__name__)
router = APIRouter(prefix="/sync", tags=["sync"])

_JOB_TYPES = ["bulk_csv", "monthly_pdf", "live_incremental", "aum_history"]


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
