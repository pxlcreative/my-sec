import logging
import traceback
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from db import get_db
from models.sync_job import SyncJob
from schemas.match import (
    BulkMatchAsyncResponse,
    BulkMatchRequest,
    BulkMatchSyncResponse,
    MatchJobStatus,
    MatchResult,
    MatchStats,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/match", tags=["match"])

_SYNC_LIMIT = 100
_MAX_RECORDS = 10_000


# ---------------------------------------------------------------------------
# POST /api/match/bulk
# ---------------------------------------------------------------------------

@router.post(
    "/bulk",
    response_model=BulkMatchSyncResponse | BulkMatchAsyncResponse,
    status_code=200,
    summary="Bulk fuzzy CRD match",
    description="Match a list of firm names/addresses to CRD numbers using Elasticsearch + rapidfuzz. ≤100 records run synchronously; >100 are queued as a Celery job.",
)
def bulk_match(
    body: BulkMatchRequest,
    db: Session = Depends(get_db),
) -> BulkMatchSyncResponse | BulkMatchAsyncResponse:
    n = len(body.records)

    if n > _MAX_RECORDS:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {_MAX_RECORDS} records per request; received {n}.",
        )

    records_as_dicts = [r.model_dump() for r in body.records]
    options_dict = body.options.model_dump()

    # ---- synchronous path ------------------------------------------------
    if n <= _SYNC_LIMIT:
        try:
            from services.matcher import match_batch
            output = match_batch(
                records_as_dicts,
                min_score=body.options.min_score,
                max_candidates=body.options.max_candidates,
            )
            return BulkMatchSyncResponse(
                results=[MatchResult(**r) for r in output["results"]],
                stats=MatchStats(**output["stats"]),
            )
        except HTTPException:
            raise
        except Exception:
            log.error("bulk_match sync error\n%s", traceback.format_exc())
            raise HTTPException(status_code=500, detail="Internal server error")

    # ---- async path -------------------------------------------------------
    try:
        job = SyncJob(
            job_type="bulk_match",
            status="pending",
            firms_processed=0,
            firms_updated=0,
            changes_detected=0,
        )
        db.add(job)
        db.commit()
        db.refresh(job)

        from celery_tasks.match_tasks import run_bulk_match
        run_bulk_match.delay(job.id, records_as_dicts, options_dict)

        return BulkMatchAsyncResponse(
            job_id=job.id,
            status="pending",
            message=f"Job queued. Poll GET /api/match/jobs/{job.id} for status.",
        )
    except HTTPException:
        raise
    except Exception:
        log.error("bulk_match async error\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# GET /api/match/jobs/{job_id}
# ---------------------------------------------------------------------------

@router.get(
    "/jobs/{job_id}",
    response_model=MatchJobStatus,
    summary="Get match job status",
    description="Poll this endpoint after POSTing >100 records. Returns status and results once complete.",
)
def get_match_job(job_id: int, db: Session = Depends(get_db)) -> MatchJobStatus:
    job: SyncJob | None = db.get(SyncJob, job_id)
    if job is None or job.job_type != "bulk_match":
        raise HTTPException(status_code=404, detail=f"Match job {job_id} not found")

    results = None
    if job.status == "complete" and job.results:
        raw = job.results  # already a dict from JSONB
        results = BulkMatchSyncResponse(
            results=[MatchResult(**r) for r in raw.get("results", [])],
            stats=MatchStats(**raw.get("stats", {})),
        )

    return MatchJobStatus(
        job_id=job.id,
        status=job.status,
        created_at=job.created_at.isoformat() if job.created_at else None,
        started_at=job.started_at.isoformat() if job.started_at else None,
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
        error_message=job.error_message,
        results=results,
    )
