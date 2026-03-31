"""
Export routes:
  POST /api/export/firms          – sync (≤500 rows) or async (>500 rows)
  GET  /api/export/jobs/{id}      – job status
  GET  /api/export/jobs/{id}/download – stream completed file
  POST /api/export/templates      – save named preset
  GET  /api/export/templates      – list saved presets
"""
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from db import get_db
from models.export_job import ExportJob
from models.export_template import ExportTemplate
from schemas.export import (
    AsyncExportResponse,
    ExportJobOut,
    ExportRequest,
    ExportTemplateCreate,
    ExportTemplateOut,
)
from services.export_service import (
    SYNC_ROW_LIMIT,
    _count_query,
    _fetch_rows,
    build_export_query,
    file_extension,
    format_rows,
    mime_type,
)

router = APIRouter(prefix="/export", tags=["export"])

DbDep = Annotated[Session, Depends(get_db)]


# ---------------------------------------------------------------------------
# POST /api/export/firms
# ---------------------------------------------------------------------------

@router.post("/firms", summary="Export firms (sync ≤500 rows, async >500)")
def export_firms(req: ExportRequest, session: DbDep):
    stmt = build_export_query(req.filter.model_dump(exclude_none=True), req.crd_list)
    count = _count_query(stmt, session)

    if count <= SYNC_ROW_LIMIT:
        # Synchronous path — stream the file immediately
        rows = _fetch_rows(stmt, session)
        data = format_rows(rows, req.format, req.field_selection)
        ext = file_extension(req.format)
        filename = f"firms_export.{ext}"
        return Response(
            content=data,
            media_type=mime_type(req.format),
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    # Async path — create job record and enqueue Celery task
    from datetime import datetime, timezone
    from celery_tasks.export_tasks import run_export_job

    field_sel_payload = (
        {"fields": req.field_selection} if req.field_selection else None
    )
    job = ExportJob(
        status="pending",
        format=req.format,
        filter_criteria=req.filter.model_dump(exclude_none=True),
        crd_list=req.crd_list,
        field_selection=field_sel_payload,
        created_at=datetime.now(timezone.utc),
    )
    session.add(job)
    session.commit()
    session.refresh(job)

    run_export_job.delay(str(job.id))

    return AsyncExportResponse(
        job_id=job.id,
        status="pending",
        message=f"Export job queued ({count:,} rows). Poll GET /api/export/jobs/{job.id} for status.",
    )


# ---------------------------------------------------------------------------
# GET /api/export/jobs/{id}
# ---------------------------------------------------------------------------

@router.get("/jobs/{job_id}", response_model=ExportJobOut, summary="Get export job status")
def get_export_job(job_id: uuid.UUID, session: DbDep):
    job = session.get(ExportJob, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Export job not found")
    return job


# ---------------------------------------------------------------------------
# GET /api/export/jobs/{id}/download
# ---------------------------------------------------------------------------

@router.get("/jobs/{job_id}/download", summary="Download completed export file")
def download_export(job_id: uuid.UUID, session: DbDep):
    job = session.get(ExportJob, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Export job not found")
    if job.status != "complete":
        raise HTTPException(
            status_code=409,
            detail=f"Export job is not ready (status={job.status!r})",
        )
    if not job.file_path:
        raise HTTPException(status_code=404, detail="Export file missing")

    from pathlib import Path
    path = Path(job.file_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Export file not found on disk")

    ext = file_extension(job.format)
    filename = f"export_{job_id}.{ext}"

    def _iter():
        with open(path, "rb") as fh:
            while chunk := fh.read(65536):
                yield chunk

    return StreamingResponse(
        _iter(),
        media_type=mime_type(job.format),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# POST /api/export/templates
# ---------------------------------------------------------------------------

@router.post("/templates", response_model=ExportTemplateOut, status_code=201,
             summary="Save a named export preset")
def create_template(body: ExportTemplateCreate, session: DbDep):
    existing = session.scalar(
        select(ExportTemplate).where(ExportTemplate.name == body.name)
    )
    if existing:
        raise HTTPException(status_code=409, detail=f"Template name {body.name!r} already exists")

    tpl = ExportTemplate(
        name=body.name,
        description=body.description,
        format=body.format,
        filter_criteria=body.filter_criteria,
        field_selection={"fields": body.field_selection} if body.field_selection else None,
    )
    session.add(tpl)
    session.commit()
    session.refresh(tpl)
    return tpl


# ---------------------------------------------------------------------------
# GET /api/export/templates
# ---------------------------------------------------------------------------

@router.get("/templates", response_model=list[ExportTemplateOut], summary="List saved export presets")
def list_templates(session: DbDep):
    return list(session.scalars(select(ExportTemplate).order_by(ExportTemplate.name)).all())
