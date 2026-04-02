"""
Celery tasks for per-firm and per-platform ADV Part 2 brochure fetching.

Tasks:
    fetch_firm_brochures(crd)              — download Part 2 PDFs for one firm
    sync_platform_brochures(platform_id)  — process all firms on a platform, with SyncJob tracking
    sync_all_platforms_brochures()        — run sync for all save_brochures platforms sequentially
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from celery_tasks.app import app

log = logging.getLogger(__name__)


@app.task(bind=True, name="brochure_tasks.fetch_firm_brochures", max_retries=2)
def fetch_firm_brochures(self, crd: int) -> int:
    """Download and store Part 2 brochures for a single firm. Returns count stored."""
    from db import SessionLocal
    from services.firm_brochure_service import fetch_and_store_firm_brochures

    try:
        with SessionLocal() as db:
            count = fetch_and_store_firm_brochures(crd, db)
        log.info("fetch_firm_brochures: crd=%d stored=%d", crd, count)
        return count
    except Exception as exc:
        log.warning("fetch_firm_brochures: crd=%d failed: %s", crd, exc)
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


@app.task(bind=True, name="brochure_tasks.sync_platform_brochures", max_retries=1)
def sync_platform_brochures(self, platform_id: int, job_id: int | None = None) -> dict:
    """
    Fetch brochures for every firm tagged to this platform, writing progress
    to a SyncJob so the UI can track it.
    """
    from db import SessionLocal
    from models.platform import FirmPlatform, PlatformDefinition
    from models.sync_job import SyncJob
    from services.firm_brochure_service import fetch_and_store_firm_brochures
    from sqlalchemy import select
    from sqlalchemy.orm.attributes import flag_modified

    with SessionLocal() as db:
        platform = db.get(PlatformDefinition, platform_id)
        if platform is None:
            log.warning("sync_platform_brochures: platform %d not found", platform_id)
            return {"skipped": True, "reason": "platform not found"}
        if not platform.save_brochures:
            return {"skipped": True, "reason": "save_brochures=False"}

        crds = list(
            db.scalars(
                select(FirmPlatform.crd_number).where(FirmPlatform.platform_id == platform_id)
            ).all()
        )

        # Attach to existing SyncJob or create one for this platform sync
        if job_id:
            job = db.get(SyncJob, job_id)
        else:
            job = None

        def _log(msg: str) -> None:
            if job is None:
                return
            entry = {"ts": datetime.now(timezone.utc).isoformat(), "msg": msg}
            current = dict(job.results) if job.results else {}
            current.setdefault("log", []).append(entry)
            job.results = current
            flag_modified(job, "results")
            db.commit()

        _log(f"Platform '{platform.name}': fetching brochures for {len(crds)} firm(s)…")

        total_stored = 0
        for i, crd in enumerate(crds, 1):
            try:
                stored = fetch_and_store_firm_brochures(crd, db)
                total_stored += stored
                if stored:
                    _log(f"  CRD {crd}: {stored} new PDF(s) stored")
            except Exception as exc:
                log.warning("sync_platform_brochures: crd=%d failed: %s", crd, exc)
                _log(f"  CRD {crd}: failed — {exc}")

            if job and i % 10 == 0:
                job.firms_processed = i
                job.firms_updated = total_stored
                db.commit()

        if job:
            job.firms_processed = (job.firms_processed or 0) + len(crds)
            job.firms_updated = (job.firms_updated or 0) + total_stored
            db.commit()

        _log(f"Platform '{platform.name}' complete: {total_stored} new PDF(s) across {len(crds)} firm(s)")

    log.info(
        "sync_platform_brochures: platform %d processed %d firm(s), stored %d PDF(s)",
        platform_id, len(crds), total_stored,
    )
    return {"platform_id": platform_id, "firms": len(crds), "stored": total_stored}


@app.task(name="brochure_tasks.sync_all_platforms_brochures")
def sync_all_platforms_brochures(job_id: int | None = None) -> dict:
    """
    Run brochure sync for every platform with save_brochures=True.
    Creates (or updates) a SyncJob of type 'brochure_sync' so the UI
    can show progress.
    """
    from db import SessionLocal
    from models.platform import PlatformDefinition
    from models.sync_job import SyncJob
    from sqlalchemy import select
    from sqlalchemy.orm.attributes import flag_modified

    with SessionLocal() as db:
        platform_ids = list(
            db.scalars(
                select(PlatformDefinition.id).where(PlatformDefinition.save_brochures == True)  # noqa: E712
            ).all()
        )

        # Create a SyncJob to track this brochure sync run
        job = SyncJob(
            job_type="brochure_sync",
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        sync_job_id = job.id

        def _log(msg: str) -> None:
            j = db.get(SyncJob, sync_job_id)
            if j:
                entry = {"ts": datetime.now(timezone.utc).isoformat(), "msg": msg}
                current = dict(j.results) if j.results else {}
                current.setdefault("log", []).append(entry)
                j.results = current
                flag_modified(j, "results")
                db.commit()

        _log(f"Starting brochure sync for {len(platform_ids)} platform(s)…")

    # Process each platform sequentially (reuse connection per platform task)
    total_stored = 0
    try:
        for pid in platform_ids:
            result = sync_platform_brochures(pid, job_id=sync_job_id)
            if isinstance(result, dict) and not result.get("skipped"):
                total_stored += result.get("stored", 0)
    except Exception as exc:
        with SessionLocal() as db:
            job = db.get(SyncJob, sync_job_id)
            if job:
                job.status = "failed"
                job.error_message = str(exc)
                job.completed_at = datetime.now(timezone.utc)
                db.commit()
        raise

    with SessionLocal() as db:
        job = db.get(SyncJob, sync_job_id)
        if job:
            job.status = "complete"
            job.completed_at = datetime.now(timezone.utc)
            entry = {"ts": datetime.now(timezone.utc).isoformat(), "msg": f"Brochure sync complete: {total_stored} new PDF(s) stored"}
            current = dict(job.results) if job.results else {}
            current.setdefault("log", []).append(entry)
            job.results = current
            flag_modified(job, "results")
            db.commit()

    log.info("sync_all_platforms_brochures: complete, %d PDF(s) stored across %d platform(s)", total_stored, len(platform_ids))
    return {"platforms": len(platform_ids), "total_stored": total_stored}
