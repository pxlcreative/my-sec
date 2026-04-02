"""
Celery tasks for per-firm and per-platform ADV Part 2 brochure fetching.

Tasks:
    fetch_firm_brochures(crd)              — download Part 2 PDFs for one firm
    sync_platform_brochures(platform_id)  — enqueue fetch for all firms on a platform
    sync_all_platforms_brochures()        — enqueue sync for all save_brochures platforms
"""
from __future__ import annotations

import logging

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
def sync_platform_brochures(self, platform_id: int) -> dict:
    """
    Enqueue fetch_firm_brochures for every firm tagged to this platform.
    Requires platform.save_brochures == True.
    """
    from db import SessionLocal
    from models.platform import FirmPlatform, PlatformDefinition
    from sqlalchemy import select

    with SessionLocal() as db:
        platform = db.get(PlatformDefinition, platform_id)
        if platform is None:
            log.warning("sync_platform_brochures: platform %d not found", platform_id)
            return {"skipped": True, "reason": "platform not found"}
        if not platform.save_brochures:
            log.info(
                "sync_platform_brochures: platform %d (%s) has save_brochures=False, skipping",
                platform_id, platform.name,
            )
            return {"skipped": True, "reason": "save_brochures=False"}

        crds = list(
            db.scalars(
                select(FirmPlatform.crd_number).where(FirmPlatform.platform_id == platform_id)
            ).all()
        )

    for crd in crds:
        fetch_firm_brochures.delay(crd)

    log.info(
        "sync_platform_brochures: platform %d enqueued %d firm(s)",
        platform_id, len(crds),
    )
    return {"enqueued": len(crds), "platform_id": platform_id}


@app.task(name="brochure_tasks.sync_all_platforms_brochures")
def sync_all_platforms_brochures() -> dict:
    """Enqueue sync_platform_brochures for every platform with save_brochures=True."""
    from db import SessionLocal
    from models.platform import PlatformDefinition
    from sqlalchemy import select

    with SessionLocal() as db:
        platform_ids = list(
            db.scalars(
                select(PlatformDefinition.id).where(PlatformDefinition.save_brochures == True)  # noqa: E712
            ).all()
        )

    for pid in platform_ids:
        sync_platform_brochures.delay(pid)

    log.info("sync_all_platforms_brochures: enqueued %d platform(s)", len(platform_ids))
    return {"platforms_enqueued": len(platform_ids)}
