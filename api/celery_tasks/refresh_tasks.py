"""
Celery tasks for live firm data refresh via IAPD.
"""
from __future__ import annotations

import logging

from celery_tasks.app import app

log = logging.getLogger(__name__)


@app.task(bind=True, name="refresh_tasks.refresh_firm_task", max_retries=2, rate_limit='10/m')
def refresh_firm_task(self, crd_number: int) -> dict:
    """
    Refresh a single firm from the IAPD API.
    Retries up to 2 times on failure (rate-limit errors, transient network issues).
    """
    from db import SessionLocal
    from services.firm_refresh_service import refresh_firm

    with SessionLocal() as session:
        try:
            diffs = refresh_firm(crd_number, session)
            result = {
                "crd_number": crd_number,
                "changed": len(diffs) > 0,
                "num_changes": len(diffs),
                "fields_changed": [d["field_path"] for d in diffs],
            }
            log.info(
                "refresh_firm_task(%d): %d change(s): %s",
                crd_number, len(diffs), result["fields_changed"],
            )
            return result
        except ValueError as exc:
            # CRD not found in IAPD — not retryable
            log.warning("refresh_firm_task(%d): not found in IAPD — %s", crd_number, exc)
            import datetime
            from models.firm import Firm
            stale_cutoff = datetime.date.today() - datetime.timedelta(days=365)
            firm = session.get(Firm, crd_number)
            if (
                firm
                and firm.registration_status == "Registered"
                and firm.last_filing_date
                and firm.last_filing_date < stale_cutoff
            ):
                firm.registration_status = "Inactive"
                firm.last_iapd_refresh_at = datetime.datetime.now(datetime.timezone.utc)
                session.commit()
                log.info("refresh_firm_task(%d): marked Inactive (not in IAPD, last filed %s)", crd_number, firm.last_filing_date)
            return {"crd_number": crd_number, "error": str(exc), "changed": False}
        except Exception as exc:
            log.exception("refresh_firm_task(%d) failed (attempt %d)", crd_number, self.request.retries + 1)
            raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))


@app.task(
    bind=True,
    name="refresh_tasks.reindex_firm",
    max_retries=3,
    default_retry_delay=60,
)
def reindex_firm(self, crd_number: int) -> dict:
    """
    Re-index a single firm to Elasticsearch.

    Enqueued by firm_refresh_service._reindex_firm when a direct re-index
    fails (e.g. ES temporarily unavailable). Retries 3 times with 60s,
    120s, 180s backoff. On final failure the task raises so it lands in
    the dead_letter queue rather than being silently dropped.
    """
    from db import SessionLocal
    from models.firm import Firm
    from models.platform import FirmPlatform, PlatformDefinition
    from services.es_client import bulk_index_firms
    from sqlalchemy import select

    with SessionLocal() as session:
        firm: Firm | None = session.get(Firm, crd_number)
        if firm is None:
            log.warning("reindex_firm(%d): firm not found, skipping", crd_number)
            return {"crd_number": crd_number, "indexed": False, "reason": "not_found"}

        plat_names = [
            row[0] for row in session.execute(
                select(PlatformDefinition.name)
                .join(FirmPlatform, FirmPlatform.platform_id == PlatformDefinition.id)
                .where(FirmPlatform.crd_number == crd_number)
            ).all()
        ]
        doc = {
            "crd_number":          firm.crd_number,
            "legal_name":          firm.legal_name or "",
            "business_name":       firm.business_name,
            "main_street1":        firm.main_street1,
            "main_city":           firm.main_city,
            "main_state":          firm.main_state,
            "main_zip":            firm.main_zip,
            "registration_status": firm.registration_status,
            "platforms":           plat_names,
        }

    try:
        indexed = bulk_index_firms([doc])
        log.info("reindex_firm(%d): indexed=%d", crd_number, indexed)
        return {"crd_number": crd_number, "indexed": bool(indexed)}
    except Exception as exc:
        log.warning(
            "reindex_firm(%d) failed (attempt %d): %s",
            crd_number, self.request.retries + 1, exc,
        )
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


@app.task(name="refresh_tasks.batch_verify_registration_status")
def batch_verify_registration_status(refresh_cooldown_days: int = 30) -> dict:
    """
    Enqueue refresh_firm_task for 'Registered' firms that appear in the 2025+ monthly
    CSV data but haven't yet been verified against the live IAPD API.

    Scope: last_filing_date >= 2025-01-01 (new data format with explicit registration_status)
    and last_iapd_refresh_at is null or older than refresh_cooldown_days.

    Pre-2025 firms are already classified as 'Inactive' — they are excluded here because
    the old bulk CSV data defaulted registration_status to 'Registered' without confirmation,
    and live IAPD returns EDGAR-format documents for those CRDs (no usable status field).
    """
    import datetime
    from sqlalchemy import select, or_

    from db import SessionLocal
    from models.firm import Firm

    monthly_data_start = datetime.date(2025, 1, 1)
    refresh_cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=refresh_cooldown_days)

    with SessionLocal() as session:
        stmt = (
            select(Firm.crd_number)
            .where(Firm.registration_status == "Registered")
            .where(Firm.last_filing_date >= monthly_data_start)
            .where(or_(Firm.last_iapd_refresh_at.is_(None), Firm.last_iapd_refresh_at < refresh_cutoff))
        )
        crds = [row[0] for row in session.execute(stmt)]

    # Stagger dispatch so we don't flood the broker with 50k+ tasks at once.
    # refresh_firm_task is rate-limited to 10/m per worker, so a burst of
    # 50k would sit in the queue for days. Spread dispatch across an hour
    # (BATCH_SIZE per 60 seconds) so earliest batches start running while
    # later ones are still being enqueued.
    BATCH_SIZE = 100
    BATCH_INTERVAL_SECONDS = 60

    for i, crd in enumerate(crds):
        countdown = (i // BATCH_SIZE) * BATCH_INTERVAL_SECONDS
        refresh_firm_task.apply_async(args=[crd], countdown=countdown)

    log.info(
        "batch_verify_registration_status: enqueued %d firms across %d batches of %d",
        len(crds),
        (len(crds) + BATCH_SIZE - 1) // BATCH_SIZE,
        BATCH_SIZE,
    )
    return {"enqueued": len(crds), "batch_size": BATCH_SIZE}


