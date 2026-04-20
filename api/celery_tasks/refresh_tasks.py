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

    for crd in crds:
        refresh_firm_task.delay(crd)

    log.info("batch_verify_registration_status: enqueued %d firms", len(crds))
    return {"enqueued": len(crds)}


