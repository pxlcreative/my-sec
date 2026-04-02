"""
Celery tasks for live firm data refresh via IAPD.
"""
import logging

from celery_tasks.app import app

log = logging.getLogger(__name__)


@app.task(bind=True, name="refresh_tasks.refresh_firm_task", max_retries=2)
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
            return {"crd_number": crd_number, "error": str(exc), "changed": False}
        except Exception as exc:
            log.exception("refresh_firm_task(%d) failed (attempt %d)", crd_number, self.request.retries + 1)
            raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))


