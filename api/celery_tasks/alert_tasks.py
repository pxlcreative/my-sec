"""
Celery tasks for batch alert evaluation.

Triggered at end of monthly sync and on-demand via the API.
"""
from __future__ import annotations

import logging

from celery_tasks.app import app

log = logging.getLogger(__name__)


@app.task(bind=True, name="alert_tasks.batch_evaluate_alerts", max_retries=1)
def batch_evaluate_alerts(self, rule_ids: list[int] | None = None) -> dict:
    """
    Evaluate all active rules (or a specific subset) in batch.

    Args:
        rule_ids: If provided, only evaluate these rules. If None, evaluate
                  all active rules.

    Returns:
        {"total_fired": int, "by_rule": {rule_id: fired_count}, "errors": [...]}
    """
    from db import SessionLocal
    from models.alert import AlertRule
    from services.alert_service import evaluate_rule_batch
    from sqlalchemy import select

    total_fired = 0
    by_rule: dict[int, int] = {}
    errors: list[str] = []

    with SessionLocal() as db:
        stmt = select(AlertRule).where(AlertRule.active.is_(True))
        if rule_ids:
            stmt = stmt.where(AlertRule.id.in_(rule_ids))
        rules = list(db.scalars(stmt).all())

        log.info("batch_evaluate_alerts: evaluating %d rule(s)", len(rules))

        for rule in rules:
            try:
                fired = evaluate_rule_batch(rule, db)
                by_rule[rule.id] = fired
                total_fired += fired
                log.info(
                    "batch_evaluate_alerts: rule_id=%d type=%s fired=%d",
                    rule.id, rule.rule_type, fired,
                )
            except Exception as exc:
                log.exception(
                    "batch_evaluate_alerts: rule_id=%d failed: %s", rule.id, exc
                )
                errors.append(f"rule {rule.id}: {exc}")

    result = {"total_fired": total_fired, "by_rule": by_rule, "errors": errors}
    log.info("batch_evaluate_alerts complete: %s", result)
    return result


@app.task(
    bind=True,
    name="alert_tasks.retry_alert_delivery",
    max_retries=3,
    default_retry_delay=300,
)
def retry_alert_delivery(self, event_id: int) -> dict:
    """
    Re-attempt delivery of a previously-failed AlertEvent.

    Enqueued by services.alert_service send_alert_email / send_alert_webhook
    when initial delivery raises. Retries 3 times with 5min, 10min, 15min
    backoff. After final failure the task raises so it lands in dead_letter.
    """
    from db import SessionLocal
    from models.alert import AlertEvent, AlertRule
    from models.firm import Firm
    from services.alert_service import send_alert_email, send_alert_webhook

    with SessionLocal() as db:
        event = db.get(AlertEvent, event_id)
        if event is None:
            log.warning("retry_alert_delivery: event %d not found", event_id)
            return {"event_id": event_id, "status": "not_found"}

        if event.delivery_status == "sent":
            return {"event_id": event_id, "status": "already_sent"}

        rule = db.get(AlertRule, event.rule_id)
        firm = db.get(Firm, event.crd_number)
        if rule is None or firm is None:
            log.warning("retry_alert_delivery(%d): rule or firm missing", event_id)
            return {"event_id": event_id, "status": "orphaned"}

        extra: dict = {}
        if event.old_value is not None:
            extra["old_value"] = event.old_value
        if event.new_value is not None:
            extra["new_value"] = event.new_value
        if rule.rule_type == "aum_decline_pct":
            extra.update({
                "prior_aum": int(event.old_value) if event.old_value else None,
                "current_aum": int(event.new_value) if event.new_value else None,
            })

        try:
            if rule.delivery == "email":
                send_alert_email(rule, firm, extra, event, db, retry_on_failure=False)
            elif rule.delivery == "webhook":
                send_alert_webhook(rule, firm, extra, event, db, retry_on_failure=False)
            else:
                event.delivery_status = "sent"
            db.commit()
        except Exception as exc:
            db.rollback()
            log.warning(
                "retry_alert_delivery(%d) failed (attempt %d): %s",
                event_id, self.request.retries + 1, exc,
            )
            raise self.retry(exc=exc)

        return {"event_id": event_id, "status": event.delivery_status}
