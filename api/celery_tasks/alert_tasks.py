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
