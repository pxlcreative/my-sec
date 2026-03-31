"""
Module G – Alert rules engine and multi-channel delivery.

Public entry point: evaluate_alerts_for_firm(crd, changes, db)
"""
import logging
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText

import requests
from sqlalchemy import and_, extract, desc, or_, select
from sqlalchemy.orm import Session

from config import settings

log = logging.getLogger(__name__)

_WEBHOOK_TIMEOUT = 10   # seconds


# ---------------------------------------------------------------------------
# a. Rule lookup
# ---------------------------------------------------------------------------

def get_active_rules_for_platforms(platform_ids: list[int], db: Session):
    """
    Return all active AlertRules that apply to this firm:
      - rule.platform_ids overlaps with platform_ids, OR
      - rule.platform_ids IS NULL (global rule)
    """
    from models.alert import AlertRule
    from sqlalchemy.dialects.postgresql import ARRAY
    import sqlalchemy as sa

    stmt = select(AlertRule).where(AlertRule.active.is_(True))

    if platform_ids:
        # platform_ids && rule.platform_ids  (PostgreSQL array overlap operator)
        stmt = stmt.where(
            or_(
                AlertRule.platform_ids.is_(None),
                AlertRule.platform_ids.overlap(platform_ids),
            )
        )
    else:
        # No platforms tagged → only global rules
        stmt = stmt.where(AlertRule.platform_ids.is_(None))

    return list(db.scalars(stmt).all())


# ---------------------------------------------------------------------------
# b. Deregistration evaluator
# ---------------------------------------------------------------------------

def evaluate_deregistration(rule, firm, changes: list[dict]) -> bool:
    return any(
        c.get("field_path") == "registration_status"
        and c.get("new_value") == "Withdrawn"
        for c in changes
    )


# ---------------------------------------------------------------------------
# c. AUM decline evaluator
# ---------------------------------------------------------------------------

def evaluate_aum_decline(rule, firm, db: Session) -> tuple[bool, dict]:
    """
    Compare firm.aum_total against the most recent filing in the prior calendar year.
    Returns (triggered, {prior_aum, current_aum, pct_change}).
    """
    from models.aum import FirmAumHistory

    current_aum = firm.aum_total
    if current_aum is None or rule.threshold_pct is None:
        return False, {}

    current_year = datetime.now(timezone.utc).year
    prior_year = current_year - 1

    prior_row = db.scalars(
        select(FirmAumHistory)
        .where(
            FirmAumHistory.crd_number == firm.crd_number,
            extract("year", FirmAumHistory.filing_date) == prior_year,
            FirmAumHistory.aum_total.is_not(None),
        )
        .order_by(desc(FirmAumHistory.filing_date))
        .limit(1)
    ).first()

    if prior_row is None or prior_row.aum_total is None:
        return False, {}

    prior_aum = prior_row.aum_total
    pct_change = (current_aum - prior_aum) / prior_aum * 100

    triggered = pct_change <= -float(rule.threshold_pct)
    return triggered, {
        "prior_aum":    prior_aum,
        "current_aum":  current_aum,
        "pct_change":   round(pct_change, 2),
    }


# ---------------------------------------------------------------------------
# d. Alert firing
# ---------------------------------------------------------------------------

def fire_alert(rule, firm, extra_data: dict, db: Session) -> None:
    """Insert AlertEvent and dispatch via configured delivery channel."""
    from models.alert import AlertEvent
    from models.platform import FirmPlatform, PlatformDefinition

    # Resolve a representative platform name for the event record
    platform_name: str | None = None
    if rule.platform_ids:
        row = db.scalars(
            select(PlatformDefinition.name)
            .join(FirmPlatform, FirmPlatform.platform_id == PlatformDefinition.id)
            .where(
                FirmPlatform.crd_number == firm.crd_number,
                FirmPlatform.platform_id.in_(rule.platform_ids),
            )
            .limit(1)
        ).first()
        platform_name = row

    now = datetime.now(timezone.utc)
    event = AlertEvent(
        rule_id=rule.id,
        crd_number=firm.crd_number,
        firm_name=firm.legal_name,
        rule_type=rule.rule_type,
        field_path=rule.field_path or (
            "registration_status" if rule.rule_type == "deregistration" else "aum_total"
        ),
        old_value=str(extra_data.get("prior_aum")) if "prior_aum" in extra_data else None,
        new_value=str(extra_data.get("current_aum", getattr(firm, "aum_total", None))),
        platform_name=platform_name,
        fired_at=now,
        delivery_status="pending",
    )
    db.add(event)
    db.flush()  # get event.id before delivery attempt

    if rule.delivery == "in_app":
        event.delivery_status = "sent"
        event.delivered_at = now
    elif rule.delivery == "email":
        send_alert_email(rule, firm, extra_data, event, db)
    elif rule.delivery == "webhook":
        send_alert_webhook(rule, firm, extra_data, event, db)

    db.commit()
    log.info(
        "fire_alert: rule_id=%d crd=%d rule_type=%s delivery=%s status=%s",
        rule.id, firm.crd_number, rule.rule_type, rule.delivery, event.delivery_status,
    )


# ---------------------------------------------------------------------------
# e. Email delivery
# ---------------------------------------------------------------------------

def _build_email_body(rule, firm, extra_data: dict) -> tuple[str, str]:
    """Return (subject, body) for an alert email."""
    rule_label = rule.rule_type.replace("_", " ").title()
    subject = f"[SEC Alert] {firm.legal_name} — {rule_label} detected"

    platform = extra_data.get("platform_name", "N/A")
    lines = [
        f"Firm: {firm.legal_name} (CRD: {firm.crd_number})",
        f"Platform: {platform}",
    ]

    if rule.rule_type == "deregistration":
        lines.append("Change: Registration status changed to Withdrawn")
    elif rule.rule_type == "aum_decline_pct":
        prior  = extra_data.get("prior_aum", 0)
        curr   = extra_data.get("current_aum", 0)
        pct    = extra_data.get("pct_change", 0)
        lines += [
            f"Prior AUM:   ${prior:,.0f}",
            f"Current AUM: ${curr:,.0f}",
            f"Change:      {pct}%",
        ]
    else:
        lines.append(f"Change: {extra_data}")

    if firm.last_filing_date:
        lines.append(f"Filing date: {firm.last_filing_date}")

    return subject, "\n".join(lines)


def send_alert_email(rule, firm, extra_data: dict, event, db: Session) -> None:
    target = rule.delivery_target
    if not target:
        log.warning("send_alert_email: rule %d has no delivery_target", rule.id)
        event.delivery_status = "failed"
        return

    subject, body = _build_email_body(rule, firm, extra_data)

    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"]    = settings.smtp_user or "alerts@sec-adv.local"
        msg["To"]      = target

        smtp_host = settings.smtp_host
        smtp_port = settings.smtp_port

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            if settings.smtp_user and settings.smtp_pass:
                server.starttls()
                server.login(settings.smtp_user, settings.smtp_pass)
            server.send_message(msg)

        event.delivery_status = "sent"
        event.delivered_at    = datetime.now(timezone.utc)
        log.info("send_alert_email: sent to %s for CRD %d", target, firm.crd_number)

    except Exception as exc:
        log.error("send_alert_email: failed for CRD %d — %s", firm.crd_number, exc)
        event.delivery_status = "failed"


# ---------------------------------------------------------------------------
# f. Webhook delivery
# ---------------------------------------------------------------------------

def send_alert_webhook(rule, firm, extra_data: dict, event, db: Session) -> None:
    target = rule.delivery_target
    if not target:
        log.warning("send_alert_webhook: rule %d has no delivery_target", rule.id)
        event.delivery_status = "failed"
        return

    payload = {
        "event_type":   rule.rule_type,
        "crd_number":   firm.crd_number,
        "firm_name":    firm.legal_name,
        "platform":     extra_data.get("platform_name"),
        "fired_at":     datetime.now(timezone.utc).isoformat(),
        "rule_id":      rule.id,
        "rule_label":   rule.label,
    }
    if rule.rule_type == "aum_decline_pct":
        payload.update({
            "prior_aum":    extra_data.get("prior_aum"),
            "current_aum":  extra_data.get("current_aum"),
            "pct_change":   extra_data.get("pct_change"),
            "threshold_pct": float(rule.threshold_pct) if rule.threshold_pct else None,
        })

    try:
        resp = requests.post(
            target, json=payload, timeout=_WEBHOOK_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        event.delivery_status = "sent"
        event.delivered_at    = datetime.now(timezone.utc)
        log.info("send_alert_webhook: POST %s → %d for CRD %d", target, resp.status_code, firm.crd_number)

    except Exception as exc:
        log.error("send_alert_webhook: failed POST %s for CRD %d — %s", target, firm.crd_number, exc)
        event.delivery_status = "failed"


# ---------------------------------------------------------------------------
# g. Top-level evaluator
# ---------------------------------------------------------------------------

def evaluate_alerts_for_firm(crd: int, changes: list[dict], db: Session) -> None:
    """
    Evaluate all active alert rules against a just-refreshed firm.
    Called after change detection when diffs is non-empty.
    """
    from models.firm import Firm
    from models.platform import FirmPlatform

    firm: Firm | None = db.get(Firm, crd)
    if firm is None:
        log.warning("evaluate_alerts_for_firm: CRD %d not found", crd)
        return

    platform_ids: list[int] = list(
        db.scalars(
            select(FirmPlatform.platform_id).where(FirmPlatform.crd_number == crd)
        ).all()
    )

    rules = get_active_rules_for_platforms(platform_ids, db)
    if not rules:
        return

    log.debug("evaluate_alerts_for_firm: CRD %d — %d rule(s) to check", crd, len(rules))

    for rule in rules:
        try:
            if rule.rule_type == "deregistration":
                if evaluate_deregistration(rule, firm, changes):
                    fire_alert(rule, firm, {}, db)

            elif rule.rule_type == "aum_decline_pct":
                triggered, extra = evaluate_aum_decline(rule, firm, db)
                if triggered:
                    fire_alert(rule, firm, extra, db)

        except Exception as exc:
            log.exception(
                "evaluate_alerts_for_firm: error evaluating rule %d for CRD %d: %s",
                rule.id, crd, exc,
            )
