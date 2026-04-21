"""
Module G – Alert rules engine and multi-channel delivery.

Public entry points:
  evaluate_alerts_for_firm(crd, changes, db)  — per-firm streaming path
  evaluate_rule_batch(rule, db) -> int         — batch evaluation for one rule
"""
from __future__ import annotations

import logging
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

import requests
from sqlalchemy import and_, desc, exists, extract, or_, select
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
# b. Scope helper
# ---------------------------------------------------------------------------

def get_inscope_crds(rule, db: Session) -> list[int] | None:
    """
    Return CRD numbers in scope for this rule, or None for global (all firms).

    Logic:
      - If rule.crd_numbers is set, use those directly; if rule.platform_ids is
        also set, intersect with platform CRDs.
      - Elif rule.platform_ids is set, return all CRDs on those platforms.
      - Else (both null): return None — global, caller handles all firms.
    """
    from models.platform import FirmPlatform

    if rule.crd_numbers:
        if rule.platform_ids:
            platform_crds = set(
                db.scalars(
                    select(FirmPlatform.crd_number).where(
                        FirmPlatform.platform_id.in_(rule.platform_ids)
                    )
                ).all()
            )
            return [c for c in rule.crd_numbers if c in platform_crds]
        return list(rule.crd_numbers)

    if rule.platform_ids:
        return list(
            db.scalars(
                select(FirmPlatform.crd_number)
                .where(FirmPlatform.platform_id.in_(rule.platform_ids))
                .distinct()
            ).all()
        )

    return None  # global


# ---------------------------------------------------------------------------
# c. Deregistration evaluator (streaming)
# ---------------------------------------------------------------------------

def evaluate_deregistration(rule, firm, changes: list[dict]) -> bool:
    return any(
        c.get("field_path") == "registration_status"
        and c.get("new_value") == "Withdrawn"
        for c in changes
    )


# ---------------------------------------------------------------------------
# d. AUM decline evaluator (streaming + batch)
# ---------------------------------------------------------------------------

_OPS = {
    "lt":  lambda a, b: a < b,
    "lte": lambda a, b: a <= b,
    "gt":  lambda a, b: a > b,
    "gte": lambda a, b: a >= b,
}


def evaluate_aum_decline(rule, firm, db: Session) -> tuple[bool, dict]:
    """
    Compare firm.aum_total against the most recent filing in the prior calendar year.
    Returns (triggered, {prior_aum, current_aum, pct_change}).
    threshold_pct is signed: negative means decline, positive means increase.
    operator controls the comparison: lt/lte/gt/gte.
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

    op = rule.operator or "lte"
    compare = _OPS.get(op, _OPS["lte"])
    triggered = compare(pct_change, float(rule.threshold_pct))
    return triggered, {
        "prior_aum":    prior_aum,
        "current_aum":  current_aum,
        "pct_change":   round(pct_change, 2),
    }


# ---------------------------------------------------------------------------
# e. Alert firing
# ---------------------------------------------------------------------------

def fire_alert(
    rule,
    firm,
    extra_data: dict,
    db: Session,
    firm_change_id: int | None = None,
):
    """
    Insert AlertEvent and dispatch via configured delivery channel.

    Returns the AlertEvent on success, or None if a duplicate was detected
    (integrity error on the partial unique index).
    """
    from models.alert import AlertEvent
    from models.platform import FirmPlatform, PlatformDefinition
    from sqlalchemy.exc import IntegrityError

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

    if rule.rule_type == "aum_decline_pct":
        ev_field = "aum_total"
        ev_old   = str(extra_data["prior_aum"]) if "prior_aum" in extra_data else None
        ev_new   = str(extra_data["current_aum"]) if "current_aum" in extra_data else None
    elif rule.rule_type == "deregistration":
        ev_field = "registration_status"
        ev_old   = "Registered"
        ev_new   = "Withdrawn"
    else:
        # field_change: caller passes {"old_value": ..., "new_value": ...}
        ev_field = rule.field_path
        ev_old   = extra_data.get("old_value")
        ev_new   = extra_data.get("new_value")

    event = AlertEvent(
        rule_id=rule.id,
        crd_number=firm.crd_number,
        firm_name=firm.legal_name,
        rule_type=rule.rule_type,
        field_path=ev_field,
        old_value=ev_old,
        new_value=ev_new,
        platform_name=platform_name,
        fired_at=now,
        delivery_status="pending",
        firm_change_id=firm_change_id,
    )
    db.add(event)

    try:
        db.flush()  # get event.id; triggers unique index check
    except IntegrityError:
        db.rollback()
        log.debug(
            "fire_alert: duplicate suppressed for rule_id=%d firm_change_id=%s",
            rule.id, firm_change_id,
        )
        return None

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
    return event


# ---------------------------------------------------------------------------
# f. Email delivery
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


def send_alert_email(
    rule, firm, extra_data: dict, event, db: Session,
    *, retry_on_failure: bool = True,
) -> None:
    """
    Send an alert email via SMTP. On failure mark the event status and, when
    *retry_on_failure* is True, enqueue a bounded Celery retry so transient
    SMTP blips don't drop alerts. The retry task calls this function with
    retry_on_failure=False to avoid recursion.
    """
    target = rule.delivery_target
    if not target:
        log.warning("send_alert_email: rule %d has no delivery_target", rule.id)
        event.delivery_status = "failed:no_target"
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
        event.delivery_status = f"failed:{str(exc)[:80]}"
        if retry_on_failure and event.id is not None:
            _enqueue_delivery_retry(event.id)


# ---------------------------------------------------------------------------
# g. Webhook delivery
# ---------------------------------------------------------------------------

def send_alert_webhook(
    rule, firm, extra_data: dict, event, db: Session,
    *, retry_on_failure: bool = True,
) -> None:
    target = rule.delivery_target
    if not target:
        log.warning("send_alert_webhook: rule %d has no delivery_target", rule.id)
        event.delivery_status = "failed:no_target"
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
        event.delivery_status = f"failed:{str(exc)[:80]}"
        if retry_on_failure and event.id is not None:
            _enqueue_delivery_retry(event.id)


def _enqueue_delivery_retry(event_id: int) -> None:
    """Enqueue a bounded retry of alert delivery. Broker failures are logged, not raised."""
    try:
        from celery_tasks.alert_tasks import retry_alert_delivery
        retry_alert_delivery.apply_async(args=[event_id], countdown=300)
    except Exception as exc:
        log.error("_enqueue_delivery_retry: could not enqueue retry for event %d: %s",
                  event_id, exc)


# ---------------------------------------------------------------------------
# h. Per-firm streaming evaluator (called from firm_refresh_service)
# ---------------------------------------------------------------------------

def evaluate_alerts_for_firm(crd: int, changes: list[dict], db: Session) -> None:
    """
    Evaluate all active alert rules against a just-refreshed firm.
    Called after change detection when diffs is non-empty.
    """
    from models.alert import AlertEvent
    from models.firm import Firm, FirmChange
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

    # Build field_path → FirmChange.id map from the just-flushed change rows.
    # save_snapshot_and_changes() calls db.flush() so IDs are available without commit.
    changed_paths = {c["field_path"] for c in changes}
    recent_changes: dict[str, int] = {}
    if changed_paths:
        rows = db.execute(
            select(FirmChange.field_path, FirmChange.id)
            .where(
                FirmChange.crd_number == crd,
                FirmChange.field_path.in_(changed_paths),
            )
            .order_by(desc(FirmChange.detected_at))
        ).all()
        for field_path, fc_id in rows:
            if field_path not in recent_changes:
                recent_changes[field_path] = fc_id

    log.debug("evaluate_alerts_for_firm: CRD %d — %d rule(s) to check", crd, len(rules))

    for rule in rules:
        try:
            if rule.rule_type == "deregistration":
                if evaluate_deregistration(rule, firm, changes):
                    fc_id = recent_changes.get("registration_status")
                    fire_alert(rule, firm, {}, db, firm_change_id=fc_id)

            elif rule.rule_type == "aum_decline_pct":
                triggered, extra = evaluate_aum_decline(rule, firm, db)
                if triggered:
                    fire_alert(rule, firm, extra, db, firm_change_id=None)

            elif rule.rule_type == "field_change" and rule.field_path:
                matching = [
                    c for c in changes
                    if c["field_path"] == rule.field_path
                    and (rule.match_old_value is None or c.get("old_value") == rule.match_old_value)
                    and (rule.match_new_value is None or c.get("new_value") == rule.match_new_value)
                ]
                if matching:
                    fc_id = recent_changes.get(rule.field_path)
                    fire_alert(rule, firm, {}, db, firm_change_id=fc_id)

        except Exception as exc:
            log.exception(
                "evaluate_alerts_for_firm: error evaluating rule %d for CRD %d: %s",
                rule.id, crd, exc,
            )


# ---------------------------------------------------------------------------
# i. Batch evaluators
# ---------------------------------------------------------------------------

def evaluate_deregistration_batch(rule, db: Session) -> int:
    """
    Batch deregistration evaluation against current firm states.
    Returns count of new AlertEvents fired.

    Part A: Firms with a FirmChange record setting status to Withdrawn that
            is not yet linked to an AlertEvent for this rule.
    Part B: Firms currently Withdrawn with no FirmChange record at all
            (pre-change-tracking era) that have never triggered this rule.
    """
    from models.alert import AlertEvent
    from models.firm import Firm, FirmChange

    fired = 0
    inscope = get_inscope_crds(rule, db)

    # --- Part A: change-anchored ---
    stmt = (
        select(FirmChange)
        .join(Firm, Firm.crd_number == FirmChange.crd_number)
        .where(
            FirmChange.field_path == "registration_status",
            FirmChange.new_value == "Withdrawn",
            Firm.registration_status == "Withdrawn",
            ~exists().where(
                and_(
                    AlertEvent.rule_id == rule.id,
                    AlertEvent.firm_change_id == FirmChange.id,
                )
            ),
        )
        .order_by(desc(FirmChange.detected_at))
    )
    if inscope is not None:
        stmt = stmt.where(FirmChange.crd_number.in_(inscope))

    changes = list(db.scalars(stmt).all())

    # Keep only the most recent change per CRD
    seen_crds: set[int] = set()
    for fc in changes:
        if fc.crd_number in seen_crds:
            continue
        seen_crds.add(fc.crd_number)
        firm = db.get(Firm, fc.crd_number)
        if firm is None:
            continue
        result = fire_alert(rule, firm, {}, db, firm_change_id=fc.id)
        if result is not None:
            fired += 1

    # --- Part B: legacy fallback (no FirmChange record) ---
    stmt_fallback = (
        select(Firm)
        .where(
            Firm.registration_status == "Withdrawn",
            ~exists().where(
                and_(
                    FirmChange.crd_number == Firm.crd_number,
                    FirmChange.field_path == "registration_status",
                )
            ),
            ~exists().where(
                and_(
                    AlertEvent.rule_id == rule.id,
                    AlertEvent.crd_number == Firm.crd_number,
                )
            ),
        )
    )
    if inscope is not None:
        stmt_fallback = stmt_fallback.where(Firm.crd_number.in_(inscope))

    for firm in db.scalars(stmt_fallback).all():
        result = fire_alert(rule, firm, {}, db, firm_change_id=None)
        if result is not None:
            fired += 1

    return fired


def evaluate_aum_decline_batch(rule, db: Session) -> int:
    """
    Batch AUM decline evaluation. Returns count of new AlertEvents fired.
    Deduplication: skip firms already alerted for this rule within the past year.
    Processes in batches of 500 to avoid loading all Firm objects at once.
    """
    from models.alert import AlertEvent
    from models.firm import Firm

    fired = 0
    inscope = get_inscope_crds(rule, db)
    one_year_ago = datetime.now(timezone.utc) - timedelta(days=365)

    base_stmt = select(Firm.crd_number).where(Firm.aum_total.is_not(None))
    if inscope is not None:
        base_stmt = base_stmt.where(Firm.crd_number.in_(inscope))

    all_crds = list(db.scalars(base_stmt).all())

    _BATCH = 500
    for i in range(0, len(all_crds), _BATCH):
        batch_crds = all_crds[i : i + _BATCH]

        already_alerted = set(
            db.scalars(
                select(AlertEvent.crd_number).where(
                    AlertEvent.rule_id == rule.id,
                    AlertEvent.crd_number.in_(batch_crds),
                    AlertEvent.fired_at >= one_year_ago,
                )
            ).all()
        )

        for crd in batch_crds:
            if crd in already_alerted:
                continue
            firm = db.get(Firm, crd)
            if firm is None:
                continue
            try:
                triggered, extra = evaluate_aum_decline(rule, firm, db)
                if triggered:
                    result = fire_alert(rule, firm, extra, db, firm_change_id=None)
                    if result is not None:
                        fired += 1
            except Exception as exc:
                log.exception(
                    "evaluate_aum_decline_batch: error for CRD %d rule %d: %s",
                    crd, rule.id, exc,
                )

    return fired


def _evaluate_field_change_batch(rule, db: Session) -> int:
    """
    Batch field_change evaluation. Fires for the most recent FirmChange per CRD
    for rule.field_path, deduplicating via firm_change_id.
    """
    from models.alert import AlertEvent
    from models.firm import Firm, FirmChange

    if not rule.field_path:
        log.warning("_evaluate_field_change_batch: rule %d has no field_path", rule.id)
        return 0

    fired = 0
    inscope = get_inscope_crds(rule, db)

    stmt = (
        select(FirmChange)
        .where(
            FirmChange.field_path == rule.field_path,
            ~exists().where(
                and_(
                    AlertEvent.rule_id == rule.id,
                    AlertEvent.firm_change_id == FirmChange.id,
                )
            ),
        )
        .order_by(desc(FirmChange.detected_at))
    )
    if inscope is not None:
        stmt = stmt.where(FirmChange.crd_number.in_(inscope))
    if rule.match_old_value is not None:
        stmt = stmt.where(FirmChange.old_value == rule.match_old_value)
    if rule.match_new_value is not None:
        stmt = stmt.where(FirmChange.new_value == rule.match_new_value)

    changes = list(db.scalars(stmt).all())

    seen_crds: set[int] = set()
    for fc in changes:
        if fc.crd_number in seen_crds:
            continue
        seen_crds.add(fc.crd_number)
        firm = db.get(Firm, fc.crd_number)
        if firm is None:
            continue
        extra = {"old_value": fc.old_value, "new_value": fc.new_value}
        result = fire_alert(rule, firm, extra, db, firm_change_id=fc.id)
        if result is not None:
            fired += 1

    return fired


def evaluate_rule_batch(rule, db: Session) -> int:
    """
    Dispatch to the appropriate batch evaluator for a single rule.
    Returns count of AlertEvents fired.
    """
    if not rule.active:
        return 0

    if rule.rule_type == "deregistration":
        return evaluate_deregistration_batch(rule, db)
    elif rule.rule_type == "aum_decline_pct":
        return evaluate_aum_decline_batch(rule, db)
    elif rule.rule_type == "field_change":
        return _evaluate_field_change_batch(rule, db)
    else:
        log.warning(
            "evaluate_rule_batch: unknown rule_type=%s for rule %d",
            rule.rule_type, rule.id,
        )
        return 0
