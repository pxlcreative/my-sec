"""
Alert rules and events routes.

GET    /api/alerts/rules              → list all rules
POST   /api/alerts/rules              → create rule
PUT    /api/alerts/rules/{id}         → update rule
DELETE /api/alerts/rules/{id}         → delete rule (sets active=False)
GET    /api/alerts/events             → recent events with optional filters
POST   /api/alerts/rules/{id}/test    → fire a test delivery (dry run)
"""
import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, desc, select
from sqlalchemy.orm import Session

from db import get_db
from models.alert import AlertEvent, AlertRule
from schemas.alert import (
    AlertEventOut,
    AlertRuleCreate,
    AlertRuleOut,
    AlertRuleUpdate,
    AlertTestResponse,
)

log = logging.getLogger(__name__)
router = APIRouter(prefix="/alerts", tags=["alerts"])

DbDep = Annotated[Session, Depends(get_db)]


def _get_rule_or_404(rule_id: int, db: Session) -> AlertRule:
    rule = db.get(AlertRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail=f"Alert rule {rule_id} not found")
    return rule


# ---------------------------------------------------------------------------
# GET /api/alerts/rules
# ---------------------------------------------------------------------------

@router.get("/rules", response_model=list[AlertRuleOut])
def list_rules(active_only: bool = Query(True), db: DbDep = None):
    stmt = select(AlertRule)
    if active_only:
        stmt = stmt.where(AlertRule.active.is_(True))
    return list(db.scalars(stmt.order_by(AlertRule.id)).all())


# ---------------------------------------------------------------------------
# POST /api/alerts/rules
# ---------------------------------------------------------------------------

@router.post("/rules", response_model=AlertRuleOut, status_code=201)
def create_rule(body: AlertRuleCreate, db: DbDep = None):
    rule = AlertRule(**body.model_dump())
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


# ---------------------------------------------------------------------------
# PUT /api/alerts/rules/{id}
# ---------------------------------------------------------------------------

@router.put("/rules/{rule_id}", response_model=AlertRuleOut)
def update_rule(rule_id: int, body: AlertRuleUpdate, db: DbDep = None):
    rule = _get_rule_or_404(rule_id, db)
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(rule, field, value)
    db.commit()
    db.refresh(rule)
    return rule


# ---------------------------------------------------------------------------
# DELETE /api/alerts/rules/{id}
# ---------------------------------------------------------------------------

@router.delete("/rules/{rule_id}", status_code=204)
def delete_rule(rule_id: int, db: DbDep = None):
    rule = _get_rule_or_404(rule_id, db)
    rule.active = False
    db.commit()


# ---------------------------------------------------------------------------
# GET /api/alerts/events
# ---------------------------------------------------------------------------

@router.get("/events", response_model=list[AlertEventOut])
def list_events(
    rule_id:         int | None  = Query(None),
    crd_number:      int | None  = Query(None),
    platform:        str | None  = Query(None, description="Filter by platform_name"),
    delivery_status: str | None  = Query(None, description="Filter by delivery_status"),
    since:           datetime | None = Query(None, description="ISO8601 datetime lower bound"),
    until:           datetime | None = Query(None, description="ISO8601 datetime upper bound"),
    limit:           int         = Query(500, ge=1, le=1000),
    offset:          int         = Query(0, ge=0),
    db: DbDep = None,
):
    stmt = select(AlertEvent).order_by(desc(AlertEvent.fired_at))

    if rule_id is not None:
        stmt = stmt.where(AlertEvent.rule_id == rule_id)
    if crd_number is not None:
        stmt = stmt.where(AlertEvent.crd_number == crd_number)
    if platform is not None:
        stmt = stmt.where(AlertEvent.platform_name == platform)
    if delivery_status is not None:
        stmt = stmt.where(AlertEvent.delivery_status == delivery_status)
    if since is not None:
        stmt = stmt.where(AlertEvent.fired_at >= since)
    if until is not None:
        stmt = stmt.where(AlertEvent.fired_at <= until)

    return list(db.scalars(stmt.offset(offset).limit(limit)).all())


# ---------------------------------------------------------------------------
# POST /api/alerts/rules/{id}/test
# ---------------------------------------------------------------------------

@router.post("/rules/{rule_id}/test", response_model=AlertTestResponse)
def test_rule_delivery(rule_id: int, db: DbDep = None):
    """
    Fire a synthetic test delivery to the rule's configured target.
    Inserts a test AlertEvent but clearly marks it delivery_status='test'.
    Does not evaluate real firm data.
    """
    from models.firm import Firm

    rule = _get_rule_or_404(rule_id, db)

    # Build a fake firm-like object for template rendering
    class _FakeFirm:
        crd_number = 0
        legal_name = "Test Firm LLC"
        aum_total = 500_000_000
        last_filing_date = None
        registration_status = "Registered"

    fake_firm = _FakeFirm()
    extra_data = {
        "platform_name": "Test Platform",
        "prior_aum":    500_000_000,
        "current_aum":  375_000_000,
        "pct_change":   -25.0,
    }

    # Insert a test event record
    event = AlertEvent(
        rule_id=rule.id,
        crd_number=0,
        firm_name="[TEST] Test Firm LLC",
        rule_type=rule.rule_type,
        field_path=rule.field_path,
        old_value=None,
        new_value=None,
        platform_name="Test Platform",
        fired_at=datetime.now(timezone.utc),
        delivery_status="test",
    )
    db.add(event)
    db.flush()

    success = True
    message = "in_app delivery — event recorded"

    if rule.delivery == "email":
        from services.alert_service import send_alert_email
        send_alert_email(rule, fake_firm, extra_data, event, db)
        success = event.delivery_status == "sent"
        message = f"Email {'sent' if success else 'failed'} to {rule.delivery_target}"

    elif rule.delivery == "webhook":
        from services.alert_service import send_alert_webhook
        send_alert_webhook(rule, fake_firm, extra_data, event, db)
        success = event.delivery_status == "sent"
        message = f"Webhook POST {'succeeded' if success else 'failed'} → {rule.delivery_target}"

    db.commit()

    return AlertTestResponse(
        rule_id=rule_id,
        delivery=rule.delivery,
        delivery_target=rule.delivery_target,
        success=success,
        message=message,
    )
