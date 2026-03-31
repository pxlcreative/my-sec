"""
Tests for alert rules and event evaluation.

GET  /api/alerts/rules
POST /api/alerts/rules
DELETE /api/alerts/rules/{id}
GET  /api/alerts/events
POST /api/alerts/rules/{id}/test

Also tests that deregistration alert evaluation fires correctly when
a firm's registration_status changes via firm_refresh_service.
"""
from __future__ import annotations

import datetime

from models.alert import AlertEvent, AlertRule
from models.firm import Firm


# ---------------------------------------------------------------------------
# Rule CRUD
# ---------------------------------------------------------------------------

class TestAlertRulesCrud:
    def test_list_rules_empty(self, client):
        r = client.get("/api/alerts/rules")
        assert r.status_code == 200
        assert r.json() == []

    def test_create_deregistration_rule(self, client):
        payload = {
            "label": "Deregistration Watch",
            "rule_type": "deregistration",
            "delivery": "log",
        }
        r = client.post("/api/alerts/rules", json=payload)
        assert r.status_code == 201
        body = r.json()
        assert body["label"] == "Deregistration Watch"
        assert body["rule_type"] == "deregistration"
        assert body["active"] is True
        assert body["id"] is not None

    def test_create_aum_decline_rule(self, client):
        payload = {
            "label": "AUM Drop 20%",
            "rule_type": "aum_decline_pct",
            "threshold_pct": 20.0,
            "delivery": "log",
        }
        r = client.post("/api/alerts/rules", json=payload)
        assert r.status_code == 201
        body = r.json()
        assert body["threshold_pct"] == 20.0

    def test_create_rule_with_platform_scope(self, client, seeded_platform):
        payload = {
            "label": "Platform-scoped Alert",
            "rule_type": "deregistration",
            "delivery": "log",
            "platform_ids": [seeded_platform.id],
        }
        r = client.post("/api/alerts/rules", json=payload)
        assert r.status_code == 201
        assert r.json()["platform_ids"] == [seeded_platform.id]

    def test_list_rules_after_create(self, client):
        client.post("/api/alerts/rules", json={"label": "R1", "rule_type": "deregistration", "delivery": "log"})
        client.post("/api/alerts/rules", json={"label": "R2", "rule_type": "deregistration", "delivery": "log"})
        r = client.get("/api/alerts/rules")
        assert r.status_code == 200
        assert len(r.json()) == 2

    def test_delete_rule_soft_deletes(self, client):
        r = client.post("/api/alerts/rules", json={"label": "Ephemeral", "rule_type": "deregistration", "delivery": "log"})
        rule_id = r.json()["id"]
        del_r = client.delete(f"/api/alerts/rules/{rule_id}")
        assert del_r.status_code == 204
        # Should no longer appear in active list
        rules = client.get("/api/alerts/rules").json()
        assert all(rule["id"] != rule_id for rule in rules)

    def test_delete_nonexistent_rule_returns_404(self, client):
        r = client.delete("/api/alerts/rules/999999")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Alert events
# ---------------------------------------------------------------------------

class TestAlertEvents:
    def test_list_events_empty(self, client):
        r = client.get("/api/alerts/events")
        assert r.status_code == 200
        assert r.json() == []

    def test_filter_events_by_rule_id(self, client, db):
        rule = AlertRule(label="R", rule_type="deregistration", delivery="log", active=True)
        db.add(rule)
        db.flush()
        event = AlertEvent(
            rule_id=rule.id,
            crd_number=100001,
            firm_name="Test Firm",
            rule_type="deregistration",
            fired_at=datetime.datetime.now(datetime.timezone.utc),
            delivery_status="logged",
        )
        db.add(event)
        db.flush()

        r = client.get("/api/alerts/events", params={"rule_id": rule.id})
        assert r.status_code == 200
        events = r.json()
        assert len(events) == 1
        assert events[0]["rule_id"] == rule.id


# ---------------------------------------------------------------------------
# Test delivery
# ---------------------------------------------------------------------------

class TestAlertTestDelivery:
    def test_test_rule_log_delivery(self, client):
        r = client.post("/api/alerts/rules", json={"label": "Test Rule", "rule_type": "deregistration", "delivery": "log"})
        rule_id = r.json()["id"]
        test_r = client.post(f"/api/alerts/rules/{rule_id}/test")
        assert test_r.status_code == 200
        body = test_r.json()
        assert body["rule_id"] == rule_id
        assert "success" in body
        assert "message" in body

    def test_test_nonexistent_rule_returns_404(self, client):
        r = client.post("/api/alerts/rules/999999/test")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Deregistration alert evaluation (unit-level, calls service directly)
# ---------------------------------------------------------------------------

class TestDeregistrationEvaluation:
    def test_deregistration_fires_alert(self, db):
        """
        When a firm's registration_status changes to Withdrawn and a
        deregistration rule exists, evaluate_alerts_for_firm should insert
        an AlertEvent.
        """
        from services.alert_service import evaluate_alerts_for_firm

        firm = Firm(
            crd_number=200001,
            legal_name="About To Withdraw LLC",
            registration_status="Withdrawn",
            last_filing_date=datetime.date(2024, 3, 31),
        )
        db.add(firm)

        rule = AlertRule(
            label="Deregistration Watch",
            rule_type="deregistration",
            delivery="log",
            active=True,
        )
        db.add(rule)
        db.flush()

        diffs = [{"field": "registration_status", "old": "Registered", "new": "Withdrawn"}]
        evaluate_alerts_for_firm(200001, diffs, db)
        db.flush()

        events = db.query(AlertEvent).filter_by(crd_number=200001).all()
        assert len(events) >= 1
        assert events[0].rule_type == "deregistration"
