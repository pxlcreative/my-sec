"""
AlertRule factory helpers for tests.

Usage:
    def test_something(db):
        rule = deregistration_rule(db, delivery="email", delivery_target="ops@x.com")
"""
from __future__ import annotations

from typing import Any


def _create(db, **kwargs: Any):
    from models.alert import AlertRule
    rule = AlertRule(**kwargs)
    db.add(rule)
    db.flush()
    return rule


def deregistration_rule(
    db,
    *,
    label: str = "Deregistration watch",
    platform_ids: list[int] | None = None,
    crd_numbers: list[int] | None = None,
    delivery: str = "in_app",
    delivery_target: str | None = None,
    active: bool = True,
):
    """AlertRule of type=deregistration."""
    return _create(
        db,
        label=label,
        rule_type="deregistration",
        platform_ids=platform_ids,
        crd_numbers=crd_numbers,
        delivery=delivery,
        delivery_target=delivery_target,
        active=active,
    )


def aum_decline_rule(
    db,
    *,
    label: str = "AUM decline watch",
    threshold_pct: float = -25.0,
    operator: str = "lte",
    platform_ids: list[int] | None = None,
    crd_numbers: list[int] | None = None,
    delivery: str = "in_app",
    delivery_target: str | None = None,
    active: bool = True,
):
    """AlertRule of type=aum_decline_pct. Default fires on ≥25% YoY drop."""
    return _create(
        db,
        label=label,
        rule_type="aum_decline_pct",
        threshold_pct=threshold_pct,
        operator=operator,
        platform_ids=platform_ids,
        crd_numbers=crd_numbers,
        delivery=delivery,
        delivery_target=delivery_target,
        active=active,
    )


def field_change_rule(
    db,
    *,
    label: str = "Field change watch",
    field_path: str = "main_state",
    match_old_value: str | None = None,
    match_new_value: str | None = None,
    platform_ids: list[int] | None = None,
    crd_numbers: list[int] | None = None,
    delivery: str = "in_app",
    delivery_target: str | None = None,
    active: bool = True,
):
    """AlertRule of type=field_change with optional old/new value filters."""
    return _create(
        db,
        label=label,
        rule_type="field_change",
        field_path=field_path,
        match_old_value=match_old_value,
        match_new_value=match_new_value,
        platform_ids=platform_ids,
        crd_numbers=crd_numbers,
        delivery=delivery,
        delivery_target=delivery_target,
        active=active,
    )
