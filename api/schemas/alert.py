import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

AlertRuleType = Literal["deregistration", "aum_decline_pct", "field_change"]
AlertDelivery = Literal["in_app", "email", "webhook"]
AlertOperator = Literal["lt", "lte", "gt", "gte"]
DeliveryStatus = Literal["pending", "sent", "failed"]


class AlertRuleCreate(BaseModel):
    label: str = Field(..., min_length=1, max_length=200)
    rule_type: AlertRuleType
    platform_ids: list[int] | None = None
    crd_numbers: list[int] | None = None
    threshold_pct: float | None = None          # signed %; negative = decline
    operator: AlertOperator | None = "lte"      # comparison operator for threshold_pct
    field_path: str | None = None
    match_old_value: str | None = None          # optional filter on old_value for field_change
    match_new_value: str | None = None          # optional filter on new_value for field_change
    delivery: AlertDelivery = "in_app"
    delivery_target: str | None = None
    active: bool = True


class AlertRuleUpdate(BaseModel):
    label: str | None = Field(default=None, min_length=1, max_length=200)
    platform_ids: list[int] | None = None
    crd_numbers: list[int] | None = None
    threshold_pct: float | None = None
    operator: AlertOperator | None = None
    field_path: str | None = None
    match_old_value: str | None = None
    match_new_value: str | None = None
    delivery: AlertDelivery | None = None
    delivery_target: str | None = None
    active: bool | None = None


class AlertRuleOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    label: str
    rule_type: str
    platform_ids: list[int] | None
    crd_numbers: list[int] | None
    threshold_pct: float | None
    operator: str | None
    field_path: str | None
    match_old_value: str | None
    match_new_value: str | None
    delivery: str
    delivery_target: str | None
    active: bool
    created_at: datetime.datetime | None


class AlertEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    rule_id: int
    crd_number: int
    firm_name: str | None
    rule_type: str
    field_path: str | None
    old_value: str | None
    new_value: str | None
    platform_name: str | None
    fired_at: datetime.datetime
    delivered_at: datetime.datetime | None
    delivery_status: str | None
    firm_change_id: int | None = None


class AlertTestResponse(BaseModel):
    rule_id: int
    delivery: str
    delivery_target: str | None
    success: bool
    message: str


class AlertEvaluateResponse(BaseModel):
    rule_id: int
    fired: int
    rule_type: str
    label: str
