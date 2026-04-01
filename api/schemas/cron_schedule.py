from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class CronScheduleOut(BaseModel):
    id: int
    name: str
    task: str
    description: str | None
    minute: str
    hour: str
    day_of_month: str
    month_of_year: str
    day_of_week: str
    enabled: bool
    updated_at: datetime

    model_config = {"from_attributes": True}


class CronSchedulePatch(BaseModel):
    minute: str | None = None
    hour: str | None = None
    day_of_month: str | None = None
    month_of_year: str | None = None
    day_of_week: str | None = None
    enabled: bool | None = None
    description: str | None = None
