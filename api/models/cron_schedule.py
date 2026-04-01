from __future__ import annotations

import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class CronSchedule(Base):
    __tablename__ = "cron_schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    task: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(String)
    minute: Mapped[str] = mapped_column(String, nullable=False, server_default="0")
    hour: Mapped[str] = mapped_column(String, nullable=False, server_default="0")
    day_of_month: Mapped[str] = mapped_column(String, nullable=False, server_default="*")
    month_of_year: Mapped[str] = mapped_column(String, nullable=False, server_default="*")
    day_of_week: Mapped[str] = mapped_column(String, nullable=False, server_default="*")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
