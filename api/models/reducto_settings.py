from __future__ import annotations

import datetime

from sqlalchemy import Boolean, DateTime, Integer, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class ReductoSettings(Base):
    __tablename__ = "reducto_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    api_key: Mapped[str | None] = mapped_column(Text)
    base_url: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="https://platform.reducto.ai"
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")

    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
