from __future__ import annotations

import datetime

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class StorageSettings(Base):
    __tablename__ = "storage_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    backend: Mapped[str] = mapped_column(String(16), nullable=False, server_default="local")

    # S3
    s3_bucket: Mapped[str | None] = mapped_column(Text)
    s3_region: Mapped[str | None] = mapped_column(Text)
    s3_access_key_id: Mapped[str | None] = mapped_column(Text)
    s3_secret_access_key: Mapped[str | None] = mapped_column(Text)
    s3_endpoint_url: Mapped[str | None] = mapped_column(Text)

    # Azure
    azure_container: Mapped[str | None] = mapped_column(Text)
    azure_connection_string: Mapped[str | None] = mapped_column(Text)

    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
