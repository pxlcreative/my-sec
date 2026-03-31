import datetime

from sqlalchemy import BigInteger, Integer, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class SyncJob(Base):
    __tablename__ = "sync_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="pending"
    )
    source_url: Mapped[str | None] = mapped_column(Text)
    firms_processed: Mapped[int] = mapped_column(Integer, server_default="0")
    firms_updated: Mapped[int] = mapped_column(Integer, server_default="0")
    changes_detected: Mapped[int] = mapped_column(Integer, server_default="0")
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime.datetime | None] = mapped_column(nullable=True)
    completed_at: Mapped[datetime.datetime | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())
