from __future__ import annotations

import datetime

from sqlalchemy import BigInteger, ForeignKey, Integer, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class SyncManifestEntry(Base):
    """
    Tracks every file available in reports_metadata.json.
    One row per (file_type, file_name). Status prevents duplicate processing.
    """

    __tablename__ = "sync_manifest"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    file_type: Mapped[str] = mapped_column(Text, nullable=False)  # advFilingData, advBrochures, advW
    file_name: Mapped[str] = mapped_column(Text, nullable=False)  # exact fileName from metadata
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    display_name: Mapped[str | None] = mapped_column(Text)
    file_size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    uploaded_on: Mapped[datetime.datetime | None] = mapped_column()  # from metadata uploadedOn
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="pending")
    # pending → processing → complete | failed
    processed_at: Mapped[datetime.datetime | None] = mapped_column()
    records_processed: Mapped[int] = mapped_column(Integer, server_default="0")
    error_message: Mapped[str | None] = mapped_column(Text)
    sync_job_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("sync_jobs.id"))
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    __table_args__ = (UniqueConstraint("file_type", "file_name", name="uq_sync_manifest_file"),)
