import datetime

from sqlalchemy import BigInteger, Date, DateTime, ForeignKey, Index, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class AdvBrochure(Base):
    __tablename__ = "adv_brochures"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crd_number: Mapped[int] = mapped_column(
        Integer, ForeignKey("firms.crd_number"), nullable=False
    )
    brochure_version_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    brochure_name: Mapped[str | None] = mapped_column(Text)
    date_submitted: Mapped[datetime.date | None] = mapped_column(Date)
    source_month: Mapped[str | None] = mapped_column(Text)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_size_bytes: Mapped[int | None] = mapped_column(Integer)
    downloaded_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())

    # Reducto parse results — populated on demand via /parse endpoint
    parse_status: Mapped[str | None] = mapped_column(Text)  # 'success' | 'failed'
    parse_error: Mapped[str | None] = mapped_column(Text)
    parsed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    parsed_markdown: Mapped[str | None] = mapped_column(Text)
    parsed_chunks: Mapped[list | None] = mapped_column(JSONB)
    reducto_job_id: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("idx_brochures_crd", "crd_number", "date_submitted"),
    )
