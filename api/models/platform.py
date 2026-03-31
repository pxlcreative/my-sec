import datetime

from sqlalchemy import BigInteger, ForeignKey, Index, Integer, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class PlatformDefinition(Base):
    __tablename__ = "platform_definitions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())


class FirmPlatform(Base):
    __tablename__ = "firm_platforms"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crd_number: Mapped[int] = mapped_column(
        Integer, ForeignKey("firms.crd_number"), nullable=False
    )
    platform_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("platform_definitions.id"), nullable=False
    )
    tagged_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())
    tagged_by: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        UniqueConstraint("crd_number", "platform_id", name="uq_firm_platform"),
        Index("idx_firm_platforms_crd", "crd_number"),
        Index("idx_firm_platforms_platform", "platform_id"),
    )
