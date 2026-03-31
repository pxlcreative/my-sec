import datetime

from sqlalchemy import BigInteger, ForeignKey, Integer, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class CustomPropertyDefinition(Base):
    __tablename__ = "custom_property_definitions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    field_type: Mapped[str] = mapped_column(Text, nullable=False)
    options: Mapped[list | None] = mapped_column(JSONB)
    created_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())


class FirmCustomProperty(Base):
    __tablename__ = "firm_custom_properties"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crd_number: Mapped[int] = mapped_column(
        Integer, ForeignKey("firms.crd_number"), nullable=False
    )
    definition_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("custom_property_definitions.id"), nullable=False
    )
    value: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("crd_number", "definition_id", name="uq_firm_custom_property"),
    )
