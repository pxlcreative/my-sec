import datetime

from sqlalchemy import ForeignKey, Integer, func
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class FirmDisclosuresSummary(Base):
    __tablename__ = "firm_disclosures_summary"

    crd_number: Mapped[int] = mapped_column(
        Integer, ForeignKey("firms.crd_number"), primary_key=True
    )
    criminal_count: Mapped[int] = mapped_column(Integer, server_default="0")
    regulatory_count: Mapped[int] = mapped_column(Integer, server_default="0")
    civil_count: Mapped[int] = mapped_column(Integer, server_default="0")
    customer_count: Mapped[int] = mapped_column(Integer, server_default="0")
    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )
