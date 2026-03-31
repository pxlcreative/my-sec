import datetime

from sqlalchemy import BigInteger, Date, ForeignKey, Index, Integer, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class FirmAumHistory(Base):
    __tablename__ = "firm_aum_history"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crd_number: Mapped[int] = mapped_column(
        Integer, ForeignKey("firms.crd_number"), nullable=False
    )
    filing_date: Mapped[datetime.date] = mapped_column(Date, nullable=False)
    aum_total: Mapped[int | None] = mapped_column(BigInteger)
    aum_discretionary: Mapped[int | None] = mapped_column(BigInteger)
    aum_non_discretionary: Mapped[int | None] = mapped_column(BigInteger)
    num_accounts: Mapped[int | None] = mapped_column(Integer)
    source: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint("crd_number", "filing_date", "source", name="uq_aum_history"),
        Index("idx_aum_history_crd", "crd_number"),
        Index("idx_aum_history_date", "filing_date"),
    )
