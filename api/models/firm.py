import datetime

from sqlalchemy import (
    BigInteger,
    Date,
    ForeignKey,
    Index,
    Integer,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.base import Base


class Firm(Base):
    __tablename__ = "firms"

    crd_number: Mapped[int] = mapped_column(Integer, primary_key=True)
    sec_number: Mapped[str | None] = mapped_column(Text)
    legal_name: Mapped[str] = mapped_column(Text, nullable=False)
    business_name: Mapped[str | None] = mapped_column(Text)
    registration_status: Mapped[str | None] = mapped_column(Text)
    firm_type: Mapped[str | None] = mapped_column(Text)
    aum_total: Mapped[int | None] = mapped_column(BigInteger)
    aum_discretionary: Mapped[int | None] = mapped_column(BigInteger)
    aum_non_discretionary: Mapped[int | None] = mapped_column(BigInteger)
    num_accounts: Mapped[int | None] = mapped_column(Integer)
    num_employees: Mapped[int | None] = mapped_column(Integer)
    main_street1: Mapped[str | None] = mapped_column(Text)
    main_street2: Mapped[str | None] = mapped_column(Text)
    main_city: Mapped[str | None] = mapped_column(Text)
    main_state: Mapped[str | None] = mapped_column(Text)
    main_zip: Mapped[str | None] = mapped_column(Text)
    main_country: Mapped[str | None] = mapped_column(Text)
    phone: Mapped[str | None] = mapped_column(Text)
    website: Mapped[str | None] = mapped_column(Text)
    fiscal_year_end: Mapped[str | None] = mapped_column(Text)
    org_type: Mapped[str | None] = mapped_column(Text)
    raw_adv: Mapped[dict | None] = mapped_column(JSONB)
    last_filing_date: Mapped[datetime.date | None] = mapped_column(Date)
    aum_2023: Mapped[int | None] = mapped_column(BigInteger)
    aum_2024: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now(), onupdate=func.now())

    snapshots: Mapped[list["FirmSnapshot"]] = relationship(back_populates="firm")
    changes: Mapped[list["FirmChange"]] = relationship(back_populates="firm")

    __table_args__ = (
        Index(
            "idx_firms_legal_name",
            text("to_tsvector('english', legal_name)"),
            postgresql_using="gin",
        ),
        Index(
            "idx_firms_business_name",
            text("to_tsvector('english', coalesce(business_name, ''))"),
            postgresql_using="gin",
        ),
        Index("idx_firms_state", "main_state"),
        Index("idx_firms_status", "registration_status"),
    )


class FirmSnapshot(Base):
    __tablename__ = "firm_snapshots"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crd_number: Mapped[int] = mapped_column(
        Integer, ForeignKey("firms.crd_number"), nullable=False
    )
    snapshot_hash: Mapped[str] = mapped_column(Text, nullable=False)
    raw_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    synced_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now(), nullable=False)

    firm: Mapped["Firm"] = relationship(back_populates="snapshots")

    __table_args__ = (
        Index("idx_snapshots_crd", "crd_number", "synced_at"),
    )


class FirmChange(Base):
    __tablename__ = "firm_changes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crd_number: Mapped[int] = mapped_column(
        Integer, ForeignKey("firms.crd_number"), nullable=False
    )
    field_path: Mapped[str] = mapped_column(Text, nullable=False)
    old_value: Mapped[str | None] = mapped_column(Text)
    new_value: Mapped[str | None] = mapped_column(Text)
    detected_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now(), nullable=False)
    snapshot_from: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("firm_snapshots.id")
    )
    snapshot_to: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("firm_snapshots.id")
    )

    firm: Mapped["Firm"] = relationship(back_populates="changes")

    __table_args__ = (
        Index("idx_changes_crd", "crd_number", "detected_at"),
        Index("idx_changes_field", "field_path"),
    )
