import datetime

from sqlalchemy import BigInteger, Boolean, ForeignKey, Index, Integer, Numeric, Text, func, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base


class AlertRule(Base):
    __tablename__ = "alert_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    rule_type: Mapped[str] = mapped_column(Text, nullable=False)
    platform_ids: Mapped[list[int] | None] = mapped_column(ARRAY(Integer))
    crd_numbers: Mapped[list[int] | None] = mapped_column(ARRAY(Integer))
    threshold_pct: Mapped[float | None] = mapped_column(Numeric(6, 2))
    operator: Mapped[str | None] = mapped_column(Text, server_default="lte")
    field_path: Mapped[str | None] = mapped_column(Text)
    match_old_value: Mapped[str | None] = mapped_column(Text)
    match_new_value: Mapped[str | None] = mapped_column(Text)
    delivery: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="in_app"
    )
    delivery_target: Mapped[str | None] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="true"
    )
    created_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())


class AlertEvent(Base):
    __tablename__ = "alert_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    rule_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("alert_rules.id"), nullable=False
    )
    crd_number: Mapped[int] = mapped_column(Integer, nullable=False)
    firm_name: Mapped[str | None] = mapped_column(Text)
    rule_type: Mapped[str] = mapped_column(Text, nullable=False)
    field_path: Mapped[str | None] = mapped_column(Text)
    old_value: Mapped[str | None] = mapped_column(Text)
    new_value: Mapped[str | None] = mapped_column(Text)
    platform_name: Mapped[str | None] = mapped_column(Text)
    fired_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now(), nullable=False)
    delivered_at: Mapped[datetime.datetime | None] = mapped_column(nullable=True)
    delivery_status: Mapped[str | None] = mapped_column(
        Text, server_default="pending"
    )
    firm_change_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("firm_changes.id", ondelete="SET NULL"), nullable=True, index=True
    )

    __table_args__ = (
        Index("idx_alert_events_rule", "rule_id", "fired_at"),
        Index("idx_alert_events_crd", "crd_number", "fired_at"),
        Index(
            "idx_alert_events_rule_change_uq",
            "rule_id",
            "firm_change_id",
            unique=True,
            postgresql_where=text("firm_change_id IS NOT NULL"),
        ),
    )
