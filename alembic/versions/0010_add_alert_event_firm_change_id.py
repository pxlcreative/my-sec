"""Add firm_change_id to alert_events for batch deduplication

Revision ID: 0010
Revises: 0009
Create Date: 2026-04-02

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "alert_events",
        sa.Column(
            "firm_change_id",
            sa.BigInteger(),
            sa.ForeignKey("firm_changes.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index(
        "idx_alert_events_firm_change",
        "alert_events",
        ["firm_change_id"],
    )
    # Partial unique index: prevents double-firing for change-anchored alerts.
    # NULL rows (AUM decline, legacy) are excluded — they use a different dedup check.
    op.create_index(
        "idx_alert_events_rule_change_uq",
        "alert_events",
        ["rule_id", "firm_change_id"],
        unique=True,
        postgresql_where=sa.text("firm_change_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_alert_events_rule_change_uq", table_name="alert_events")
    op.drop_index("idx_alert_events_firm_change", table_name="alert_events")
    op.drop_column("alert_events", "firm_change_id")
