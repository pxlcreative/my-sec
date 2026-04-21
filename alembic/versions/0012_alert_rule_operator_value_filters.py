"""Add operator, match_old_value, match_new_value to alert_rules

Revision ID: 0012
Revises: 69c9643aaa75
Create Date: 2026-04-20

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0012"
down_revision = "69c9643aaa75"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "alert_rules",
        sa.Column("operator", sa.Text(), nullable=True, server_default="lte"),
    )
    op.add_column(
        "alert_rules",
        sa.Column("match_old_value", sa.Text(), nullable=True),
    )
    op.add_column(
        "alert_rules",
        sa.Column("match_new_value", sa.Text(), nullable=True),
    )
    # Existing aum_decline_pct rules stored threshold_pct as a positive number with
    # implicit negation in the evaluator. Migrate them to signed values so the new
    # evaluator can apply the operator directly: pct_change {op} threshold_pct.
    op.execute(
        sa.text(
            "UPDATE alert_rules "
            "SET operator = 'lte', threshold_pct = -ABS(threshold_pct) "
            "WHERE rule_type = 'aum_decline_pct' AND threshold_pct > 0"
        )
    )


def downgrade() -> None:
    # Restore positive threshold_pct for aum_decline_pct rules before dropping column
    op.execute(
        sa.text(
            "UPDATE alert_rules "
            "SET threshold_pct = ABS(threshold_pct) "
            "WHERE rule_type = 'aum_decline_pct'"
        )
    )
    op.drop_column("alert_rules", "match_new_value")
    op.drop_column("alert_rules", "match_old_value")
    op.drop_column("alert_rules", "operator")
