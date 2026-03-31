"""add_firm_disclosures_summary

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-30

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "firm_disclosures_summary",
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column("criminal_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("regulatory_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("civil_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("customer_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(["crd_number"], ["firms.crd_number"]),
        sa.PrimaryKeyConstraint("crd_number"),
    )


def downgrade() -> None:
    op.drop_table("firm_disclosures_summary")
