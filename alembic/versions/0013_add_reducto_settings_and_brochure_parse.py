"""add reducto_settings table and brochure parse columns

Revision ID: 0013
Revises: 0012
Create Date: 2026-04-23

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # reducto_settings — singleton config row, mirrors storage_settings
    # ------------------------------------------------------------------
    op.create_table(
        "reducto_settings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("api_key", sa.Text(), nullable=True),
        sa.Column(
            "base_url",
            sa.Text(),
            nullable=False,
            server_default="https://platform.reducto.ai",
        ),
        sa.Column(
            "enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    # Idempotent seed of singleton row
    op.execute(
        "INSERT INTO reducto_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING"
    )

    # ------------------------------------------------------------------
    # adv_brochures — parse result columns
    # ------------------------------------------------------------------
    op.add_column("adv_brochures", sa.Column("parse_status", sa.Text(), nullable=True))
    op.add_column("adv_brochures", sa.Column("parse_error", sa.Text(), nullable=True))
    op.add_column(
        "adv_brochures",
        sa.Column("parsed_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.add_column("adv_brochures", sa.Column("parsed_markdown", sa.Text(), nullable=True))
    op.add_column(
        "adv_brochures",
        sa.Column("parsed_chunks", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column("adv_brochures", sa.Column("reducto_job_id", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("adv_brochures", "reducto_job_id")
    op.drop_column("adv_brochures", "parsed_chunks")
    op.drop_column("adv_brochures", "parsed_markdown")
    op.drop_column("adv_brochures", "parsed_at")
    op.drop_column("adv_brochures", "parse_error")
    op.drop_column("adv_brochures", "parse_status")
    op.drop_table("reducto_settings")
