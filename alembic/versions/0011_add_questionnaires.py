"""add_questionnaires

Revision ID: 0011
Revises: 0010
Create Date: 2026-04-07

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0011"
down_revision: Union[str, None] = "0010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "questionnaire_templates",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("style_type", sa.Text(), nullable=False, server_default="custom"),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    op.create_table(
        "questionnaire_questions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("template_id", sa.Integer(), nullable=False),
        sa.Column("section", sa.Text(), nullable=False, server_default="General"),
        sa.Column("order_index", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("question_text", sa.Text(), nullable=False),
        sa.Column("answer_field_path", sa.Text(), nullable=True),
        sa.Column("answer_hint", sa.Text(), nullable=True),
        sa.Column("notes_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["template_id"], ["questionnaire_templates.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "questionnaire_responses",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("template_id", sa.Integer(), nullable=False),
        sa.Column("crd_number", sa.Integer(), nullable=False),
        sa.Column(
            "generated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
        sa.Column(
            "answers",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "analyst_notes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "ai_suggested",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("status", sa.Text(), nullable=False, server_default="draft"),
        sa.ForeignKeyConstraint(
            ["template_id"], ["questionnaire_templates.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["crd_number"], ["firms.crd_number"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_index(
        "ix_questionnaire_responses_template_crd",
        "questionnaire_responses",
        ["template_id", "crd_number"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_questionnaire_responses_template_crd", "questionnaire_responses")
    op.drop_table("questionnaire_responses")
    op.drop_table("questionnaire_questions")
    op.drop_table("questionnaire_templates")
