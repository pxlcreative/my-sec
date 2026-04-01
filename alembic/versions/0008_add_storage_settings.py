"""add_storage_settings

Revision ID: 0008
Revises: e5ba5348632a
Create Date: 2026-03-31

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0008"
down_revision: Union[str, None] = "e5ba5348632a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "storage_settings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("backend", sa.String(16), nullable=False, server_default="local"),
        sa.Column("s3_bucket", sa.Text(), nullable=True),
        sa.Column("s3_region", sa.Text(), nullable=True),
        sa.Column("s3_access_key_id", sa.Text(), nullable=True),
        sa.Column("s3_secret_access_key", sa.Text(), nullable=True),
        sa.Column("s3_endpoint_url", sa.Text(), nullable=True),
        sa.Column("azure_container", sa.Text(), nullable=True),
        sa.Column("azure_connection_string", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    # Seed the singleton row
    op.execute("INSERT INTO storage_settings (id, backend) VALUES (1, 'local')")


def downgrade() -> None:
    op.drop_table("storage_settings")
