"""add_firms_aum_year_columns

Revision ID: 0006
Revises: 0005
Create Date: 2026-03-31

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("firms", sa.Column("aum_2023", sa.BigInteger(), nullable=True))
    op.add_column("firms", sa.Column("aum_2024", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("firms", "aum_2024")
    op.drop_column("firms", "aum_2023")
