"""add_last_iapd_refresh_at

Revision ID: 69c9643aaa75
Revises: 0011
Create Date: 2026-04-20 21:11:19.240863

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '69c9643aaa75'
down_revision: Union[str, None] = '0011'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('firms', sa.Column('last_iapd_refresh_at', sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column('firms', 'last_iapd_refresh_at')
