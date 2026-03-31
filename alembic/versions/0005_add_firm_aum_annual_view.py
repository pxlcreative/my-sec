"""add_firm_aum_annual_view

Revision ID: 0005
Revises: 0004
Create Date: 2026-03-31

"""
from typing import Sequence, Union

from alembic import op

revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE VIEW firm_aum_annual AS
        SELECT
            crd_number,
            EXTRACT(YEAR FROM filing_date)::INTEGER AS year,
            MAX(aum_total) AS peak_aum,
            MIN(aum_total) AS trough_aum,
            (ARRAY_AGG(aum_total ORDER BY filing_date DESC))[1] AS latest_aum_for_year,
            COUNT(*) AS filing_count
        FROM firm_aum_history
        WHERE aum_total IS NOT NULL
        GROUP BY crd_number, EXTRACT(YEAR FROM filing_date)
    """)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS firm_aum_annual")
