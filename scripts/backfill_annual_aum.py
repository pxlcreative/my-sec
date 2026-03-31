"""
Module C – Backfill aum_2023 / aum_2024 on the firms table.

Reads from the firm_aum_annual view (latest_aum_for_year for each year)
and batch-updates firms in chunks of 1000.

Usage (inside the api container):
    python /project/scripts/backfill_annual_aum.py
"""
import logging
import os
import sys

# Make api/ importable when run from within the container
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

BATCH_SIZE = 1000
TARGET_YEARS = (2023, 2024)


def fetch_annual_aum(conn, year: int) -> dict[int, int]:
    """Return {crd_number: latest_aum_for_year} for *year*."""
    rows = conn.execute(
        text(
            "SELECT crd_number, latest_aum_for_year "
            "FROM firm_aum_annual WHERE year = :year AND latest_aum_for_year IS NOT NULL"
        ),
        {"year": year},
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def batch_update(conn, col: str, data: dict[int, int]) -> int:
    """
    UPDATE firms SET {col} = value WHERE crd_number = crd
    Processes in batches of BATCH_SIZE. Returns total rows updated.
    """
    items = list(data.items())
    total_updated = 0

    for start in range(0, len(items), BATCH_SIZE):
        chunk = items[start: start + BATCH_SIZE]
        # Build a VALUES list for a single UPDATE … FROM (VALUES …) statement
        values_sql = ", ".join(f"({crd}, {aum})" for crd, aum in chunk)
        conn.execute(text(
            f"UPDATE firms SET {col} = v.aum "
            f"FROM (VALUES {values_sql}) AS v(crd, aum) "
            f"WHERE firms.crd_number = v.crd"
        ))
        total_updated += len(chunk)
        log.info(
            "  %s: updated rows %d–%d / %d",
            col, start + 1, start + len(chunk), len(items),
        )

    return total_updated


def main() -> None:
    from config import settings
    engine = create_engine(settings.database_url)

    log.info("Starting backfill_annual_aum for years %s", TARGET_YEARS)

    with engine.begin() as conn:
        for year in TARGET_YEARS:
            col = f"aum_{year}"
            log.info("Fetching firm_aum_annual for year=%d …", year)
            data = fetch_annual_aum(conn, year)
            log.info("  %d firms have %s data", len(data), col)

            if not data:
                log.info("  Nothing to update for %d, skipping.", year)
                continue

            updated = batch_update(conn, col, data)
            log.info("  Backfill complete for %s: %d firm(s) updated", col, updated)

    log.info("backfill_annual_aum finished.")


if __name__ == "__main__":
    main()
