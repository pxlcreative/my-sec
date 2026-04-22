#!/usr/bin/env python3
"""
Seed the cron_schedules table with default scheduled jobs.
Safe to run multiple times — skips rows that already exist by name.

Usage:
    docker compose exec api python scripts/seed_schedules.py
"""
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "api"))
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_SCHEDULES = [
    {
        "name": "monthly-data-sync",
        "task": "monthly_sync.monthly_data_sync",
        "description": "Monthly data sync — filing data + brochures from reports.adviserinfo.sec.gov",
        "minute": "0",
        "hour": "6",
        "day_of_month": "2",
        "month_of_year": "*",
        "day_of_week": "*",
        "enabled": True,
    },
    {
        "name": "cleanup-expired-exports",
        "task": "export_tasks.cleanup_expired_exports",
        "description": "Hourly cleanup of expired export files from disk",
        "minute": "5",
        "hour": "*",
        "day_of_month": "*",
        "month_of_year": "*",
        "day_of_week": "*",
        "enabled": True,
    },
]


def main() -> None:
    from sqlalchemy import select
    from db import SessionLocal
    from models.cron_schedule import CronSchedule

    db = SessionLocal()
    try:
        inserted = 0
        skipped = 0

        # Disable the old monthly-pdf-sync schedule if it still exists
        old = db.scalars(
            select(CronSchedule).where(CronSchedule.name == "monthly-pdf-sync")
        ).first()
        if old:
            old.enabled = False
            print("  disable monthly-pdf-sync (replaced by monthly-data-sync)")

        for entry in DEFAULT_SCHEDULES:
            existing = db.scalars(
                select(CronSchedule).where(CronSchedule.name == entry["name"])
            ).first()
            if existing:
                print(f"  skip   {entry['name']} (already exists, id={existing.id})")
                skipped += 1
            else:
                db.add(CronSchedule(**entry))
                print(f"  insert {entry['name']}")
                inserted += 1
        db.commit()
        print(f"\nDone. Inserted {inserted}, skipped {skipped}.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
