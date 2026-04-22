#!/usr/bin/env python3
"""
Insert default platform definitions into the database.
Safe to run multiple times — uses INSERT ... ON CONFLICT DO NOTHING.

Usage:
    docker compose run --rm api python /project/scripts/seed_platforms.py
"""
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "api"))
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_PLATFORMS = [
    ("Orion",     "Orion Advisor Solutions portfolio management platform"),
    ("Envestnet", "Envestnet wealth management platform"),
    ("Schwab",    "Charles Schwab Advisor Services custodian"),
    ("Fidelity",  "Fidelity Institutional custodian"),
    ("Pershing",  "Pershing / BNY Mellon custodian"),
]


def main() -> None:
    from db import SessionLocal
    from models.platform import PlatformDefinition

    db = SessionLocal()
    try:
        inserted = 0
        skipped = 0
        for name, description in DEFAULT_PLATFORMS:
            from sqlalchemy import select
            existing = db.scalars(
                select(PlatformDefinition).where(PlatformDefinition.name == name)
            ).first()
            if existing:
                print(f"  skip  {name} (already exists, id={existing.id})")
                skipped += 1
            else:
                db.add(PlatformDefinition(name=name, description=description))
                print(f"  insert {name}")
                inserted += 1
        db.commit()
        print(f"\nDone. Inserted {inserted}, skipped {skipped}.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
