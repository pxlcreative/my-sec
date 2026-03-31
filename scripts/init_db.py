#!/usr/bin/env python3
"""
Run Alembic migrations to bring the database up to head.

Usage (from project root, with .env in place):
    python scripts/init_db.py

Or inside the running api container:
    docker compose exec api python /app/../scripts/init_db.py
"""
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent


def main() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        print("ERROR: alembic upgrade failed.", file=sys.stderr)
        sys.exit(result.returncode)
    print("Database is up to date.")


if __name__ == "__main__":
    main()
