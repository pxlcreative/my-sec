"""
Management script: create a new API key.

Usage:
    docker compose run --rm api python /project/scripts/create_api_key.py --label "Partner ABC"

Prints the raw key ONCE — it is never stored in plaintext.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))


def main():
    parser = argparse.ArgumentParser(description="Create a new external API key.")
    parser.add_argument("--label", required=True, help="Human-readable label for this key")
    args = parser.parse_args()

    from db import SessionLocal
    from models.api_key import ApiKey
    from services.auth_service import generate_api_key

    raw_key, key_hash = generate_api_key()

    db = SessionLocal()
    try:
        existing = db.query(ApiKey).filter_by(key_hash=key_hash).first()
        if existing:
            print("ERROR: Key hash collision — this should never happen.", file=sys.stderr)
            sys.exit(1)

        api_key = ApiKey(key_hash=key_hash, label=args.label, active=True)
        db.add(api_key)
        db.commit()
        db.refresh(api_key)
    finally:
        db.close()

    print()
    print(f"API key created (id={api_key.id}, label={args.label!r})")
    print()
    print(f"  RAW KEY (copy now — never shown again):")
    print(f"  {raw_key}")
    print()
    print("  Usage:  Authorization: Bearer <raw_key>")
    print()


if __name__ == "__main__":
    main()
