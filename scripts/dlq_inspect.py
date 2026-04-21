#!/usr/bin/env python3
"""
Inspect the Celery dead_letter queue — tasks that exhausted their retry budget
and were routed to the DLQ instead of being dropped.

Usage:
    docker compose exec api python scripts/dlq_inspect.py
    docker compose exec api python scripts/dlq_inspect.py --drain   # requeue all to their original queue
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "api"))
load_dotenv(PROJECT_ROOT / ".env")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--drain", action="store_true", help="Requeue all DLQ tasks and exit")
    parser.add_argument("--limit", type=int, default=25, help="How many tasks to show")
    args = parser.parse_args()

    import redis
    from config import settings

    r = redis.from_url(settings.redis_url)
    count = r.llen("dead_letter")
    print(f"dead_letter queue has {count} task(s).")

    if count == 0:
        return 0

    if args.drain:
        moved = 0
        while True:
            raw = r.lpop("dead_letter")
            if raw is None:
                break
            try:
                payload = json.loads(raw)
                original_queue = payload.get("properties", {}).get("delivery_info", {}).get("routing_key", "celery")
                r.rpush(original_queue, raw)
                moved += 1
            except Exception as exc:
                print(f"  skip: could not requeue ({exc})")
        print(f"Requeued {moved} task(s).")
        return 0

    # Show the first N without consuming them.
    entries = r.lrange("dead_letter", 0, args.limit - 1)
    for i, raw in enumerate(entries, 1):
        try:
            payload = json.loads(raw)
            headers = payload.get("headers", {})
            task = headers.get("task", "?")
            task_id = headers.get("id", "?")
            args_ = headers.get("argsrepr", "?")
            print(f"  [{i}] {task} id={task_id} args={args_}")
        except Exception as exc:
            print(f"  [{i}] (could not parse: {exc})")

    if count > args.limit:
        print(f"  ... and {count - args.limit} more. Use --limit to show more, --drain to requeue all.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
