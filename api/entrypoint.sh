#!/bin/bash
# Container entrypoint for sec-adv (api / celery_worker / celery_beat).
#
# All three services share this entrypoint. Whichever container starts first
# runs the migrate + seed block (guarded by a Postgres advisory lock so the
# others block until it finishes, then continue to their real command).
#
# Environment:
#   DATABASE_URL           Required. Postgres connection string.
#   ELASTICSEARCH_URL      Optional. Defaults to http://elasticsearch:9200.
#   REDIS_URL              Optional. Defaults to redis://redis:6379/0.
#   SKIP_ENTRYPOINT_INIT   If "1", skip migrate/seed/index-creation.
#                          Use for scripts that manage their own lifecycle.

set -e

cd /project

log() {
    echo "[entrypoint] $*" >&2
}

wait_for_postgres() {
    local timeout=60 deadline
    deadline=$(( $(date +%s) + timeout ))
    log "Waiting for Postgres..."
    while true; do
        if python - <<'PY' 2>/dev/null
import os, sys
import psycopg2
try:
    psycopg2.connect(os.environ["DATABASE_URL"]).close()
except Exception:
    sys.exit(1)
PY
        then
            log "Postgres ready."
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            log "ERROR: Postgres not ready within ${timeout}s."
            return 1
        fi
        sleep 2
    done
}

wait_for_redis() {
    local timeout=30 deadline
    deadline=$(( $(date +%s) + timeout ))
    log "Waiting for Redis..."
    while true; do
        if python - <<'PY' 2>/dev/null
import os, sys
import redis
try:
    redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0")).ping()
except Exception:
    sys.exit(1)
PY
        then
            log "Redis ready."
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            log "ERROR: Redis not ready within ${timeout}s."
            return 1
        fi
        sleep 1
    done
}

wait_for_elasticsearch() {
    local timeout=180 deadline
    deadline=$(( $(date +%s) + timeout ))
    local es_url="${ELASTICSEARCH_URL:-http://elasticsearch:9200}"
    log "Waiting for Elasticsearch at ${es_url}..."
    while true; do
        if curl -sf "${es_url}/_cluster/health" 2>/dev/null | grep -q '"status":"\(green\|yellow\)"'; then
            log "Elasticsearch ready."
            return 0
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            log "ERROR: Elasticsearch not ready within ${timeout}s."
            return 1
        fi
        sleep 3
    done
}

run_migrate_and_seed() {
    log "Acquiring advisory lock for migrate+seed (other containers will wait)..."
    python - <<'PY'
import os
import subprocess
import sys

import psycopg2

# Arbitrary 32-bit integer key — must be stable across processes.
LOCK_KEY = 0x5EC1DB0A
SEED_SCRIPTS = ["seed_platforms.py", "seed_schedules.py", "seed_questionnaires.py"]


def fail(msg: str, code: int = 1) -> None:
    print(f"[entrypoint] ERROR: {msg}", flush=True)
    sys.exit(code)


conn = psycopg2.connect(os.environ["DATABASE_URL"])
conn.autocommit = True
cur = conn.cursor()

# Blocks until we acquire — other containers running the same entrypoint
# will wait here, then release immediately when they reach this point and
# re-run the (idempotent) migrate+seed block themselves. Alembic and the
# seed scripts are all idempotent, so re-running is a no-op.
cur.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
try:
    print("[entrypoint] Running alembic upgrade head...", flush=True)
    r = subprocess.run(["alembic", "upgrade", "head"], cwd="/project")
    if r.returncode != 0:
        fail(f"alembic upgrade failed (exit {r.returncode})")

    for name in SEED_SCRIPTS:
        script = f"/project/scripts/{name}"
        if not os.path.exists(script):
            print(f"[entrypoint] {name} not found, skipping.", flush=True)
            continue
        print(f"[entrypoint] Running {name}...", flush=True)
        r = subprocess.run(["python", script], cwd="/project")
        if r.returncode != 0:
            fail(f"{name} failed (exit {r.returncode})")

    print("[entrypoint] Ensuring Elasticsearch firms index exists...", flush=True)
    sys.path.insert(0, "/project/api")
    try:
        from services.es_client import create_index_if_not_exists
        create_index_if_not_exists()
    except Exception as exc:
        # Non-fatal: the API can start without the index (it will be created
        # on first indexing call). Log and continue so the container boots.
        print(f"[entrypoint] WARNING: ES index creation skipped: {exc}", flush=True)
finally:
    cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
    cur.close()
    conn.close()
PY
}

wait_for_postgres
wait_for_redis
wait_for_elasticsearch

if [ "${SKIP_ENTRYPOINT_INIT:-0}" = "1" ]; then
    log "SKIP_ENTRYPOINT_INIT=1 — skipping migrate/seed/index-creation."
else
    run_migrate_and_seed
fi

if [ "$#" -eq 0 ]; then
    log "No command supplied; nothing to exec. Exiting."
    exit 0
fi

log "Handing off to: $*"
exec "$@"
