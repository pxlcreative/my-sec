# MySEC

A private, self-hosted database of ~37,000 SEC-registered investment adviser firms built on public IAPD/ADV data. Provides full-text and fuzzy firm search, historical AUM time series, ADV Part 2 PDF storage, bulk CRD matching, platform tagging, alerts, and on-demand due diligence Excel workbooks.

**Stack:** Python 3.12 · FastAPI · PostgreSQL 16 · Elasticsearch 8 · Redis 7 · Celery · React/TypeScript

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [First-Time Setup](#2-first-time-setup)
3. [Running the Platform](#3-running-the-platform)
4. [Loading SEC Data](#4-loading-sec-data)
5. [Verifying Everything Works](#5-verifying-everything-works)
6. [Day-to-Day Development](#6-day-to-day-development)
7. [Running Tests](#7-running-tests)
8. [Project Structure](#8-project-structure)
9. [Environment Variables Reference](#9-environment-variables-reference)
10. [Common Issues](#10-common-issues)
11. [Make Targets Reference](#11-make-targets-reference)
12. [CI/CD](#12-cicd)
13. [Upgrading an Existing Install](#13-upgrading-an-existing-install)

---

## 1. Prerequisites

Install these before anything else:

| Tool | Version | Install |
|------|---------|---------|
| Docker Desktop | ≥ 4.25 | https://www.docker.com/products/docker-desktop |
| Docker Compose | ≥ 2.23 (bundled with Docker Desktop) | — |
| Node.js | ≥ 20 LTS | https://nodejs.org or `brew install node` |
| Python | ≥ 3.12 (optional, for running scripts outside Docker) | https://www.python.org |
| Make | any | pre-installed on macOS/Linux |

Verify your install:

```bash
docker --version          # Docker version 24.x or higher
docker compose version    # Docker Compose version v2.x or higher
node --version            # v20.x or higher
```

---

## 2. First-Time Setup

Three steps. `make install` handles migrations, platform/schedule/questionnaire seeding, and Elasticsearch index creation automatically — no `make migrate` / `make seed` chaining required.

### 2a. Clone and configure environment

```bash
git clone <your-repo-url> mysec
cd mysec
cp .env.example .env
```

The defaults in `.env.example` work out of the box for local development. Only edit `.env` if you're changing ports or adding SMTP credentials. **Generate a real `SECRET_KEY`** — `make install` will warn if the placeholder is still there:

```bash
openssl rand -hex 32          # paste into .env as SECRET_KEY=...
```

See [Section 9](#9-environment-variables-reference) for the full variable list.

### 2b. Start all services

```bash
make install
```

This brings up PostgreSQL, Elasticsearch, Redis, the FastAPI backend, and both Celery services (worker + beat), then blocks until `/health` returns 200 — i.e. until the entrypoint has finished migrations, seeds, and ES index creation. First run will pull Docker images (2–3 minutes) and wait 30–60s for Elasticsearch to go green.

Under the hood: all three services (`api`, `celery_worker`, `celery_beat`) share `api/entrypoint.sh`. Whichever container starts first acquires a Postgres advisory lock and runs `alembic upgrade head` + all `seed_*` scripts + `es_client.create_index_if_not_exists("firms")`. The other two wait, then each hands off to its command (uvicorn, celery worker, celery beat). Re-running `make install` on an existing install is a safe no-op.

Confirm it's green:

```bash
make verify
```

Five-check readiness table — API health, Postgres, Elasticsearch, Redis, Celery worker.

### 2c. Start the frontend

```bash
cd frontend
npm install
npm run dev
```

The frontend runs on **http://localhost:5173** (Vite default).

---

## 3. Running the Platform

After first-time setup, daily startup is:

```bash
# Terminal 1 — backend
make up

# Terminal 2 — frontend
cd frontend && npm run dev
```

| Service | URL |
|---------|-----|
| Frontend | http://localhost:5173 |
| API | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |
| API docs (Redoc) | http://localhost:8000/redoc |
| Elasticsearch | http://localhost:9200 |

> **Note:** The platform is fully functional with zero SEC data loaded. All pages render with empty states, and all create/search/export flows work end-to-end. Load data when you're ready (see Section 4).

---

## 4. Loading SEC Data

Data loading is a one-time bulk operation followed by monthly incremental syncs. Each step is independent — you can stop and resume at any point.

### Step 1 — Download and import bulk historical CSV data (2000–2024)

This downloads ~3 GB of ZIP files from sec.gov and imports every ADV filing from Oct 2000 through Dec 2024 into PostgreSQL.

**Expected time:** 60–90 minutes depending on your connection and disk speed.

```bash
make load-data
```

This runs four scripts in sequence:
1. `scripts/load_bulk_csv.py` — downloads 3 legacy ZIPs, parses `IA_MAIN.csv`, upserts firms and AUM history (2000–2024)
2. `scripts/load_filing_data.py` — fetches `reports_metadata.json` from SEC, downloads all pending `advFilingData` and `advW` ZIPs (2025+), upserts firms and marks withdrawals
3. `scripts/index_firms_to_es.py` — indexes all firms into Elasticsearch for fuzzy search
4. `scripts/backfill_annual_aum.py` — populates the `aum_2023`/`aum_2024` convenience columns

Each step is idempotent — `load_filing_data.py` tracks processed files in the `sync_manifest` table and skips anything already marked complete. You can run scripts individually if needed:

```bash
docker compose exec api python scripts/load_bulk_csv.py
docker compose exec api python scripts/load_filing_data.py
docker compose exec api python scripts/index_firms_to_es.py
docker compose exec api python scripts/backfill_annual_aum.py
```

> **Storage:** Raw ZIP files are saved to `./data/raw/csv/` and are not deleted after import. They total ~3–4 GB. Once confirmed successful, you can delete them to reclaim space.

### Step 4 — Classify pre-2025 firms as Inactive

The 2000–2024 bulk CSVs did not include a `REGISTRATION_STATUS` column, so all pre-2025 firms were imported with a default status of `Registered`. Run this one-time SQL to reclassify them correctly:

```bash
docker compose exec postgres psql -U secadv -d secadv -c \
  "UPDATE firms SET registration_status = 'Inactive' WHERE registration_status = 'Registered' AND last_filing_date < '2025-01-01';"
```

**This step is required.** Without it, ~19,000 old firms will incorrectly appear as `Registered`. The 2025+ monthly CSVs carry an explicit status column, so only pre-2025 firms need this correction.

After running, confirm the distribution looks roughly correct:

```bash
docker compose exec postgres psql -U secadv -d secadv -c \
  "SELECT registration_status, COUNT(*) FROM firms GROUP BY 1 ORDER BY 2 DESC;"
# Expected: ~17,000 Registered, ~19,000 Inactive, ~1,000+ Withdrawn
```

### Step 5 — Trigger a manual monthly sync (optional)

This checks `reports_metadata.json` for any new files and processes them (filing data CSVs first, then brochure PDFs).

```bash
curl -X POST http://localhost:8000/api/sync/trigger
```

Or use the **Run Sync** button on the Sync Dashboard in the frontend. The sync discovers all pending files automatically — no month parameter needed.

PDFs are stored in `./data/brochures/`. Budget ~5–10 GB/year for storage.

### Step 6 — Verify data loaded correctly

```bash
# Five-check readiness table (API, Postgres, ES, Redis, Celery)
make verify

# Inspect the sync manifest — every file from reports_metadata.json plus the bulk
# CSV and ES-reindex runs get a row here. Status: pending | processing | complete | failed.
curl http://localhost:8000/api/sync/manifest | jq '.[] | {file_type, file_name, status, completed_at}'

# Check sync job history
curl http://localhost:8000/api/sync/status

# Search for a known firm by name
curl "http://localhost:8000/api/firms?q=vanguard"

# Check Elasticsearch index count
curl http://localhost:9200/firms/_count
```

The Sync dashboard in the frontend renders the same manifest view — useful for spotting `failed` rows without shelling out.

### Monthly sync (ongoing)

Celery Beat automatically runs `monthly_data_sync` on the schedule stored in the `cron_schedules` table (default: 2nd of every month at 06:00 UTC). Each run fetches `reports_metadata.json`, adds any new files to the `sync_manifest` table, then processes them in order: filing data → withdrawals → brochure PDFs.

You can view, edit, enable/disable, or manually trigger schedules from the **Sync → Schedules** tab in the frontend, or via the API (`GET/PATCH /api/schedules`, `POST /api/schedules/{id}/trigger`). Schedule changes take effect within 60 seconds without restarting Beat.

---

## 5. Verifying Everything Works

After setup (before any data is loaded), confirm each layer is healthy:

```bash
# 1. API health check
curl http://localhost:8000/health
# Expected: {"status": "ok"}

# 2. Database connected
curl http://localhost:8000/api/sync/status
# Expected: JSON with job_type list (may be empty)

# 3. Elasticsearch reachable
curl http://localhost:9200/_cluster/health
# Expected: {"status": "green"} or "yellow" (yellow is fine for single-node)

# 4. Celery worker alive
docker compose exec celery_worker celery -A celery_tasks.app inspect ping
# Expected: {"celery@...": {"ok": "pong"}}

# 5. Frontend loads
open http://localhost:5173
# Expected: Dashboard with empty states on all pages
```

---

## 6. Day-to-Day Development

### Backend (API / scripts)

The API container has hot-reload enabled via uvicorn `--reload`. Edit any file under `api/` and the server restarts automatically.

```bash
# View live API logs
docker compose logs -f api

# Open a shell inside the API container
docker compose exec api bash

# Run a one-off script
docker compose exec api python scripts/backfill_annual_aum.py

# Create a new Alembic migration after changing a model
docker compose exec api alembic revision --autogenerate -m "add_column_xyz"
docker compose exec api alembic upgrade head
```

### Frontend

Standard Vite dev server with HMR. Edit any file under `frontend/src/` and the browser updates instantly.

```bash
cd frontend
npm run dev       # dev server
npm run build     # production build
npm run preview   # preview production build locally
```

### Creating an external API key

The external API (brochure retrieval, bulk CRD lookup) requires a Bearer token. Generate one:

```bash
docker compose exec api python scripts/create_api_key.py --label "my-external-system"
```

This prints the raw key **once** — copy it immediately. It is never stored in plaintext. Use it in requests:

```bash
curl -H "Authorization: Bearer <your-key>" http://localhost:8000/api/external/firms/12345/brochure
```

---

## 7. Running Tests

### Backend (pytest)

```bash
make test
```

Or run specific files:

```bash
docker compose exec api pytest tests/test_firms_api.py -v
docker compose exec api pytest tests/test_matcher.py -v
docker compose exec api pytest tests/test_excel_generator.py -v
```

Tests use a separate `test_` database that is created and torn down automatically. No production data is touched. External systems (IAPD API, SEC downloads, SMTP, Elasticsearch) are mocked via the `mock_iapd`, `mock_es`, `mock_smtp`, and `mock_requests` fixtures in `tests/conftest.py`.

Run with coverage locally:

```bash
docker compose exec api pytest tests/ --cov=api --cov=scripts --cov-report=term
```

### Frontend (Vitest + Playwright)

```bash
cd frontend
npm test              # Vitest unit tests
npm run test:e2e      # Playwright E2E smoke (requires make install first)
```

### Coverage thresholds

CI's `coverage-check` job fails the PR if any bucket drops below its floor:

| Surface | Floor |
|---|---|
| `api/services/` | 80% |
| `api/celery_tasks/` | 80% |
| `api/routes/` | 70% |
| `scripts/` | 60% |

Floors are enforced by `scripts/check_coverage.py` against `coverage.xml`. Raise floors over time; lowering them requires explicit discussion.

---

## 8. Project Structure

```
mysec/
│
├── api/                        # FastAPI application
│   ├── main.py                 # App entry point, router registration, middleware
│   ├── db.py                   # SQLAlchemy engine, session factory, get_db() dependency
│   ├── config.py               # Pydantic settings loaded from .env
│   ├── models/                 # SQLAlchemy ORM models (one file per domain)
│   │   ├── firm.py             # Firm, FirmSnapshot, FirmChange
│   │   ├── aum.py              # FirmAumHistory
│   │   ├── brochure.py         # AdvBrochure
│   │   ├── platform.py         # PlatformDefinition, FirmPlatform
│   │   ├── alert.py            # AlertRule, AlertEvent
│   │   ├── sync_job.py         # SyncJob
│   │   ├── sync_manifest.py    # SyncManifestEntry (tracks files from metadata feed)
│   │   ├── export_job.py       # ExportJob
│   │   ├── api_key.py          # ApiKey
│   │   └── cron_schedule.py    # CronSchedule (Beat schedule definitions)
│   ├── routes/                 # FastAPI routers
│   │   ├── firms.py            # GET /api/firms, /api/firms/{crd}, etc.
│   │   ├── match.py            # POST /api/match/bulk
│   │   ├── platforms.py        # /api/platforms
│   │   ├── export.py           # /api/export
│   │   ├── alerts.py           # /api/alerts
│   │   ├── excel.py            # /api/firms/{crd}/due-diligence-excel
│   │   ├── external.py         # /api/external (Bearer token required)
│   │   ├── sync.py             # /api/sync/status, /api/sync/trigger
│   │   └── schedules.py        # /api/schedules (cron job management)
│   ├── schemas/                # Pydantic request/response schemas
│   ├── services/               # Business logic (not FastAPI-aware)
│   │   ├── firm_service.py
│   │   ├── iapd_client.py      # IAPD API calls + rate limiting
│   │   ├── change_detector.py  # Snapshot diffing
│   │   ├── firm_refresh_service.py
│   │   ├── matcher.py          # Fuzzy name+address matching
│   │   ├── metadata_service.py # Fetches reports_metadata.json, manages sync_manifest
│   │   ├── firm_brochure_service.py # Per-firm brochure fetch via IAPD API
│   │   ├── alert_service.py
│   │   ├── export_service.py
│   │   ├── excel_generator.py  # openpyxl DDQ workbook
│   │   ├── es_client.py        # Elasticsearch client + indexing
│   │   ├── platform_service.py
│   │   └── auth_service.py     # API key hashing + verification
│   └── celery_tasks/
│       ├── app.py              # Celery app configuration
│       ├── db_scheduler.py     # DatabaseScheduler — loads Beat schedules from DB
│       ├── monthly_sync.py     # monthly_data_sync task (filing data + brochures)
│       ├── refresh_tasks.py    # refresh_firm_task
│       ├── export_tasks.py     # run_export_job task + cleanup beat task
│       └── match_tasks.py      # run_bulk_match task
│
├── alembic/                    # Database migrations
│   ├── env.py
│   └── versions/               # Auto-generated migration files
│
├── scripts/                    # One-off and admin scripts
│   ├── load_bulk_csv.py        # MODULE A1: bulk historical load (2000–2024)
│   ├── load_filing_data.py     # MODULE A2: 2025+ monthly filing data from metadata feed
│   ├── index_firms_to_es.py    # Index all firms into Elasticsearch
│   ├── backfill_annual_aum.py  # MODULE C: populate aum_2023/aum_2024 columns
│   ├── seed_platforms.py       # Insert default platform definitions
│   ├── seed_schedules.py       # Insert default Celery Beat schedules
│   ├── create_api_key.py       # Generate a new external API key
│   ├── init_db.py              # Run migrations (alias for alembic upgrade head)
│   └── export_openapi.py       # Write openapi.json to docs/
│
├── tests/
│   ├── conftest.py             # pytest fixtures, test DB, seeded firm records
│   ├── test_firms_api.py
│   ├── test_match_api.py
│   ├── test_export_api.py
│   ├── test_alerts.py
│   ├── test_external_api.py
│   └── test_excel_generator.py
│
├── frontend/                   # React/TypeScript app (Vite)
│   ├── src/
│   │   ├── api/client.ts       # Axios instance pointed at localhost:8000
│   │   ├── pages/
│   │   │   ├── Search.tsx          # Search & Explore
│   │   │   ├── FirmDetail.tsx      # Firm Detail (tabbed)
│   │   │   ├── BulkMatch.tsx       # Bulk Match Tool
│   │   │   ├── Platforms.tsx       # Platform Manager
│   │   │   ├── PlatformDetail.tsx  # Platform firm list + bulk untag
│   │   │   ├── Alerts.tsx          # Alert Rules + Event Feed
│   │   │   ├── Export.tsx          # Export Center
│   │   │   └── Sync.tsx            # Sync Dashboard
│   │   ├── components/         # Shared UI components
│   │   └── main.tsx
│   ├── package.json
│   └── vite.config.ts
│
├── data/                       # Runtime data (git-ignored)
│   ├── raw/csv/                # Downloaded ZIP files from SEC
│   ├── brochures/              # ADV Part 2 PDFs by CRD
│   └── exports/                # Async export output files (24hr TTL)
│
├── docs/
│   └── openapi.json            # Generated OpenAPI spec
│
├── docker-compose.yml
├── .env.example
├── .env                        # Local overrides (git-ignored)
├── Makefile
├── requirements.txt
├── CLAUDE.md                   # Claude Code conventions for this project
└── README.md
```

---

## 9. Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://postgres:postgres@postgres:5432/sec_adviser` | PostgreSQL connection string |
| `ELASTICSEARCH_URL` | `http://elasticsearch:9200` | Elasticsearch endpoint |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection string |
| `SECRET_KEY` | *(required)* | Random secret for internal signing — generate with `openssl rand -hex 32` |
| `DATA_DIR` | `/data` | Root path for brochures, exports, and raw CSV storage |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `SMTP_HOST` | — | SMTP server hostname for email alerts (optional) |
| `SMTP_PORT` | `587` | SMTP port |
| `SMTP_USER` | — | SMTP username |
| `SMTP_PASS` | — | SMTP password |
| `CORS_ORIGINS` | `http://localhost:5173` | Comma-separated allowed CORS origins |

---

## 10. Common Issues

### "Connection refused" on API startup

PostgreSQL or Elasticsearch may still be initializing. The API container retries on startup, but if it crashes:

```bash
docker compose restart api
```

### Elasticsearch yellow/red health

Single-node Elasticsearch always reports `yellow` (no replica shards). This is normal and does not affect functionality. Only `red` requires attention:

```bash
curl http://localhost:9200/_cluster/health?pretty
```

### Alembic "Target database is not up to date"

Run migrations before starting the API:

```bash
make migrate
```

### Bulk CSV import hangs or fails mid-way

Both load scripts are idempotent — re-running skips already-processed data:

```bash
# 2000–2024 historical data (skips already-downloaded ZIPs)
docker compose exec api python scripts/load_bulk_csv.py

# 2025+ monthly data (skips files already complete in sync_manifest)
docker compose exec api python scripts/load_filing_data.py
```

Check the log output for the specific ZIP and row count where it stopped.

### Frontend shows "Network Error" on API calls

Confirm the API is running and CORS is configured:

```bash
curl http://localhost:8000/health
```

If the API is up but the frontend still fails, check the proxy configuration in `frontend/vite.config.ts` — it should forward `/api` requests to `http://localhost:8000`.

### "No firms found" after data load

Elasticsearch indexing runs as a separate step. If you loaded the CSV but skipped indexing:

```bash
docker compose exec api python scripts/index_firms_to_es.py
```

---

## 11. Make Targets Reference

| Target | What it does |
|--------|-------------|
| `make install` | One-command install: copies `.env` from `.env.example` if missing, brings services up, blocks until `/health` is 200. Use this for first-time setup. |
| `make up` | Start all Docker services in detached mode (no health wait) |
| `make down` | Stop and remove all containers |
| `make restart` | `down` then `up` |
| `make verify` | Five-check readiness table (API / Postgres / ES / Redis / Celery worker). Run after `make install` or any time you suspect a service is down. |
| `make logs` | Tail logs for all services |
| `make logs-api` | Tail API logs only |
| `make logs-worker` | Tail Celery worker logs only |
| `make logs-beat` | Tail Celery beat logs only |
| `make migrate` | Run `alembic upgrade head` in the API container (entrypoint already does this; this is an escape hatch) |
| `make seed` | Insert default platforms, schedules, and questionnaires (entrypoint already does this; escape hatch) |
| `make seed-schedules` | Seed Celery Beat schedules only |
| `make load-data` | Full data pipeline: bulk CSV (2000–2024) → 2025+ filing data → ES index → AUM backfill |
| `make test` | Run the full pytest suite |
| `make test-frontend` | Run frontend Vitest unit tests |
| `make reindex` | Re-index all firms into Elasticsearch (useful after schema changes) |
| `make dlq-inspect` | List tasks that exhausted retries and landed in the `dead_letter` queue |
| `make shell` | Open a bash shell inside the API container |
| `make ps` | Show status of all running containers |

---

## 12. CI/CD

GitHub Actions workflow at `.github/workflows/ci.yml`. Five jobs:

| Job | When | What it does |
|---|---|---|
| `backend` | push + PR | Lint (ruff) → mypy (permissive) → pytest with `--cov=api --cov=scripts`, uploads `coverage.xml` |
| `frontend` | push + PR | Lint → Vitest → production build |
| `coverage-check` | push + PR (after `backend`) | Runs `scripts/check_coverage.py` against `coverage.xml`; fails if any bucket drops below its floor |
| `smoke-install` | push + PR | Fresh-runner `cp .env.example .env && make install && make verify && make test`. Catches regressions in the auto-init entrypoint. |
| `sec-schema-drift` | weekly (Mon 14:00 UTC) | Runs `scripts/probe_sec_schema.py` against live SEC endpoints. Fails loud if `reports_metadata.json` shape, URL patterns, or advW column headers drift. |

Reproduce locally:

```bash
# Backend gate
ruff check api scripts tests
pytest tests/ --cov=api --cov=scripts --cov-report=xml
COV_SERVICES_MIN=80 COV_CELERY_MIN=80 COV_ROUTES_MIN=70 COV_SCRIPTS_MIN=60 \
    python scripts/check_coverage.py coverage.xml

# Smoke install
docker compose down -v
cp .env.example .env && make install && make verify && make test

# Schema drift (runs against live SEC endpoints — be polite with frequency)
python scripts/probe_sec_schema.py --months 2
```

---

## 13. Upgrading an Existing Install

If you're on an older commit that predates the auto-init entrypoint (anything before the Phase 1 distribution hardening), this is the upgrade path:

```bash
git pull
docker compose build            # rebuild images with the new entrypoint
make restart                    # entrypoint runs pending migrations + seeds on start
make verify                     # confirm all five checks are green
```

No data loss — Alembic handles the migration, existing `firms`, `sync_manifest`, and platform rows are preserved. If `make verify` flags anything red, check `make logs-api` for the entrypoint output.

If you have local uncommitted schedules in `cron_schedules` that conflict with the seeded defaults, the seed scripts skip existing rows by name, so your customisations are safe. Same for platforms and questionnaires.