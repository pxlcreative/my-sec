# MySEC

A private, self-hosted database of SEC-registered investment adviser firms built on public IAPD/ADV data. Provides full-text and fuzzy firm search, historical AUM time series, ADV Part 2 PDF storage, bulk CRD matching, platform tagging, alerts, and on-demand due diligence Excel workbooks.

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

### 2a. Clone and configure environment

```bash
git clone <your-repo-url> mysec
cd mysec

# Copy the example env file and fill in any local overrides
cp .env.example .env
```

The defaults in `.env.example` work out of the box for local development — you only need to edit `.env` if you're changing ports or adding SMTP credentials for email alerts. See [Section 9](#9-environment-variables-reference) for the full variable list.

### 2b. Start all services

```bash
make up
```

This starts PostgreSQL, Elasticsearch, Redis, the FastAPI backend, and both Celery services (worker + beat scheduler). First run will pull Docker images — allow 2–3 minutes.

Wait until you see `api_1 | INFO: Application startup complete` in the logs:

```bash
docker compose logs -f api
```

### 2c. Run database migrations

```bash
make migrate
```

This runs `alembic upgrade head` inside the API container, creating all tables and indexes.

### 2d. Seed platform definitions

```bash
make seed
```

This inserts the default platforms (Orion, Envestnet, Schwab, Fidelity, Pershing) into the database. Safe to re-run — it skips existing records.

### 2e. Start the frontend

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

### Step 1 — Download and import bulk historical CSV data

This downloads ~3 GB of ZIP files from sec.gov and imports every ADV filing from Oct 2000 through Dec 2024 into PostgreSQL. It populates the `firms` and `firm_aum_history` tables.

**Expected time:** 45–90 minutes depending on your connection and disk speed.

```bash
make load-data
```

This runs three scripts in sequence:
1. `scripts/load_bulk_csv.py` — downloads ZIPs, parses `IA_MAIN.csv`, upserts firms and AUM history
2. `scripts/index_firms_to_es.py` — indexes all firms into Elasticsearch for fuzzy search
3. `scripts/backfill_annual_aum.py` — populates the `aum_2023`/`aum_2024` convenience columns

You can run them individually if needed:

```bash
docker compose exec api python scripts/load_bulk_csv.py
docker compose exec api python scripts/index_firms_to_es.py
docker compose exec api python scripts/backfill_annual_aum.py
```

> **Storage:** The raw ZIP files are saved to `./data/raw/csv/` and are not deleted after import. They total ~3 GB. Once the import is confirmed successful, you can delete them to reclaim space.

### Step 2 — Trigger a manual monthly PDF sync (optional)

This downloads ADV Part 2 brochure PDFs for the most recent published month.

```bash
# Sync the most recent available month
curl -X POST http://localhost:8000/api/sync/trigger

# Sync a specific month
curl -X POST "http://localhost:8000/api/sync/trigger?month_str=2025-02"
```

PDFs are stored in `./data/brochures/{crd}/`. Budget ~5–10 GB/year for storage.

### Step 3 — Verify data loaded correctly

```bash
# Check firm count
curl http://localhost:8000/api/sync/status

# Search for a known firm by name
curl "http://localhost:8000/api/firms?q=vanguard"

# Check Elasticsearch index count
curl http://localhost:9200/firms/_count
```

### Monthly sync (ongoing)

Celery Beat automatically triggers a PDF sync on the 2nd of every month at 06:00 UTC. No manual action needed once the platform is running. Monitor sync jobs on the **Sync Dashboard** page in the frontend.

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

```bash
make test
```

Or run specific test files:

```bash
docker compose exec api pytest tests/test_firms_api.py -v
docker compose exec api pytest tests/test_matcher.py -v
docker compose exec api pytest tests/test_excel_generator.py -v
```

Tests use a separate `test_` database that is created and torn down automatically. No production data is touched.

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
│   │   ├── export_job.py       # ExportJob
│   │   └── api_key.py          # ApiKey
│   ├── routes/                 # FastAPI routers
│   │   ├── firms.py            # GET /api/firms, /api/firms/{crd}, etc.
│   │   ├── match.py            # POST /api/match/bulk
│   │   ├── platforms.py        # /api/platforms
│   │   ├── export.py           # /api/export
│   │   ├── alerts.py           # /api/alerts
│   │   ├── excel.py            # /api/firms/{crd}/due-diligence-excel
│   │   ├── external.py         # /api/external (Bearer token required)
│   │   └── sync.py             # /api/sync/status, /api/sync/trigger
│   ├── schemas/                # Pydantic request/response schemas
│   ├── services/               # Business logic (not FastAPI-aware)
│   │   ├── firm_service.py
│   │   ├── iapd_client.py      # IAPD API calls + rate limiting
│   │   ├── change_detector.py  # Snapshot diffing
│   │   ├── firm_refresh_service.py
│   │   ├── matcher.py          # Fuzzy name+address matching
│   │   ├── pdf_sync_service.py # Monthly PDF download
│   │   ├── alert_service.py
│   │   ├── export_service.py
│   │   ├── excel_generator.py  # openpyxl DDQ workbook
│   │   ├── es_client.py        # Elasticsearch client + indexing
│   │   ├── platform_service.py
│   │   └── auth_service.py     # API key hashing + verification
│   └── celery_tasks/
│       ├── app.py              # Celery app + Beat schedule
│       ├── monthly_sync.py     # monthly_pdf_sync task
│       ├── refresh_tasks.py    # refresh_firm_task
│       ├── export_tasks.py     # run_export_job task + cleanup beat task
│       └── match_tasks.py      # run_bulk_match task
│
├── alembic/                    # Database migrations
│   ├── env.py
│   └── versions/               # Auto-generated migration files
│
├── scripts/                    # One-off and admin scripts
│   ├── load_bulk_csv.py        # MODULE A: bulk historical load
│   ├── index_firms_to_es.py    # Index all firms into Elasticsearch
│   ├── backfill_annual_aum.py  # MODULE C: populate aum_2023/aum_2024 columns
│   ├── seed_platforms.py       # Insert default platform definitions
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

The script is idempotent — restarting it skips already-imported rows:

```bash
docker compose exec api python scripts/load_bulk_csv.py
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
| `make up` | Start all Docker services in detached mode |
| `make down` | Stop and remove all containers |
| `make restart` | `down` then `up` |
| `make logs` | Tail logs for all services |
| `make migrate` | Run `alembic upgrade head` in the API container |
| `make seed` | Insert default platform definitions |
| `make load-data` | Full data pipeline: CSV import → ES index → AUM backfill |
| `make test` | Run the full pytest suite |
| `make reindex` | Re-index all firms into Elasticsearch (useful after schema changes) |
| `make shell` | Open a bash shell inside the API container |
| `make ps` | Show status of all running containers |