# CLAUDE.md — MySEC

This file tells Claude Code how this project is structured and what conventions to follow.
Read it before making any changes. Update it when conventions change.

---

## What This Project Does

Private, self-hosted database of ~37,000 SEC investment adviser firms (active and historical).
Sources data from public IAPD/ADV bulk CSVs and live IAPD API. Key capabilities:
- Fuzzy bulk name+address → CRD matching (Elasticsearch + rapidfuzz)
- Historical AUM time series back to 2000
- ADV Part 2 PDF storage and retrieval
- Platform tagging for firms (Orion, Envestnet, etc.)
- Monthly sync via Celery Beat
- On-demand due diligence Excel workbooks (openpyxl)
- External API with Bearer token auth

---

## Running the Project

```bash
make up          # start all Docker services
make migrate     # run alembic upgrade head (after model changes)
make seed        # insert default platforms + seed cron schedules
make test        # run pytest suite
make load-data   # full SEC data pipeline (takes 45-90 min, one-time)
```

Frontend: `cd frontend && npm run dev` → http://localhost:5173
API: http://localhost:8000
API docs: http://localhost:8000/docs

---

## Project Layout

```
api/
  main.py              # FastAPI app, router registration
  db.py                # SQLAlchemy engine + get_db() dependency
  config.py            # Pydantic settings from .env
  models/              # SQLAlchemy ORM models
  routes/              # FastAPI routers
  schemas/             # Pydantic request/response models
  services/            # Pure business logic (no FastAPI imports)
  celery_tasks/        # Celery tasks + Beat scheduler (app.py, db_scheduler.py, monthly_sync.py [monthly_data_sync task], refresh_tasks.py, export_tasks.py, match_tasks.py)
scripts/               # One-off admin/data scripts
alembic/               # Database migrations
tests/                 # pytest test suite
frontend/              # React/TypeScript (Vite)
data/                  # Runtime files (git-ignored): CSVs, PDFs, exports
```

---

## Architecture Decisions

### Database
- **PostgreSQL 16** for all relational data and JSONB blobs
- `raw_adv JSONB` on the `firms` table stores the full IAPD API response
- Every table has `created_at TIMESTAMPTZ DEFAULT NOW()`; mutable tables also have `updated_at`
- `updated_at` is maintained by a SQLAlchemy `onupdate` trigger, not a Postgres trigger

### Search
- **Elasticsearch 8** is the search layer — Postgres GIN is a fallback only
- ES index name: `firms`
- ES document includes: crd_number, legal_name, business_name, main_city, main_state, main_zip, registration_status, plus a `normalized_name` field (suffixes stripped, lowercased)
- Keep ES and Postgres in sync: every `firms` row update must call `es_client.index_firm()`

### Task Queue
- **Celery** with Redis broker for async tasks and Beat for scheduled jobs
- Beat schedules are stored in the `cron_schedules` DB table — **not hardcoded** in `app.py`
- `celery_tasks/db_scheduler.py` (`DatabaseScheduler`) reads from DB on startup and re-syncs every 60 seconds, so schedule edits via the UI take effect without restarting Beat
- Manage schedules via the UI (Sync → Schedules tab) or API (`GET/PATCH /api/schedules`, `POST /api/schedules/{id}/trigger`)
- Seed default schedules: `make seed-schedules` (also runs as part of `make seed`)
- The active monthly sync task is `monthly_sync.monthly_data_sync` — it runs three phases:
  advFilingData CSVs → advW withdrawals → advBrochures PDFs, all driven by `sync_manifest`
- After each monthly sync, `batch_verify_registration_status` is dispatched automatically — it enqueues `refresh_firm_task` for post-2025 Registered firms not yet verified against the live IAPD API (to populate `raw_adv` and catch status changes between CSV cycles). Pre-2025 firms are excluded — they are already classified as Inactive.
- All long-running operations (bulk match >100 rows, exports >500 firms) run as Celery tasks
- Task results and status are stored in the `sync_jobs` or `export_jobs` DB tables, not in Celery result backend
- `sync_manifest` tracks every file from `reports_metadata.json` — check it before re-running syncs

### External API
- Routes under `/api/external/` require `Authorization: Bearer <key>` header
- Keys are stored as SHA-256 hashes in the `api_keys` table (never plaintext)
- Generate keys with: `docker compose exec api python scripts/create_api_key.py --label "name"`
- Rate limit: 100 req/min per key, enforced via Redis sliding window

---

## Coding Conventions

### SQLAlchemy Models
- **Always use `Mapped[T]` annotations on every column.** SQLAlchemy 2.0 requires this or Alembic autogenerate will silently miss columns.
  ```python
  # Correct
  crd_number: Mapped[int] = mapped_column(Integer, primary_key=True)
  aum_total: Mapped[int | None] = mapped_column(BigInteger)
  # Wrong — do not use Column() without Mapped
  aum_total = Column(BigInteger)
  ```
- The `firm_aum_annual` VIEW is a PostgreSQL view, not a table — it has no SQLAlchemy model. Query it with `db.execute(text("SELECT ... FROM firm_aum_annual WHERE ..."))`. Do not define an ORM class for it.
- `FirmSnapshot` and `FirmChange` are written by `services/change_detector.py` during each refresh cycle. `FirmSnapshot` stores the full field dict as JSONB; `FirmChange` stores individual field diffs with `snapshot_from`/`snapshot_to` FK links. Alert evaluation in `services/alert_service.py` reads `FirmChange` to detect deregistration and AUM decline events.

### Python
- Python 3.12, type hints on all function signatures
- Use `from __future__ import annotations` in all files
- Services (`api/services/`) must not import from `api/routes/` — dependency flows one way
- Services accept a SQLAlchemy `Session` as a parameter; they do not call `get_db()`
- All DB writes use explicit transactions; do not rely on autocommit
- Use batch operations (bulk insert, executemany) for any loop over >100 rows
- `requests` calls to the IAPD API must always: set a User-Agent header, sleep 0.5s between calls, retry with exponential backoff on 429/503

### Naming
- DB table names: `snake_case` plural (e.g., `firms`, `firm_platforms`)
- SQLAlchemy model classes: `PascalCase` singular (e.g., `Firm`, `FirmPlatform`)
- Pydantic schema classes: suffix with `In` (request) or `Out` (response), e.g., `FirmOut`, `MatchRequestIn`
- Route files: named after the resource (e.g., `firms.py`, `platforms.py`)
- Service files: named `{resource}_service.py`

### AUM Field Mapping (IAPD JSON)
These paths are used when parsing live IAPD API responses:
```
FormInfo.Part1A.Item5F.Q5F2C → aum_total          (USD, not millions)
FormInfo.Part1A.Item5F.Q5F2A → aum_discretionary
FormInfo.Part1A.Item5F.Q5F2B → aum_non_discretionary
Info.FirmCrdNb               → crd_number
Info.BusNm                   → business_name
Info.Nm                      → legal_name
```

### Column Name Normalization (Bulk CSV)
The 2000–2011 and 2011–2024 bulk ZIPs use different column names. The normalizer in
`scripts/load_bulk_csv.py` maps all variants to these canonical names:
- `crd_number` ← CRD_NUMBER, CRDNumber
- `filing_date` ← ADV_FILING_DATE, FILING_DATE
- `aum_total` ← ITEM5F_2C, Item5F2C
- `aum_discretionary` ← ITEM5F_2A, Item5F2A
- `aum_non_discretionary` ← ITEM5F_2B, Item5F2B
- `registration_status` ← REGISTRATION_STATUS, RegStatus

### Firm Name Normalization
Run before ES indexing and before fuzzy matching. Strip these suffixes (case-insensitive):
`LLC, L.L.C., LP, L.P., LLP, L.L.P., Inc, Inc., Corp, Corp., Ltd, Ltd., Co, Co.`
Then: lowercase, remove all punctuation, collapse multiple spaces.
Store original in `legal_name`, normalized in ES `normalized_name` field.

### Zip Code Handling
Always store and compare only the first 5 digits. Never store ZIP+4 — it is inconsistent
across data sources.

### State Abbreviations
Always store as 2-letter uppercase codes (CA, NY, TX). The matcher normalizes full state
names from external input before comparison.

---

## Data Gotchas

- **AUM lags by up to 15 months.** Advisers file annually within 90 days of fiscal year end.
  The `aum_total` on a firm record may reflect data from a year ago — this is expected.
- **One CRD, many CSV rows.** `IA_MAIN.csv` has one row per ADV amendment per firm.
  Always use `FILING_DATE DESC` to identify the current record for a given CRD.
- **Brochure ZIPs are never downloaded.** PDFs are fetched per-firm using the IAPD firm
  search API (`api.adviserinfo.sec.gov/search/firm/{crd}`) which returns
  `brochures.brochuredetails` with `{brochureVersionID, brochureName, dateSubmitted}` (date
  format: `M/D/YYYY`). PDFs are downloaded individually via `files.adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={id}`.
  Only firms on platforms with `save_brochures=True` receive brochure fetches.
- **The sync_manifest table is the single source of truth for what has been processed.**
  Every file from `reports_metadata.json` gets a row in `sync_manifest`. Status transitions:
  `pending → processing → complete | failed`. Never re-process a `complete` entry.
- **Registration status has three values with different authoritative sources:**
  - `Registered` — confirmed from 2025+ monthly `advFilingData` CSVs, which carry an explicit
    `REGISTRATION_STATUS` column. Only firms whose last filing is on or after 2025-01-01 can
    be trusted to carry this value accurately.
  - `Withdrawn` — confirmed from monthly `advW` CSVs (coverage starts Jan 2025). The advW CSVs
    use `"CRD Number"` (space, not underscore) and datetime format `MM/DD/YYYY HH:MM:SS AM/PM`.
    Pre-2025 withdrawals are not in the data — those firms will appear as `Inactive` instead.
  - `Inactive` — derived status for firms whose `last_filing_date < 2025-01-01`. Old bulk CSVs
    (2000–2024) did not include a `REGISTRATION_STATUS` column, so pre-2025 firms defaulted to
    `Registered` on import. After bulk load, a one-time SQL classifies them correctly (see
    README Section 4, Step 4). The live IAPD API returns EDGAR-format documents for these old
    CRDs (no `Info.RegistrationStatus` field), so live refresh cannot fix their status either.
- **After any fresh bulk load, run the inactive classification SQL** to reclassify stale firms:
  ```sql
  UPDATE firms SET registration_status = 'Inactive'
  WHERE registration_status = 'Registered' AND last_filing_date < '2025-01-01';
  ```
  Without this step, ~19,000 pre-2025 firms will incorrectly show as `Registered`.
- **`batch_verify_registration_status` only targets post-2025 firms.** Pre-2025 firms are
  already classified as `Inactive` and IAPD returns EDGAR-format responses for them anyway.
  The task scope: `last_filing_date >= 2025-01-01`, `registration_status = 'Registered'`,
  `last_iapd_refresh_at` null or older than `refresh_cooldown_days`.

---

## Frontend Conventions

- **React Query** for all server state — no manual fetch/useEffect patterns
- **@tanstack/react-table** for all data tables
- **Recharts** for all charts
- **react-dropzone** for CSV upload on Bulk Match page
- API base URL: proxied via Vite (`/api` → `http://localhost:8000`) — there is no `VITE_API_URL` env var; do not add one
- All pages must render a useful empty state when data arrays are empty (no blank white screens)
- Loading states: use skeleton loaders, not spinners, for table/list content
- Error states: use an inline error card with a retry button; do not throw to the error boundary for API errors
- URL params: Search page filters are stored in URL search params so URLs are shareable
- Sidebar is collapsible (toggle button in header); collapsed state stored in local `useState`

### Empty State Requirements
Every data-fetching page must show a non-broken empty state before SEC data is loaded:
- **Search & Explore:** "No firms found" message with instructions to load data
- **Firm Detail:** 404 page with link back to search
- **Bulk Match:** Upload prompt (works before data load — matching simply returns no_match)
- **Platform Manager:** "No platforms yet" with create form visible
- **Alerts:** "No alert rules" with create button
- **Export Center:** Filter form renders; export with 0 results returns an empty file gracefully
- **Sync Dashboard:** Status cards show "Never synced" state; job history table is empty; Schedules tab shows "No schedules configured" with seed instructions

---

## Distribution invariants

A fresh clone must produce a working install with `cp .env.example .env && make install`.
Nothing in the pipeline can assume a human ran `make migrate` or `make seed` first.

- **`api/entrypoint.sh` must remain idempotent.** All three services (api, celery_worker,
  celery_beat) share this entrypoint. The migrate + seed + ES-index block is guarded by a
  Postgres advisory lock (`SELECT pg_try_advisory_lock(...)`) so only the first container to
  acquire the lock runs it; the others wait, then continue to their real command. Never add
  state that the entrypoint doesn't seed on a fresh DB.
- **Every seed script must be safely re-runnable.** `seed_platforms.py`, `seed_schedules.py`,
  and `seed_questionnaires.py` are called on every container start — a second run must be a
  no-op, not a duplicate-key error.
- **ES index creation belongs in the entrypoint**, not in search code. `es_client.create_index_if_not_exists("firms")`
  runs on startup so the first search can't 404. Search paths must not auto-create indexes.
- **`make verify` is the readiness contract.** If you add a new critical dependency (new
  container, new external service), extend `make verify` to cover it. CI's `smoke-install`
  job runs `make verify` on a clean clone — breaking it breaks distribution.
- **`SKIP_ENTRYPOINT_INIT=1`** bypasses migrate/seed/index-create. Use only for scripts that
  manage their own lifecycle (e.g. one-off admin containers). Never set it in production.

## Celery task contract

Every task must declare these explicitly — inherited defaults are not enough:

- `soft_time_limit` and `time_limit` — kill runaway jobs before they exhaust the worker
  pool. Global defaults in `celery_tasks/app.py` (50 min soft, 60 min hard) cover most
  cases; override per-task for anything expected to run longer or shorter.
- `max_retries` and an explicit retry policy. Tasks that hit the SEC API or do large DB
  writes must retry transient failures (429, 503, connection reset) with exponential
  backoff. `match_tasks.run_bulk_match` is the reference — `max_retries=2`.
- **Idempotency on any task that writes to DB.** Re-running the task must not duplicate
  rows or corrupt state. `task_acks_late=True` means tasks can re-run after worker crashes;
  design for it.
- **Progress updates for long tasks.** Anything expected to run >30s must update a
  `SyncJob` row (or `ExportJob` for exports) at least every 30s so the UI can render
  progress. The dashboards poll these tables, not Celery's result backend.
- **Dead-letter queue.** Tasks that exhaust retries land in the `dead_letter` queue.
  Inspect with `make dlq-inspect`. Do not silently catch-and-log in task bodies — if a
  failure is unrecoverable, let it raise so the DLQ captures it.
- **Queue routing.** Bulk match runs on the `match` queue so it doesn't block small
  interactive tasks. Add a new queue via `QUEUES` in `celery_tasks/app.py` and wire it in
  `task_routes`; update the worker `-Q` flag in `docker-compose.yml` to match.

## Bulk import rules

Long-running scripts (`load_bulk_csv.py`, `load_filing_data.py`, `index_firms_to_es.py`)
sit outside the Celery task contract but share the same reliability requirements:

- **Explicit transaction boundaries.** Wrap each batch in `BEGIN/COMMIT/ROLLBACK`. A
  failure mid-batch must leave no partial state. `autocommit` is off-limits.
- **`requests.Retry` adapter on every SEC download.** 3 attempts, exponential backoff on
  429/5xx/timeout. The SEC CDN rate-limits aggressively during peak hours and a single
  transient failure should never abort a multi-hour job.
- **Every completed unit of work gets a `sync_manifest` row.** Bulk CSVs, 2025+ monthly
  files, and full ES reindexes all write manifest entries so the dashboard shows what's
  been processed and when. Check the manifest before re-running — `status='complete'`
  entries are never re-processed.
- **Manifest status transitions are `pending → processing → complete | failed`.** The
  interim `processing` state exists so a crash mid-job doesn't cause duplicate
  re-processing on the next run. Never skip the intermediate state.
- **ZIP integrity checks on download.** `zipfile.ZipFile(dest).testzip()` after every
  download; re-download on corruption. Trust no SEC file blindly.
- **Count parse errors, don't swallow them.** Per-row parse failures emit a warning and
  increment a counter; the summary line at the end of each script prints processed /
  upserted / skipped counts. A script that finishes silent is a bug.

## Test conventions

Phase 2 built the fixtures in `tests/conftest.py` and `tests/fixtures/`. Phase 3 used
them to reach coverage floors. New code must keep those floors intact.

- **Every new service** gets unit tests using the `mock_iapd`, `mock_es`, `mock_smtp`,
  `mock_requests`, and `tmp_data_dir` fixtures. Services must never hit real external
  systems in tests.
- **Every new route** gets a route test covering auth, happy path, 404, and validation
  errors. `tests/test_external_api.py` is the reference for Bearer-token routes.
- **Every new Celery task** uses the `celery_eager` fixture so `.delay()` runs inline.
  Task tests assert on DB state after the task, not on Celery's return value.
- **External network calls are mocked via `responses`.** Never hit the live SEC API from
  a test. `mock_requests` is preconfigured with fixture ZIPs and metadata JSON — extend
  it before reaching for raw `responses.add`.
- **Time is frozen** via `frozen_time`/`@pytest.mark.freeze_time` for anything that
  touches `datetime.now()`, `date.today()`, or Celery ETAs. Real-clock tests are flaky.
- **Coverage floors are enforced in CI** (`coverage-check` job): services/celery ≥80%,
  routes ≥70%, scripts ≥60%. Landing a PR that drops any bucket below its floor requires
  raising the floor discussion first, not silently lowering it.

## What must be idempotent

Call this list out every time a new script, task, or migration is added — if it can't
meet idempotency, it needs a good reason in the commit message.

- All seed scripts (`seed_platforms`, `seed_schedules`, `seed_questionnaires`).
- The entrypoint's migrate + seed + ES-index block.
- ES index creation (`create_index_if_not_exists`).
- Bulk import scripts (`load_bulk_csv`, `load_filing_data`, `index_firms_to_es`,
  `backfill_annual_aum`).
- All Celery tasks that write to the database.
- Alembic migrations' data sections (`op.execute(...)` blocks that backfill) — wrap in
  `INSERT ... ON CONFLICT DO NOTHING` or equivalent.
- `make install`, `make verify`, `make migrate`, `make seed` — any operator-facing
  command that might be re-run when confused.

## Adding a New API Endpoint

1. Add business logic to the relevant `api/services/*.py` file
2. Add Pydantic schemas to `api/schemas/*.py`
3. Add the route to the relevant `api/routes/*.py` file
4. Register the router in `api/main.py` if it's a new file
5. Add a test in `tests/test_{resource}_api.py` covering: auth (if protected),
   happy path, 404, validation failure. Use `mock_*` fixtures from `tests/conftest.py`;
   do not hit external systems.
6. Run `make test` to confirm — CI's `coverage-check` will fail the PR if the routes
   bucket drops below 70%

## Adding a Database Column

1. Edit the SQLAlchemy model in `api/models/` — the column **must** use `Mapped[T]`
   annotation, otherwise `alembic revision --autogenerate` silently misses it
2. Generate a migration: `docker compose exec api alembic revision --autogenerate -m "description"`
3. Review the generated migration in `alembic/versions/` — always review before applying
4. Apply: `make migrate`
5. Update any affected Pydantic schemas and service functions
6. Run `pytest tests/test_migrations.py` — the schema-parity test fails if
   `Base.metadata.create_all` and `alembic upgrade head` diverge (the Phase 3 safety net
   for the `Mapped[T]` rule above)

---

## Key External URLs

| Resource | URL |
|----------|-----|
| IAPD firm search | `https://efts.sec.gov/LATEST/search-index?query=Info.FirmCrdNb:{crd}&forms=ADV` |
| IAPD name search | `https://efts.sec.gov/LATEST/search-index?query=Info.BusNm:"{name}"&forms=ADV` |
| IAPD brochure download | `https://files.adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={id}` |
| SEC reports metadata feed | `https://reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json` |
| Bulk CSV legacy (2000–2011) | `https://www.sec.gov/files/adv-filing-data-20001019-20111104.zip` |
| Bulk CSV part 1 (2011–2024) | `https://www.sec.gov/files/adv-filing-data-20111105-20241231-part1.zip` |
| Bulk CSV part 2 (2011–2024) | `https://www.sec.gov/files/adv-filing-data-20111105-20241231-part2.zip` |
| Monthly filing data (2025+) | `https://reports.adviserinfo.sec.gov/reports/foia/advFilingData/{YYYY}/{fileName}` |
| Monthly brochure ZIPs (2025+) | `https://reports.adviserinfo.sec.gov/reports/foia/advBrochures/{YYYY}/{fileName}` |
| Monthly ADV-W (2025+) | `https://reports.adviserinfo.sec.gov/reports/foia/advW/{YYYY}/{fileName}` |