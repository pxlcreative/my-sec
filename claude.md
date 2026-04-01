# CLAUDE.md — MySEC

This file tells Claude Code how this project is structured and what conventions to follow.
Read it before making any changes. Update it when conventions change.

---

## What This Project Does

Private, self-hosted database of ~21,000 SEC-registered investment adviser firms.
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
make seed        # insert default platforms
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
  celery_tasks/        # Celery tasks + Beat schedule (app.py, monthly_sync.py, refresh_tasks.py, export_tasks.py, match_tasks.py)
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
- Beat schedule is defined in `celery_tasks/app.py`
- Monthly PDF sync runs on the 2nd of each month at 06:00 UTC
- All long-running operations (bulk match >100 rows, exports >500 firms) run as Celery tasks
- Task results and status are stored in the `sync_jobs` or `export_jobs` DB tables, not in Celery result backend

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
- **March PDF ZIPs are huge.** The annual amendment season produces 8–10 ZIP parts.
  The URL discovery logic must handle part-numbered URLs:
  `adv-brochures-2025-03.zip`, `adv-brochures-2025-03-part1.zip` ... `part10.zip`
- **Mapping CSV is authoritative.** Never infer CRD number from PDF filename.
  Always use the mapping CSV bundled in each brochure ZIP.

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

### Empty State Requirements
Every data-fetching page must show a non-broken empty state before SEC data is loaded:
- **Search & Explore:** "No firms found" message with instructions to load data
- **Firm Detail:** 404 page with link back to search
- **Bulk Match:** Upload prompt (works before data load — matching simply returns no_match)
- **Platform Manager:** "No platforms yet" with create form visible
- **Alerts:** "No alert rules" with create button
- **Export Center:** Filter form renders; export with 0 results returns an empty file gracefully
- **Sync Dashboard:** Status cards show "Never synced" state; job history table is empty

---

## Adding a New API Endpoint

1. Add business logic to the relevant `api/services/*.py` file
2. Add Pydantic schemas to `api/schemas/*.py`
3. Add the route to the relevant `api/routes/*.py` file
4. Register the router in `api/main.py` if it's a new file
5. Add a test in `tests/test_{resource}_api.py`
6. Run `make test` to confirm

## Adding a Database Column

1. Edit the SQLAlchemy model in `api/models/`
2. Generate a migration: `docker compose exec api alembic revision --autogenerate -m "description"`
3. Review the generated migration in `alembic/versions/` — always review before applying
4. Apply: `make migrate`
5. Update any affected Pydantic schemas and service functions

---

## Key External URLs

| Resource | URL |
|----------|-----|
| IAPD firm search | `https://efts.sec.gov/LATEST/search-index?query=Info.FirmCrdNb:{crd}&forms=ADV` |
| IAPD name search | `https://efts.sec.gov/LATEST/search-index?query=Info.BusNm:"{name}"&forms=ADV` |
| IAPD brochure download | `https://files.adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={id}` |
| SEC FOIA bulk data page | `https://www.sec.gov/dera/data/investment-adviser-data` |
| Bulk CSV part 1 (2011–2024) | `https://www.sec.gov/files/adv-filing-data-20111105-20241231-part1.zip` |
| Bulk CSV part 2 (2011–2024) | `https://www.sec.gov/files/adv-filing-data-20111105-20241231-part2.zip` |
| Bulk CSV legacy (2000–2011) | `https://www.sec.gov/files/adv-filing-data-20001019-20111104.zip` |
| Monthly brochure ZIPs | `https://www.sec.gov/files/adv-brochures-{YYYY}-{MM}.zip` |