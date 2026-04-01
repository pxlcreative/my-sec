# MySEC — Full Architecture Spec

**Version 2.0** — Updated with requirements 1–8  
**Prepared for:** Claude Code  
**Stack:** Python (FastAPI) · PostgreSQL · Elasticsearch · Redis/Celery · React/TypeScript

---

## Table of Contents

1. [Overview & Goals](#1-overview--goals)
2. [Data Sources & APIs](#2-data-sources--apis)
3. [Database Schema](#3-database-schema)
4. [Module A: Bulk Historical Load](#module-a-bulk-historical-load)
5. [Module B: Monthly Incremental Sync](#module-b-monthly-incremental-sync)
6. [Module C: Historical AUM Population (2023–2024)](#module-c-historical-aum-population-20232024)
7. [Module D: Bulk Name+Address → CRD Matching](#module-d-bulk-nameaddress--crd-matching)
8. [Module E: CRD Export with Platform Tagging](#module-e-crd-export-with-platform-tagging)
9. [Module F: External ADV Part-2 PDF API](#module-f-external-adv-part-2-pdf-api)
10. [Module G: Deregistration & AUM Decline Alerts](#module-g-deregistration--aum-decline-alerts)
11. [Module H: On-Demand Due Diligence Excel Generator](#module-h-on-demand-due-diligence-excel-generator)
12. [REST API Spec](#12-rest-api-spec)
13. [Frontend Dashboard](#13-frontend-dashboard)
14. [Build Order for Claude Code](#14-build-order-for-claude-code)
15. [Key Engineering Decisions & Gotchas](#15-key-engineering-decisions--gotchas)

---

## 1. Overview & Goals

Build a private, self-hosted database of SEC-registered investment adviser firms sourced entirely from public SEC data. The platform must:

- Mirror the full IAPD/ADV dataset locally and keep it current monthly
- Store and serve ADV Part 2 brochure PDFs by CRD
- Reconstruct historical AUM time series from 2023–2024 bulk CSV data
- Support fuzzy bulk matching of external name+address lists to CRD numbers
- Expose a machine-readable API for external systems (PDF retrieval, bulk CRD lookup)
- Fire alerts on deregistration or AUM decline events, scoped to platform tags
- Generate pre-populated due diligence Excel workbooks on demand

---

## 2. Data Sources & APIs

### 2a. Primary Public Data Sources

| Source | Format | Coverage | URL |
|--------|--------|----------|-----|
| IAPD live search | JSON (unofficial REST) | Current filings, all firms | `https://efts.sec.gov/LATEST/search-index` |
| ADV Part 1 bulk CSV | ZIP of multiple CSVs | Nov 2011–Dec 2024 (two ZIPs) | sec.gov/foia Form ADV Data page |
| ADV Part 1 (legacy) | ZIP of CSVs | Oct 2000–Nov 2011 | sec.gov/foia Form ADV Data page |
| ADV Part 2 brochures | ZIP of PDFs + mapping CSV | Monthly, 2020–present | sec.gov/foia Form ADV Data page |
| ADV Part 3 (CRS) | ZIP of PDFs | Monthly, 2020–present | sec.gov/foia Form ADV Data page |
| ADV-W (withdrawals) | ZIP of CSVs | Deregistration history | sec.gov/foia Form ADV Data page |
| IAPD brochure download | PDF per version ID | Per-firm, per-filing | `https://files.adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={id}` |

### 2b. Key IAPD API Patterns (captured via browser devtools)

**Firm search:**
```
GET https://efts.sec.gov/LATEST/search-index?query=Info.FirmCrdNb:{crd}&forms=ADV
```

**Firm search by name:**
```
GET https://efts.sec.gov/LATEST/search-index?query=Info.BusNm:"{name}"&forms=ADV&from=0&size=10
```

**Brochure list for a CRD:**
```
GET https://files.adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={id}
```

The IAPD search returns structured JSON with the full ADV filing. AUM is in:
`FormInfo.Part1A.Item5F.Q5F2C` (total regulatory AUM, in USD)

### 2c. ADV Part 1 CSV Table Structure

The bulk CSV ZIPs contain multiple relational tables linked by `CRD_NUMBER`:

- `IA_MAIN.csv` — primary firm record (name, AUM Item 5F, employees, registration status, address)
- `IA_OTHRREGULATOR.csv` — other regulatory registrations
- `IA_SCHEDULE_D_*.csv` — owners, affiliations, private funds
- `DRP_*.csv` — disciplinary records (criminal, regulatory, civil, etc.)
- `IA_WRAP.csv` — wrap fee programs

The `FILING_DATE` column on `IA_MAIN` is critical for reconstructing the historical AUM time series — each row is one filing snapshot.

---

## 3. Database Schema

```sql
-- ============================================================
-- CORE FIRM DATA
-- ============================================================

CREATE TABLE firms (
  crd_number          INTEGER PRIMARY KEY,
  sec_number          TEXT,                    -- 801-XXXXX
  legal_name          TEXT NOT NULL,
  business_name       TEXT,
  registration_status TEXT,                    -- 'Registered', 'ERA', 'Withdrawn'
  firm_type           TEXT,
  aum_total           BIGINT,                  -- Item 5F(2)(c), current
  aum_discretionary   BIGINT,                  -- Item 5F(2)(a)
  aum_non_discretionary BIGINT,               -- Item 5F(2)(b)
  num_accounts        INTEGER,
  num_employees       INTEGER,
  main_street1        TEXT,
  main_street2        TEXT,
  main_city           TEXT,
  main_state          TEXT,
  main_zip            TEXT,
  main_country        TEXT,
  phone               TEXT,
  website             TEXT,
  fiscal_year_end     TEXT,                    -- month name
  org_type            TEXT,                    -- LLC, Corp, LP, etc.
  raw_adv             JSONB,                   -- full IAPD JSON response
  last_filing_date    DATE,
  created_at          TIMESTAMPTZ DEFAULT NOW(),
  updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_firms_legal_name ON firms USING gin(to_tsvector('english', legal_name));
CREATE INDEX idx_firms_business_name ON firms USING gin(to_tsvector('english', coalesce(business_name, '')));
CREATE INDEX idx_firms_state ON firms (main_state);
CREATE INDEX idx_firms_status ON firms (registration_status);

-- ============================================================
-- HISTORICAL AUM TIME SERIES
-- ============================================================

CREATE TABLE firm_aum_history (
  id              BIGSERIAL PRIMARY KEY,
  crd_number      INTEGER NOT NULL REFERENCES firms(crd_number),
  filing_date     DATE NOT NULL,
  aum_total       BIGINT,
  aum_discretionary BIGINT,
  aum_non_discretionary BIGINT,
  num_accounts    INTEGER,
  source          TEXT NOT NULL,              -- 'bulk_csv_2011_2024', 'iapd_live', 'monthly_sync'
  UNIQUE (crd_number, filing_date, source)
);

CREATE INDEX idx_aum_history_crd ON firm_aum_history(crd_number);
CREATE INDEX idx_aum_history_date ON firm_aum_history(filing_date);

-- ============================================================
-- CHANGE SNAPSHOTS & DIFF LOG
-- ============================================================

CREATE TABLE firm_snapshots (
  id              BIGSERIAL PRIMARY KEY,
  crd_number      INTEGER NOT NULL REFERENCES firms(crd_number),
  snapshot_hash   TEXT NOT NULL,              -- SHA-256 of canonical JSON
  raw_json        JSONB NOT NULL,
  synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_snapshots_crd ON firm_snapshots(crd_number, synced_at DESC);

CREATE TABLE firm_changes (
  id              BIGSERIAL PRIMARY KEY,
  crd_number      INTEGER NOT NULL REFERENCES firms(crd_number),
  field_path      TEXT NOT NULL,              -- e.g. 'registration_status', 'aum_total'
  old_value       TEXT,
  new_value       TEXT,
  detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  snapshot_from   BIGINT REFERENCES firm_snapshots(id),
  snapshot_to     BIGINT REFERENCES firm_snapshots(id)
);

CREATE INDEX idx_changes_crd ON firm_changes(crd_number, detected_at DESC);
CREATE INDEX idx_changes_field ON firm_changes(field_path);

-- ============================================================
-- ADV PART 2 PDF STORE
-- ============================================================

CREATE TABLE adv_brochures (
  id              BIGSERIAL PRIMARY KEY,
  crd_number      INTEGER NOT NULL REFERENCES firms(crd_number),
  brochure_version_id  INTEGER NOT NULL UNIQUE,    -- BRCHR_VRSN_ID from IAPD
  brochure_name   TEXT,
  date_submitted  DATE,
  source_month    TEXT,                       -- e.g. '2025-03' (which bulk ZIP)
  file_path       TEXT NOT NULL,              -- local path: /data/brochures/{crd}/{version_id}.pdf
  file_size_bytes INTEGER,
  downloaded_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_brochures_crd ON adv_brochures(crd_number, date_submitted DESC);

-- ============================================================
-- PLATFORM TAGS (CUSTOM PROPERTIES)
-- ============================================================

CREATE TABLE platform_definitions (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,           -- e.g. 'Orion', 'Envestnet', 'Schwab'
  description TEXT,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE firm_platforms (
  id              BIGSERIAL PRIMARY KEY,
  crd_number      INTEGER NOT NULL REFERENCES firms(crd_number),
  platform_id     INTEGER NOT NULL REFERENCES platform_definitions(id),
  tagged_at       TIMESTAMPTZ DEFAULT NOW(),
  tagged_by       TEXT,
  notes           TEXT,
  UNIQUE (crd_number, platform_id)
);

CREATE INDEX idx_firm_platforms_crd ON firm_platforms(crd_number);
CREATE INDEX idx_firm_platforms_platform ON firm_platforms(platform_id);

-- ============================================================
-- GENERIC CUSTOM PROPERTIES (beyond platform)
-- ============================================================

CREATE TABLE custom_property_definitions (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  field_type  TEXT NOT NULL,                  -- 'text', 'number', 'date', 'boolean', 'select', 'multiselect'
  options     JSONB,                          -- for select/multiselect: ["opt1","opt2"]
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE firm_custom_properties (
  id              BIGSERIAL PRIMARY KEY,
  crd_number      INTEGER NOT NULL REFERENCES firms(crd_number),
  definition_id   INTEGER NOT NULL REFERENCES custom_property_definitions(id),
  value           TEXT,
  updated_at      TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (crd_number, definition_id)
);

-- ============================================================
-- ALERT RULES & EVENTS
-- ============================================================

CREATE TABLE alert_rules (
  id              SERIAL PRIMARY KEY,
  label           TEXT NOT NULL,
  rule_type       TEXT NOT NULL,              -- 'deregistration', 'aum_decline_pct', 'field_change'
  platform_ids    INTEGER[],                  -- scope by platform; NULL = all firms
  crd_numbers     INTEGER[],                  -- scope by specific CRDs; NULL = all
  threshold_pct   NUMERIC(5,2),               -- for aum_decline_pct rules
  field_path      TEXT,                       -- for field_change rules
  delivery        TEXT NOT NULL DEFAULT 'in_app',  -- 'in_app', 'email', 'webhook'
  delivery_target TEXT,                       -- email address or webhook URL
  active          BOOLEAN NOT NULL DEFAULT TRUE,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE alert_events (
  id              BIGSERIAL PRIMARY KEY,
  rule_id         INTEGER NOT NULL REFERENCES alert_rules(id),
  crd_number      INTEGER NOT NULL,
  firm_name       TEXT,
  rule_type       TEXT NOT NULL,
  field_path      TEXT,
  old_value       TEXT,
  new_value       TEXT,
  platform_name   TEXT,
  fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  delivered_at    TIMESTAMPTZ,
  delivery_status TEXT DEFAULT 'pending'      -- 'pending', 'sent', 'failed'
);

CREATE INDEX idx_alert_events_rule ON alert_events(rule_id, fired_at DESC);
CREATE INDEX idx_alert_events_crd ON alert_events(crd_number, fired_at DESC);

-- ============================================================
-- SYNC JOB TRACKING
-- ============================================================

CREATE TABLE sync_jobs (
  id              BIGSERIAL PRIMARY KEY,
  job_type        TEXT NOT NULL,              -- 'bulk_csv', 'monthly_pdf', 'live_incremental', 'aum_history'
  status          TEXT NOT NULL DEFAULT 'pending', -- 'pending','running','complete','failed'
  source_url      TEXT,
  firms_processed INTEGER DEFAULT 0,
  firms_updated   INTEGER DEFAULT 0,
  changes_detected INTEGER DEFAULT 0,
  error_message   TEXT,
  started_at      TIMESTAMPTZ,
  completed_at    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- ASYNC EXPORT JOBS
-- ============================================================

CREATE TABLE export_jobs (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  format          TEXT NOT NULL,              -- 'csv', 'json', 'xlsx'
  filter_criteria JSONB,
  crd_list        INTEGER[],
  field_selection JSONB,
  status          TEXT NOT NULL DEFAULT 'pending',
  file_path       TEXT,
  row_count       INTEGER,
  error_message   TEXT,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  completed_at    TIMESTAMPTZ,
  expires_at      TIMESTAMPTZ
);
```

---

## Module A: Bulk Historical Load

**Goal:** Populate the `firms` and `firm_aum_history` tables from the SEC's historical CSV ZIP files, covering Oct 2000–Dec 2024.

### Steps

1. Download the three historical ZIP files from sec.gov/foia:
   - `adv-filing-data-20001019-20111104.zip`
   - `adv-filing-data-20111105-20241231-part1.zip`
   - `adv-filing-data-20111105-20241231-part2.zip`

2. Parse `IA_MAIN.csv` from each ZIP. Key columns to extract:
   ```
   CRD_NUMBER, FIRM_NAME, ADV_FILING_DATE,
   ITEM5F_2A (AUM discretionary),
   ITEM5F_2B (AUM non-discretionary),
   ITEM5F_2C (AUM total),
   ITEM5A_TOTAL_EMPLOYEES,
   ITEM1A_LEGAL_NAME, ITEM1F_ADDRESS, ITEM1F_CITY, ITEM1F_STATE, ITEM1F_ZIP,
   REGISTRATION_STATUS
   ```

3. Dedup and upsert into `firms` using the most recent `ADV_FILING_DATE` row per CRD as the "current" record.

4. Insert all rows (one per filing date per CRD) into `firm_aum_history` with `source = 'bulk_csv_2011_2024'`. This gives you a full AUM time series automatically.

5. Run `IA_MAIN.csv` join with disciplinary tables (`DRP_Criminal`, `DRP_Regulatory`, etc.) and store the counts per CRD in a `firm_disclosures_summary` table (optional but useful for due diligence Excel).

### Script: `scripts/load_bulk_csv.py`

```python
import zipfile, csv, hashlib, psycopg2
from pathlib import Path

ZIP_URLS = [
    "https://www.sec.gov/files/adv-filing-data-20001019-20111104.zip",
    "https://www.sec.gov/files/adv-filing-data-20111105-20241231-part1.zip",
    "https://www.sec.gov/files/adv-filing-data-20111105-20241231-part2.zip",
]
DOWNLOAD_DIR = Path("data/raw/csv")

def download_and_extract(url, dest_dir):
    # Use requests with streaming, save to dest_dir, unzip
    ...

def load_ia_main(csv_path, conn, source_tag):
    # Batch upsert into firms + firm_aum_history
    ...
```

---

## Module B: Monthly Incremental Sync

**Goal:** Each month, when new ADV Part 2 PDF ZIPs are published, automatically download them, extract PDFs, and update firm records via the live IAPD API.

### B1. Monthly PDF Download

The SEC publishes new Part 2 brochure ZIPs each month at predictable URLs:

```
https://www.sec.gov/files/adv-brochures-{YYYY}-{month}.zip
https://www.sec.gov/files/adv-brochures-{YYYY}-{month}-part1.zip  (large months)
```

A companion mapping CSV lists `CRD_NUMBER`, `BRCHR_VRSN_ID`, `BROCHURE_NAME`, `SUBMIT_DATE`.

**Scheduler task (runs 1st of each month):**

```python
# In celery_tasks/monthly_sync.py
@app.task
def monthly_pdf_sync():
    month_str = get_previous_month_str()          # e.g. "2025-02"
    zip_urls  = discover_month_zip_urls(month_str) # check sec.gov for part1/part2/etc.
    
    for url in zip_urls:
        zip_path = download_file(url, DOWNLOAD_DIR)
        mapping  = load_mapping_csv(zip_path)
        
        for row in mapping:
            crd, version_id, name, date = row
            if not brochure_already_stored(version_id):
                pdf_bytes = extract_pdf_from_zip(zip_path, version_id)
                local_path = store_pdf(crd, version_id, pdf_bytes)
                insert_brochure_record(crd, version_id, name, date, 
                                       month_str, local_path)
```

**PDF storage layout:**
```
/data/brochures/
  {crd}/
    {version_id}_{YYYYMMDD}.pdf
```

### B2. Firm Data Refresh via IAPD API

After PDFs are stored, refresh firm metadata for any CRD that had a new brochure:

```python
@app.task
def refresh_firm_from_iapd(crd_number: int):
    url = f"https://efts.sec.gov/LATEST/search-index"
    params = {"query": f"Info.FirmCrdNb:{crd_number}", "forms": "ADV"}
    data = requests.get(url, params=params, headers=HEADERS).json()
    
    filing = data["hits"]["hits"][0]["_source"]
    old_hash = get_current_snapshot_hash(crd_number)
    new_hash = sha256_canonical(filing)
    
    if old_hash != new_hash:
        save_snapshot(crd_number, new_hash, filing)
        diffs = compute_diffs(crd_number, filing)
        save_changes(crd_number, diffs)
        update_firm_record(crd_number, filing)
        evaluate_alert_rules(crd_number, diffs)
```

### B3. Monitoring for New Monthly Publications

Check sec.gov/foia monthly for new ZIP links — the page structure is stable. Build a scraper that reads the HTML and detects new entries not yet in `sync_jobs`.

---

## Module C: Historical AUM Population (2023–2024)

**Goal:** Populate `firm_aum_history` with year-specific AUM figures for 2023 and 2024.

### Source

Historical ADV filing data is available in CSV format covering November 2011 to December 2024. The `IA_MAIN.csv` file in the bulk ZIP contains a row per filing per firm, with `ADV_FILING_DATE`. Filtering for 2023-01-01 through 2024-12-31 gives you every firm's AUM at each filing date within that window.

### Steps

1. Run the bulk CSV importer (Module A) — it automatically populates `firm_aum_history` for all years including 2023–2024.

2. For 2023–2024 specifically, also fetch the IAPD live API for each firm to get the most precise current and most-recent-annual values (since some advisers' annual amendment filings update AUM more precisely than the bulk CSV).

3. Build a helper view for reporting:

```sql
CREATE VIEW firm_aum_annual AS
SELECT
  crd_number,
  EXTRACT(YEAR FROM filing_date) AS year,
  MAX(aum_total)                 AS peak_aum,
  MIN(aum_total)                 AS trough_aum,
  (ARRAY_AGG(aum_total ORDER BY filing_date DESC))[1] AS latest_aum_for_year,
  COUNT(*)                       AS filing_count
FROM firm_aum_history
GROUP BY crd_number, EXTRACT(YEAR FROM filing_date);
```

4. In the `firms` table, add convenience columns `aum_2023` and `aum_2024` populated by a backfill script that pulls from this view post-load.

---

## Module D: Bulk Name+Address → CRD Matching

**Goal:** Accept a list of `{name, address}` pairs, fuzzy-match against the database, and return `{name, address, crd, match_score, match_status}`.

### Implementation

Use a two-stage matching pipeline:

#### Stage 1 — Elasticsearch Fuzzy Name Search

Index all firms into Elasticsearch with:

```json
{
  "crd_number": 12345,
  "legal_name": "Acme Capital Management LLC",
  "business_name": "Acme Capital",
  "main_city": "New York",
  "main_state": "NY",
  "main_zip": "10001",
  "main_street1": "100 Park Ave",
  "registration_status": "Registered"
}
```

Query with multi-match + fuzzy:

```json
{
  "query": {
    "bool": {
      "must": [
        {"match": {"legal_name": {"query": "{{input_name}}", "fuzziness": "AUTO", "boost": 3}}},
        {"match": {"business_name": {"query": "{{input_name}}", "fuzziness": "AUTO", "boost": 2}}}
      ],
      "should": [
        {"match": {"main_city": "{{input_city}}"}},
        {"term":  {"main_state": "{{input_state}}"}}
      ]
    }
  }
}
```

#### Stage 2 — Post-Score Refinement in Python

Use `rapidfuzz` to compute `token_sort_ratio` between the input name and each ES candidate's name. Combine with address component overlap (city, state, zip) into a weighted score:

```python
def compute_match_score(input_row, candidate):
    name_score  = token_sort_ratio(input_row.name, candidate.legal_name)
    city_score  = 100 if input_row.city.lower() == candidate.city.lower() else 0
    state_score = 100 if input_row.state.upper() == candidate.state.upper() else 0
    zip_score   = 100 if input_row.zip[:5] == candidate.zip[:5] else 0
    return (name_score * 0.60 + city_score * 0.15 + 
            state_score * 0.15 + zip_score * 0.10)
```

#### Stage 3 — Match Status Classification

| Score | Status |
|-------|--------|
| ≥ 90 | `confirmed` |
| 70–89 | `probable` — flag for manual review |
| 50–69 | `possible` — flag for manual review |
| < 50 | `no_match` |

### API Endpoint: `POST /api/match/bulk`

**Request:**
```json
{
  "records": [
    {"id": "row_1", "name": "Acme Capital Mgmt", "address": "100 Park Ave", "city": "New York", "state": "NY", "zip": "10001"},
    ...
  ],
  "options": {
    "min_score": 50,
    "max_candidates_per_record": 3
  }
}
```

**Response:**
```json
{
  "results": [
    {
      "id": "row_1",
      "input": {...},
      "matches": [
        {
          "crd_number": 12345,
          "legal_name": "ACME CAPITAL MANAGEMENT LLC",
          "main_city": "NEW YORK",
          "main_state": "NY",
          "registration_status": "Registered",
          "match_score": 94.2,
          "match_status": "confirmed"
        }
      ]
    }
  ],
  "stats": {
    "total": 100,
    "confirmed": 78,
    "probable": 12,
    "possible": 5,
    "no_match": 5
  }
}
```

Input size up to 10,000 rows. For >500 rows, make async with a job ID.

---

## Module E: CRD Export with Platform Tagging

**Goal:** From a bulk match result or search, export `{name, address, CRD, platform}` in standard formats, and allow platform tags to be set in bulk.

### E1. Platform Tagging

**Single firm:**
```
PUT /api/firms/{crd}/platforms
Body: {"platform_ids": [1, 3], "notes": "confirmed via DDQ"}
```

**Bulk tagging from match results:**
```
POST /api/match/bulk-tag
Body: {
  "records": [
    {"crd_number": 12345, "platform_id": 2},
    {"crd_number": 67890, "platform_id": 2}
  ]
}
```

### E2. Export Endpoint

```
POST /api/export/crd-list
Body: {
  "filter": {
    "platform_ids": [2],
    "match_status": "confirmed",
    "registration_status": "Registered"
  },
  "fields": ["crd_number", "legal_name", "main_address", "main_city", "main_state", "platforms", "aum_total"],
  "format": "csv"   // or "xlsx", "json"
}
```

**CSV output columns:**
```
CRD_NUMBER, LEGAL_NAME, BUSINESS_NAME, STREET, CITY, STATE, ZIP, 
AUM_TOTAL, REGISTRATION_STATUS, PLATFORMS, LAST_FILING_DATE
```

For Excel output, add a second sheet with the full platform tag list and tagging history.

---

## Module F: External ADV Part-2 PDF API

**Goal:** Allow an external system to query this private repo and retrieve the latest (or a specific) ADV Part 2 PDF for any CRD number.

### Authentication

Issue static API keys stored in an `api_keys` table. External systems pass:
```
Authorization: Bearer {api_key}
```

### Endpoints

```
# Get latest brochure PDF for a CRD
GET /api/external/firms/{crd}/brochure
Response: application/pdf (binary stream)
          Headers: Content-Disposition: attachment; filename="ADV_Part2_{crd}_{date}.pdf"

# Get list of all available brochures for a CRD
GET /api/external/firms/{crd}/brochures
Response: JSON array of {version_id, name, date_submitted, download_url}

# Get a specific brochure by version ID
GET /api/external/brochures/{version_id}
Response: application/pdf

# Bulk brochure manifest — list latest brochure per CRD for a platform
GET /api/external/platforms/{platform_id}/brochures
Response: JSON array of {crd_number, firm_name, version_id, date_submitted, download_url}
```

### Fallback: Live Passthrough

If the PDF is not stored locally (e.g., a CRD not yet synced), optionally proxy the download directly from `files.adviserinfo.sec.gov`:

```python
BROCHURE_URL = "https://files.adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={}"

def get_brochure_pdf(version_id: int) -> bytes:
    local = db.get_brochure_path(version_id)
    if local and Path(local).exists():
        return Path(local).read_bytes()
    # Live fallback
    return requests.get(BROCHURE_URL.format(version_id)).content
```

---

## Module G: Deregistration & AUM Decline Alerts

**Goal:** After each monthly sync, evaluate all active alert rules and fire notifications for firms that have deregistered or seen AUM decline beyond a configured threshold — scoped to specific platforms.

### Alert Rule Types

**Type 1: Deregistration**
- Condition: `registration_status` changed from `Registered` → `Withdrawn`
- Scope: all firms tagged to `platform_ids` in the rule

**Type 2: AUM Decline Percentage**
- Condition: latest `aum_total` is < previous `aum_total` × (1 - `threshold_pct` / 100)
- Compare: current filing AUM vs. prior year's AUM (from `firm_aum_history`)
- Scope: all firms tagged to `platform_ids` in the rule

### Alert Evaluation (runs after each sync cycle)

```python
def evaluate_alerts_for_firm(crd: int, changes: list[dict]):
    firm = db.get_firm(crd)
    platform_ids = db.get_firm_platform_ids(crd)
    active_rules = db.get_active_rules_for_platforms(platform_ids)
    
    for rule in active_rules:
        if rule.rule_type == "deregistration":
            if any(c["field_path"] == "registration_status" and
                   c["new_value"] == "Withdrawn" for c in changes):
                fire_alert(rule, firm, changes)
        
        elif rule.rule_type == "aum_decline_pct":
            prev_aum = db.get_prior_year_aum(crd)
            curr_aum = firm.aum_total
            if prev_aum and curr_aum:
                pct_change = (curr_aum - prev_aum) / prev_aum * 100
                if pct_change <= -rule.threshold_pct:
                    fire_alert(rule, firm, changes, extra={
                        "prior_aum": prev_aum,
                        "current_aum": curr_aum,
                        "pct_change": round(pct_change, 2)
                    })
```

### Alert Delivery

**In-app:** Insert to `alert_events`, surfaced in dashboard notification center.

**Email:** Use SMTP/SendGrid.
```
Subject: [SEC Alert] {firm_name} — {rule_type} detected
Body:    Firm: {firm_name} (CRD: {crd})
         Platform: {platform}
         Change: {description}
         Prior AUM: ${prior_aum:,.0f}
         Current AUM: ${current_aum:,.0f}
         Change: {pct_change}%
         Filing date: {last_filing_date}
```

**Webhook:** POST JSON to configured URL:
```json
{
  "event_type": "aum_decline",
  "crd_number": 12345,
  "firm_name": "Acme Capital Management",
  "platform": "Orion",
  "prior_aum": 500000000,
  "current_aum": 380000000,
  "pct_change": -24.0,
  "threshold_pct": 20.0,
  "fired_at": "2025-03-01T08:00:00Z"
}
```

---

## Module H: On-Demand Due Diligence Excel Generator

**Goal:** For any firm in the database, generate a structured Excel workbook pre-populated with known ADV data alongside standard due diligence questions.

### Workbook Structure (one file per firm)

**Sheet 1: Cover**

| Field | Value |
|-------|-------|
| Firm Legal Name | `{legal_name}` |
| CRD Number | `{crd_number}` |
| SEC Number | `{sec_number}` |
| Report Generated | `{today}` |
| Data as of | `{last_filing_date}` |
| Registration Status | `{registration_status}` |

**Sheet 2: Firm Overview (ADV Part 1 — Pre-filled)**

Sections: Basic Information, Business Operations, AUM History (sparkline if possible), Client Types (from Item 5D), Compensation Arrangements (Item 5E), Investment Strategies (Item 8), Custody (Item 9), Financial Industry Affiliations (Item 7).

**Sheet 3: Due Diligence Questionnaire**

Each row = one DD question with three columns: Question, Known Answer (from ADV), Analyst Notes (blank, editable).

| # | Due Diligence Question | ADV Source Field | Pre-populated Answer |
|---|------------------------|------------------|----------------------|
| 1 | What is the firm's total AUM? | `Item5F.Q5F2C` | `$XXX,XXX,XXX` |
| 2 | How many client accounts does the firm serve? | `Item5F.Q5F2F` | `XXX` |
| 3 | What is the firm's primary business name? | `Item1.BusNm` | `...` |
| 4 | Is the firm currently registered with the SEC? | `registration_status` | `Registered` |
| 5 | Does the firm have disciplinary disclosures? | DRP records | `Yes/No + count` |
| 6 | What types of clients does the firm serve? | `Item5D` | `Individuals, Institutions...` |
| 7 | What compensation arrangements does the firm use? | `Item5E` | `Percentage of AUM...` |
| 8 | Does the firm have custody of client assets? | `Item9` | `Yes/No` |
| 9 | What is the firm's year of most recent ADV filing? | `last_filing_date` | `YYYY` |
| 10 | Does the firm have private fund clients? | `Item7B` | `Yes/No` |
| … | *(expand to 40–60 standard questions)* | … | … |

**Sheet 4: AUM History**

Table of `{year, aum_total, num_accounts, filing_date}` for available history (2023, 2024, current). Include a chart if openpyxl chart support is feasible.

**Sheet 5: Disclosures Summary**

Pre-populated table of any DRP records from the database.

**Sheet 6: Notes**

Free-form analyst notes area.

### Implementation

```python
# api/routes/excel_generator.py
import openpyxl
from openpyxl.styles import Font, Alignment, PatternFill, Border

@router.get("/api/firms/{crd}/due-diligence-excel")
def generate_dd_excel(crd: int):
    firm = db.get_firm_full(crd)
    aum_history = db.get_aum_history(crd, years=[2023, 2024])
    disclosures = db.get_disclosures(crd)
    
    wb = build_dd_workbook(firm, aum_history, disclosures)
    
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    
    filename = f"DDQ_{firm.crd_number}_{firm.legal_name[:30].replace(' ','_')}_{today()}.xlsx"
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})
```

Use `openpyxl` for full control over formatting. Style the workbook with:

- Header rows in dark blue with white bold text
- Pre-filled "known" cells in light blue
- Empty analyst input cells in light yellow with thin border
- Firm name prominently on the cover sheet
- Column widths auto-sized to content

---

## 12. REST API Spec

Full OpenAPI 3.0 spec auto-generated by FastAPI. Key endpoint groups:

### Firms

```
GET    /api/firms                          Paginated list with filters
GET    /api/firms/{crd}                    Full firm detail
GET    /api/firms/{crd}/history            Snapshot & change log
GET    /api/firms/{crd}/aum-history        AUM time series
GET    /api/firms/{crd}/brochures          Available Part 2 PDFs
GET    /api/firms/{crd}/brochure           Latest Part 2 PDF (binary)
GET    /api/firms/{crd}/due-diligence-excel DDQ Excel workbook
GET    /api/firms/search?q=               Full-text search
```

### Bulk Match

```
POST   /api/match/bulk                     Name+address → CRD (sync ≤100 rows, async otherwise)
GET    /api/match/jobs/{job_id}            Poll async match job
POST   /api/match/bulk-tag                 Tag all matched firms with platform
```

### Platform Tags

```
GET    /api/platforms                      List all platform definitions
POST   /api/platforms                      Create a platform
GET    /api/firms/{crd}/platforms          Get firm's platform tags
PUT    /api/firms/{crd}/platforms          Set platform tags (replaces)
DELETE /api/firms/{crd}/platforms/{id}     Remove one platform tag
GET    /api/platforms/{id}/firms           List all firms on a platform
```

### Export

```
POST   /api/export/firms                   Bulk export (async for >500 firms)
GET    /api/export/jobs/{id}               Poll job status
GET    /api/export/jobs/{id}/download      Download completed export file
POST   /api/export/templates               Save export preset
GET    /api/export/templates               List saved presets
```

### Alerts

```
GET    /api/alerts/rules                   List rules
POST   /api/alerts/rules                   Create rule
PUT    /api/alerts/rules/{id}              Update rule
DELETE /api/alerts/rules/{id}              Delete rule
GET    /api/alerts/events                  Recent alert events (filterable)
```

### External API (requires Bearer token)

```
GET    /api/external/firms/{crd}/brochure  Latest Part 2 PDF
GET    /api/external/firms/{crd}/brochures Brochure list
GET    /api/external/brochures/{version_id} Specific brochure PDF
GET    /api/external/platforms/{id}/firms  Firm list for a platform
GET    /api/external/platforms/{id}/brochures Latest brochure per firm on platform
POST   /api/external/match/bulk            Bulk CRD lookup (external auth)
```

### Sync Admin

```
GET    /api/sync/status                    Last sync summary
POST   /api/sync/trigger                   Manual trigger
GET    /api/sync/jobs                      Job history
```

---

## 13. Frontend Dashboard

### Pages

**1. Search & Explore**
Search bar (firm name, CRD, address), filters (state, AUM range, status, platform), sortable results table. Save/share search URLs. Click row → Firm Detail.

**2. Firm Detail**
Tabs: Overview, ADV Data, AUM History (chart), Brochures (download list), Disclosures, Platform Tags (editable), Changes History, Custom Properties. Action buttons: Download Excel DDQ, Download Latest Brochure.

**3. Bulk Match Tool**
Upload CSV (name + address columns), configure options (min score, columns), run match, review results table with match scores, approve/reject matches, tag with platform, export results.

**4. Platform Manager**
Create platforms, view all firms per platform, bulk tag/untag, export firm list by platform.

**5. Alerts**
Create/edit/delete alert rules, event feed with platform+firm context, configure delivery (email/webhook), test delivery.

**6. Export Center**
Build custom exports: pick firms (search/filter/platform), pick fields, choose format (CSV/JSON/XLSX), save as named preset, download or poll async job.

**7. Sync Dashboard**
Last sync time, next scheduled sync, job history log, manual trigger button, PDF storage stats (count, disk usage), change velocity chart.

---

## 14. Build Order for Claude Code

Build in this sequence to minimize rework:

1. Docker Compose setup — Postgres, Elasticsearch, Redis, FastAPI app, Celery worker
2. Alembic migrations — all tables from Section 3
3. Bulk CSV importer (Module A) — get firms + AUM history populated
4. Core FastAPI app — firm CRUD, search endpoints
5. Elasticsearch integration — index firms, wire full-text search + fuzzy
6. Bulk name+address matching (Module D) — ES + rapidfuzz pipeline
7. Platform tagging (Module E) — CRUD + bulk tag endpoint
8. Export engine (Module E2) — CSV/JSON/XLSX with async jobs
9. Monthly PDF sync scheduler (Module B) — Celery beat task
10. IAPD live refresh + change detection (Module B2)
11. Historical AUM population (Module C) — view + backfill script
12. Alert rules + evaluation + delivery (Module G)
13. Due diligence Excel generator (Module H) — openpyxl workbook
14. External API with auth (Module F) — Bearer token, PDF endpoints
15. React frontend — all pages from Section 13
16. OpenAPI spec — auto from FastAPI, add to `/docs`

---

## 15. Key Engineering Decisions & Gotchas

### Data

- **IAPD AUM field mapping:** AUM is in `FormInfo.Part1A.Item5F.Q5F2C` (total). Field `Q5F2A` = discretionary, `Q5F2B` = non-discretionary. These are in USD dollars, not millions.
- **Bulk CSV column names vary** between the 2000–2011 and 2011–2024 ZIPs. Write a normalizer that maps both column naming conventions to canonical names.
- **One firm, many filings:** `IA_MAIN.csv` has multiple rows per CRD — one per ADV amendment. Always use `FILING_DATE DESC` to get the current record.
- **AUM is self-reported and lags** — advisers file annually within 90 days of fiscal year end. The "current" AUM in IAPD may be up to ~15 months old.

### Search & Matching

- **Normalize firm names** before indexing and before matching: remove LLC/LP/Inc/Corp suffixes, lowercase, strip punctuation. Store both raw and normalized forms.
- **State abbreviations:** Standardize all to 2-letter codes. Input data from external systems may use full state names.
- **Zip matching:** Use only the first 5 digits; ZIP+4 is inconsistent.

### PDFs

- **Large month ZIPs** (March annual filing season) come in 8–10 parts. The downloader must detect part-numbering in URLs and download all parts.
- **Mapping CSV** links version IDs to CRDs — always use the mapping CSV, don't try to infer CRD from PDF filename.
- **Storage:** With 21,000+ firms and monthly PDFs, budget ~5–10 GB per year for storage. Consider a file size cap and a "latest only" retention policy to start.

### Performance

- **Elasticsearch must be the search layer** — Postgres full-text is insufficient for fuzzy name matching at scale. Keep ES and Postgres in sync via the ORM layer.
- **Async exports** for any request exceeding 500 firms. Store outputs in `/data/exports/` with a 24-hour TTL.
- **IAPD rate limiting:** Use a 0.5s sleep between firm fetch requests, retry with exponential backoff on 429/503. Log all rate limit events.

### Excel Generator

- Use `openpyxl` not `xlwt` (`xlwt` only supports `.xls`). Pin version `openpyxl>=3.1`.
- Pre-fill cells using named styles so formatting is consistent across all workbooks.
- Lock pre-filled cells and unlock analyst input cells (set `sheet.protection.sheet = True` with a password, using `cell.protection = Protection(locked=False)` on input cells).
