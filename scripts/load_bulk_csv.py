#!/usr/bin/env python3
"""
Module A: Bulk Historical Load
================================
Downloads and loads the three SEC ADV bulk CSV ZIPs into:
  - firms              (most-recent filing per CRD)
  - firm_aum_history   (every filing row as a time-series point)
  - firm_disclosures_summary (DRP counts per CRD)

Usage (from project root with DATABASE_URL in .env or environment):
    python scripts/load_bulk_csv.py

Or inside the api container:
    docker compose run --rm api python /project/scripts/load_bulk_csv.py
"""

import csv
import io
import logging
import os
import sys
import time
import zipfile
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Bootstrap paths / config
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT / "api"))

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DOWNLOAD_DIR = PROJECT_ROOT / "data" / "raw" / "csv"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

ZIP_URLS = [
    "https://www.sec.gov/files/adv-filing-data-20001019-20111104.zip",
    "https://www.sec.gov/files/adv-filing-data-20111105-20241231-part1.zip",
    "https://www.sec.gov/files/adv-filing-data-20111105-20241231-part2.zip",
]

# ---------------------------------------------------------------------------
# Column name normalisation
# ---------------------------------------------------------------------------
# Maps canonical name → list of known column header variants (upper-cased).
# Covers three formats:
#   • 2000–2011 legacy ZIP (IA_MAIN.csv, old column names)
#   • 2011–2024 IA_MAIN.csv (renamed columns)
#   • 2011–2024 IA_ADV_Base_A_*.csv (ADV form field numbers — SEC's current format)
COLUMN_MAP: dict[str, list[str]] = {
    "crd_number":            ["CRD_NUMBER", "FIRM_CRD_NUMBER", "CRD",
                              "1E1"],
    "firm_name":             ["FIRM_NAME", "ORGNAME", "BUS_NAME", "BUSNAME",
                              "1B1"],
    "legal_name":            ["ITEM1A_LEGAL_NAME", "LEGAL_NAME", "LEGALNAME", "FIRM_NAME",
                              "1A"],
    "filing_date":           ["ADV_FILING_DATE", "FILING_DATE", "ADVFILINGDATE",
                              "DATESUBMITTED"],
    "aum_discretionary":     ["ITEM5F_2A", "ITEM5F2A", "AUM_DISCRETIONARY", "DISCRET_AUM",
                              "5F2A"],
    "aum_non_discretionary": ["ITEM5F_2B", "ITEM5F2B", "AUM_NONDISCRETIONARY", "NONDISCRET_AUM",
                              "5F2B"],
    "aum_total":             ["ITEM5F_2C", "ITEM5F2C", "AUM_TOTAL", "TOTAL_AUM", "REG_AUM",
                              "5F2C"],
    "num_employees":         ["ITEM5A_TOTAL_EMPLOYEES", "TOTAL_EMPLOYEES", "NUM_EMPLOYEES",
                              "ITEM5A_EMPL_TOTAL", "EMPLOYEES_TOTAL",
                              "5A"],
    "main_street1":          ["ITEM1F_ADDRESS", "MAIN_ADDRESS", "ADDRESS1", "STREET1",
                              "ITEM1F_STREET1",
                              "1F1-STREET 1"],
    "main_city":             ["ITEM1F_CITY", "MAIN_CITY", "CITY",
                              "1F1-CITY"],
    "main_state":            ["ITEM1F_STATE", "MAIN_STATE", "STATE",
                              "1F1-STATE"],
    "main_zip":              ["ITEM1F_ZIP", "MAIN_ZIP", "ZIP", "ZIPCODE",
                              "1F1-POSTAL"],
    "registration_status":   ["REGISTRATION_STATUS", "REG_STATUS", "REGSTATUS"],
}

# Reverse lookup: uppercased header → canonical name
_HEADER_TO_CANONICAL: dict[str, str] = {}
for canonical, variants in COLUMN_MAP.items():
    for v in variants:
        _HEADER_TO_CANONICAL[v.upper()] = canonical


def normalize_headers(raw_headers: list[str]) -> dict[str, str]:
    """Return mapping: raw header → canonical name (only for known columns)."""
    result = {}
    for h in raw_headers:
        canonical = _HEADER_TO_CANONICAL.get(h.strip().upper())
        if canonical:
            result[h.strip()] = canonical
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _int_or_none(val: str) -> int | None:
    """Parse a string to int, returning None for blank/non-numeric values."""
    if not val or not val.strip():
        return None
    try:
        return int(float(val.strip()))
    except (ValueError, OverflowError):
        return None


def _parse_date(val: str) -> date | None:
    """Try common date formats used in SEC CSVs.
    Handles both plain dates and datetime strings (e.g. '11/13/2012 01:39:54 PM').
    """
    if not val or not val.strip():
        return None
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(val.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _normalize_zip(val: str) -> str | None:
    """Return first 5 digits of a ZIP string, or None."""
    if not val:
        return None
    digits = "".join(c for c in val if c.isdigit())
    return digits[:5] if len(digits) >= 5 else (digits or None)


def _normalize_state(val: str) -> str | None:
    """Return 2-letter state abbreviation or None."""
    if not val:
        return None
    v = val.strip()
    return v[:2].upper() if len(v) >= 2 else v.upper() or None


# ---------------------------------------------------------------------------
# 1. Download
# ---------------------------------------------------------------------------

def download_zip(url: str, dest_dir: Path) -> Path:
    """
    Streaming download of url into dest_dir/<filename>.
    Skips if the file already exists (re-run safe).
    Returns the local path.
    """
    filename = url.rsplit("/", 1)[-1]
    dest = dest_dir / filename
    if dest.exists():
        log.info("Already downloaded: %s", dest.name)
        return dest

    log.info("Downloading %s ...", url)
    headers = {"User-Agent": "MySEC/1.0 (self-hosted; research use)"}
    with requests.get(url, stream=True, timeout=120, headers=headers) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        last_logged = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):  # 1 MB chunks
                f.write(chunk)
                downloaded += len(chunk)
                if total and downloaded - last_logged >= 50 << 20:  # log every 50 MB
                    log.info("  %.0f%% (%.0f MB)", 100 * downloaded / total,
                             downloaded / (1 << 20))
                    last_logged = downloaded
    log.info("Saved %s (%.0f MB)", dest.name, dest.stat().st_size / (1 << 20))
    return dest


# ---------------------------------------------------------------------------
# 2 & 3. Parse IA_MAIN.csv
# ---------------------------------------------------------------------------

def _parse_adv_csv(zf: zipfile.ZipFile, entry_name: str,
                   default_registration_status: str | None = None) -> list[dict]:
    """
    Parse one CSV entry from an open ZipFile using the canonical column map.
    Returns a list of row dicts with canonical keys.
    """
    rows: list[dict] = []
    errors = 0

    with zf.open(entry_name) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
        reader = csv.DictReader(text)
        header_map = normalize_headers(reader.fieldnames or [])

        for i, raw_row in enumerate(reader):
            try:
                row = {canonical: raw_row[raw_h]
                       for raw_h, canonical in header_map.items()
                       if raw_h in raw_row}

                crd = _int_or_none(row.get("crd_number", ""))
                if not crd:
                    continue

                filing_date = _parse_date(row.get("filing_date", ""))
                if not filing_date:
                    continue

                reg_status = (row.get("registration_status") or "").strip() or default_registration_status

                rows.append({
                    "crd_number":            crd,
                    "firm_name":             (row.get("firm_name") or "").strip() or None,
                    "legal_name":            (row.get("legal_name") or "").strip() or None,
                    "filing_date":           filing_date,
                    "aum_discretionary":     _int_or_none(row.get("aum_discretionary", "")),
                    "aum_non_discretionary": _int_or_none(row.get("aum_non_discretionary", "")),
                    "aum_total":             _int_or_none(row.get("aum_total", "")),
                    "num_employees":         _int_or_none(row.get("num_employees", "")),
                    "main_street1":          (row.get("main_street1") or "").strip() or None,
                    "main_city":             (row.get("main_city") or "").strip() or None,
                    "main_state":            _normalize_state(row.get("main_state", "")),
                    "main_zip":              _normalize_zip(row.get("main_zip", "")),
                    "registration_status":   reg_status,
                })
            except Exception as exc:
                errors += 1
                if errors <= 5:
                    log.warning("Row %d parse error in %s: %s", i, entry_name, exc)

    return rows, errors


def parse_ia_main(zip_path: Path) -> list[dict]:
    """
    Open the ZIP, find the main firm data CSV, normalise columns, and return
    a list of dicts with canonical keys.

    Supports two formats:
      • Legacy:  IA_MAIN.csv (used in older bulk ZIPs)
      • Current: IA_ADV_Base_A_*.csv (SEC's format since ~2024)

    Rows with no crd_number or filing_date are skipped.
    """
    import re

    rows: list[dict] = []
    total_errors = 0

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()

        # --- Try legacy IA_MAIN.csv first ---
        ia_main = next((n for n in names if Path(n).name.upper() == "IA_MAIN.CSV"), None)
        if ia_main:
            log.info("Parsing %s from %s ...", ia_main, zip_path.name)
            chunk, errors = _parse_adv_csv(zf, ia_main)
            rows.extend(chunk)
            total_errors += errors
            log.info("Parsed %d rows from IA_MAIN (%d errors)", len(rows), total_errors)
            return rows

        # --- Fall back to IA_ADV_Base_A_*.csv (current SEC format) ---
        base_a_files = sorted(
            n for n in names
            if re.search(r"IA_ADV_Base_A", n, re.IGNORECASE) and n.endswith(".csv")
        )
        if not base_a_files:
            log.warning("No recognised firm data CSV (IA_MAIN.CSV or IA_ADV_Base_A*.csv) "
                        "found in %s", zip_path.name)
            return rows

        for entry in base_a_files:
            log.info("Parsing %s from %s ...", entry, zip_path.name)
            chunk, errors = _parse_adv_csv(zf, entry, default_registration_status="Registered")
            rows.extend(chunk)
            total_errors += errors

    log.info("Parsed %d rows from IA_ADV_Base_A (%d errors)", len(rows), total_errors)
    return rows


# ---------------------------------------------------------------------------
# 4. Upsert firms (most-recent filing per CRD)
# ---------------------------------------------------------------------------

def upsert_firms(rows: list[dict], conn) -> int:
    """
    Deduplicate to one row per CRD (most recent filing_date), then bulk-upsert
    into firms. Only overwrites an existing row when the incoming filing_date
    is newer than the stored last_filing_date.

    Returns number of rows passed to execute (not necessarily changed).
    """
    # Keep only the most-recent row per CRD
    best: dict[int, dict] = {}
    for row in rows:
        crd = row["crd_number"]
        if crd not in best or row["filing_date"] > best[crd]["filing_date"]:
            best[crd] = row

    deduplicated = list(best.values())

    sql = """
        INSERT INTO firms (
            crd_number, legal_name, business_name,
            aum_total, aum_discretionary, aum_non_discretionary,
            num_employees,
            main_street1, main_city, main_state, main_zip,
            registration_status,
            last_filing_date
        ) VALUES (
            %(crd_number)s,
            COALESCE(%(legal_name)s, %(firm_name)s, 'Unknown'),
            %(firm_name)s,
            %(aum_total)s, %(aum_discretionary)s, %(aum_non_discretionary)s,
            %(num_employees)s,
            %(main_street1)s, %(main_city)s, %(main_state)s, %(main_zip)s,
            %(registration_status)s,
            %(filing_date)s
        )
        ON CONFLICT (crd_number) DO UPDATE SET
            legal_name             = COALESCE(EXCLUDED.legal_name, EXCLUDED.business_name,
                                              firms.legal_name),
            business_name          = COALESCE(EXCLUDED.business_name, firms.business_name),
            aum_total              = EXCLUDED.aum_total,
            aum_discretionary      = EXCLUDED.aum_discretionary,
            aum_non_discretionary  = EXCLUDED.aum_non_discretionary,
            num_employees          = EXCLUDED.num_employees,
            main_street1           = EXCLUDED.main_street1,
            main_city              = EXCLUDED.main_city,
            main_state             = EXCLUDED.main_state,
            main_zip               = EXCLUDED.main_zip,
            registration_status    = EXCLUDED.registration_status,
            last_filing_date       = EXCLUDED.last_filing_date,
            updated_at             = NOW()
        WHERE EXCLUDED.last_filing_date > firms.last_filing_date
              OR firms.last_filing_date IS NULL
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, deduplicated, page_size=500)
    conn.commit()
    log.info("Firms upsert: %d unique CRDs submitted", len(deduplicated))
    return len(deduplicated)


# ---------------------------------------------------------------------------
# 5. Insert AUM history
# ---------------------------------------------------------------------------

def insert_aum_history(rows: list[dict], conn, source_tag: str) -> int:
    """
    Insert every row as one AUM time-series point.
    Skips duplicates (crd_number, filing_date, source) silently.
    Returns number of rows submitted.
    """
    sql = """
        INSERT INTO firm_aum_history (
            crd_number, filing_date,
            aum_total, aum_discretionary, aum_non_discretionary,
            num_accounts, source
        ) VALUES (
            %(crd_number)s, %(filing_date)s,
            %(aum_total)s, %(aum_discretionary)s, %(aum_non_discretionary)s,
            NULL, %(source)s
        )
        ON CONFLICT (crd_number, filing_date, source) DO NOTHING
    """

    tagged = [{**r, "source": source_tag} for r in rows]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, tagged, page_size=1000)
    conn.commit()
    log.info("AUM history: %d rows submitted (dupes skipped)", len(tagged))
    return len(tagged)


# ---------------------------------------------------------------------------
# 8. Parse DRP files → firm_disclosures_summary
# ---------------------------------------------------------------------------

# DRP CSV filenames → which counter to increment
DRP_COUNTER_MAP = {
    "DRP_CRIMINAL":    "criminal_count",
    "DRP_REGULATORY":  "regulatory_count",
    "DRP_CIVIL":       "civil_count",
    "DRP_CUSTOMER":    "customer_count",
    # older naming variants
    "DRP_CRIM":        "criminal_count",
    "DRP_REG":         "regulatory_count",
    "DRP_CIV":         "civil_count",
    "DRP_CUST":        "customer_count",
}


def parse_drp_counts(zip_path: Path) -> dict[int, dict[str, int]]:
    """
    Read all DRP_*.csv files from the ZIP and count records per CRD.
    Returns {crd_number: {criminal_count: N, regulatory_count: N, ...}}.

    Handles two layouts:
      • Legacy: CRD column present (CRD_NUMBER / FIRM_CRD_NUMBER)
      • Current (IA_ADV_Base_A format): FilingID column; resolves to CRD via
        a lookup built from IA_ADV_Base_A in the same ZIP.
    """
    import re as _re

    counts: dict[int, dict[str, int]] = defaultdict(
        lambda: {"criminal_count": 0, "regulatory_count": 0,
                 "civil_count": 0, "customer_count": 0}
    )

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()

        # Build FilingID→CRD lookup for new-format ZIPs
        filing_id_to_crd: dict[str, int] = {}
        base_a = next(
            (n for n in names if _re.search(r"IA_ADV_Base_A", n, _re.IGNORECASE)
             and n.endswith(".csv")), None
        )
        if base_a:
            with zf.open(base_a) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
                reader = csv.DictReader(text)
                for row in reader:
                    fid = row.get("FilingID", "").strip()
                    crd = _int_or_none(row.get("1E1", ""))
                    if fid and crd:
                        filing_id_to_crd[fid] = crd

        # Match DRP files by partial stem (handles date suffixes like _20001019_20111104)
        for name in names:
            stem_upper = Path(name).stem.upper()
            counter_field = next(
                (v for k, v in DRP_COUNTER_MAP.items() if k in stem_upper), None
            )
            if not counter_field:
                continue

            log.info("Parsing %s from %s ...", name, zip_path.name)
            with zf.open(name) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
                reader = csv.DictReader(text)
                fieldnames = reader.fieldnames or []

                # Prefer direct CRD column; fall back to FilingID lookup
                crd_field = next(
                    (c for c in ("CRD_NUMBER", "FIRM_CRD_NUMBER", "CRD") if c in fieldnames),
                    None,
                )
                filing_id_field = "FilingID" if "FilingID" in fieldnames else None

                if not crd_field and not filing_id_field:
                    log.warning("No CRD or FilingID column in %s, skipping", name)
                    continue

                for row in reader:
                    crd = _int_or_none(row.get(crd_field, "")) if crd_field else None
                    if crd is None and filing_id_field:
                        fid = row.get(filing_id_field, "").strip()
                        crd = filing_id_to_crd.get(fid)
                    if crd:
                        counts[crd][counter_field] += 1

    log.info("DRP counts: %d CRDs with disclosures in %s", len(counts), zip_path.name)
    return counts


def upsert_disclosures_summary(counts: dict[int, dict[str, int]], conn) -> int:
    """
    Upsert disclosure counts into firm_disclosures_summary.
    Adds to existing counts so running multiple ZIPs is additive.
    Only upserts for CRDs that exist in the firms table.
    """
    if not counts:
        return 0

    sql = """
        INSERT INTO firm_disclosures_summary (
            crd_number, criminal_count, regulatory_count,
            civil_count, customer_count, updated_at
        )
        SELECT
            %(crd_number)s,
            %(criminal_count)s,
            %(regulatory_count)s,
            %(civil_count)s,
            %(customer_count)s,
            NOW()
        WHERE EXISTS (SELECT 1 FROM firms WHERE crd_number = %(crd_number)s)
        ON CONFLICT (crd_number) DO UPDATE SET
            criminal_count    = firm_disclosures_summary.criminal_count
                                + EXCLUDED.criminal_count,
            regulatory_count  = firm_disclosures_summary.regulatory_count
                                + EXCLUDED.regulatory_count,
            civil_count       = firm_disclosures_summary.civil_count
                                + EXCLUDED.civil_count,
            customer_count    = firm_disclosures_summary.customer_count
                                + EXCLUDED.customer_count,
            updated_at        = NOW()
    """

    rows = [{"crd_number": crd, **v} for crd, v in counts.items()]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    log.info("Disclosures summary: %d CRDs upserted", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# 6. Orchestrator for one ZIP
# ---------------------------------------------------------------------------

def load_zip(zip_url: str, conn, source_tag: str) -> dict:
    """Download → parse → upsert. Returns a stats dict."""
    t0 = time.time()
    zip_path = download_zip(zip_url, DOWNLOAD_DIR)

    ia_rows = parse_ia_main(zip_path)
    firms_submitted = upsert_firms(ia_rows, conn)
    aum_submitted = insert_aum_history(ia_rows, conn, source_tag)

    drp_counts = parse_drp_counts(zip_path)
    disclosures_submitted = upsert_disclosures_summary(drp_counts, conn)

    elapsed = time.time() - t0
    stats = {
        "zip": zip_url.rsplit("/", 1)[-1],
        "ia_rows_parsed": len(ia_rows),
        "firms_submitted": firms_submitted,
        "aum_rows_submitted": aum_submitted,
        "disclosures_submitted": disclosures_submitted,
        "elapsed_s": round(elapsed, 1),
    }
    log.info("Done %-55s  firms=%d  aum=%d  disclosures=%d  t=%.0fs",
             stats["zip"], firms_submitted, aum_submitted,
             disclosures_submitted, elapsed)
    return stats


# ---------------------------------------------------------------------------
# 7. main()
# ---------------------------------------------------------------------------

def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        log.error("DATABASE_URL is not set. Copy .env.example → .env and fill it in.")
        sys.exit(1)

    # psycopg2 expects postgresql:// not postgresql+psycopg2://
    dsn = database_url.replace("postgresql+psycopg2://", "postgresql://")

    log.info("Connecting to database ...")
    conn = psycopg2.connect(dsn)

    t_start = time.time()
    all_stats = []

    for url in ZIP_URLS:
        stats = load_zip(url, conn, source_tag="bulk_csv_2011_2024")
        all_stats.append(stats)

    conn.close()

    total_ia = sum(s["ia_rows_parsed"] for s in all_stats)
    total_aum = sum(s["aum_rows_submitted"] for s in all_stats)
    total_firms = sum(s["firms_submitted"] for s in all_stats)
    total_disc = sum(s["disclosures_submitted"] for s in all_stats)
    total_t = time.time() - t_start

    log.info(
        "=== ALL DONE ===  ia_rows=%d  firms_submitted=%d  "
        "aum_rows=%d  disclosures=%d  total_time=%.0fs",
        total_ia, total_firms, total_aum, total_disc, total_t,
    )


if __name__ == "__main__":
    main()
