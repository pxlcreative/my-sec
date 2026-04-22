#!/usr/bin/env python3
"""Weekly CI canary — probe SEC endpoints for schema drift.

We don't re-run the full pipeline; we just confirm that the contracts the
loaders depend on are still intact:

1. reports_metadata.json is fetchable and has the expected top-level
   sections (advFilingData, advBrochures, advW).
2. For the most recent month in advFilingData, the ZIP URL we'd construct
   is actually reachable (HEAD 200).
3. For the most recent advW entry, the expected CSV column header set is
   still present ("CRD Number" with a space, not underscore — see
   CLAUDE.md Data Gotchas).

A failure in any step means the real loaders (scripts/load_filing_data.py)
will break at the next run. The job exits non-zero so the failure is loud.

Runs without touching the DB. Designed to be fast (<60s on a good link).
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import sys
import zipfile
from datetime import datetime, timezone

import requests

log = logging.getLogger("probe_sec_schema")

METADATA_URL = "https://reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json"
REPORTS_BASE_URL = "https://reports.adviserinfo.sec.gov/reports/foia"
HEADERS = {"User-Agent": "MySEC-CI/1.0 (schema drift probe)"}
TIMEOUT = 30

REQUIRED_SECTIONS = ("advFilingData", "advBrochures", "advW")
ADVW_REQUIRED_COLUMN = "CRD Number"  # space, not underscore


def _fail(msg: str) -> None:
    log.error(msg)
    sys.exit(1)


def fetch_metadata() -> dict:
    log.info("GET %s", METADATA_URL)
    r = requests.get(METADATA_URL, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        _fail(f"metadata feed returned HTTP {r.status_code}")
    try:
        return r.json()
    except ValueError as exc:
        _fail(f"metadata feed did not return valid JSON: {exc}")
    return {}  # unreachable


def check_sections(metadata: dict) -> None:
    missing = [s for s in REQUIRED_SECTIONS if s not in metadata]
    if missing:
        _fail(f"metadata feed missing expected sections: {missing}")
    log.info("Top-level sections present: %s", ", ".join(REQUIRED_SECTIONS))


def _latest_entry(section: dict, within_months: int) -> tuple[str, str] | None:
    """Return (year, file_name) of the most recent entry, or None.

    Filters out entries older than `within_months` so a stale feed can't
    silently pass the probe.
    """
    now = datetime.now(timezone.utc)
    cutoff_ym = (now.year * 12 + now.month) - within_months

    candidates: list[tuple[int, int, str, str]] = []
    for year_str, year_data in section.items():
        if year_str in ("sectionDisplayName", "sectionDisplayOrder"):
            continue
        try:
            year = int(year_str)
        except ValueError:
            continue
        files = year_data if isinstance(year_data, list) else year_data.get("files", [])
        for f in files:
            file_name = f.get("fileName") if isinstance(f, dict) else str(f)
            if not file_name:
                continue
            # Best-effort month extraction from the filename.
            month = _guess_month(file_name)
            ym = year * 12 + (month or 12)
            if ym < cutoff_ym:
                continue
            candidates.append((year, month or 12, str(year), file_name))

    if not candidates:
        return None
    candidates.sort(reverse=True)
    _, _, year, file_name = candidates[0]
    return year, file_name


def _guess_month(file_name: str) -> int | None:
    # Typical pattern: advFilingData_2026_01.zip or advW_2026_01.zip
    for token in file_name.replace(".", "_").split("_"):
        if token.isdigit() and 1 <= int(token) <= 12 and len(token) == 2:
            return int(token)
    return None


def check_filing_data_reachable(metadata: dict, months: int) -> None:
    section = metadata.get("advFilingData", {})
    latest = _latest_entry(section, within_months=months)
    if latest is None:
        _fail(
            f"advFilingData has no entries within the last {months} month(s) — "
            "either the feed stalled or our cutoff logic is wrong."
        )
    year, file_name = latest  # type: ignore[misc]
    url = f"{REPORTS_BASE_URL}/advFilingData/{year}/{file_name}"
    log.info("HEAD %s", url)
    r = requests.head(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    if r.status_code != 200:
        _fail(f"advFilingData URL returned HTTP {r.status_code}: {url}")
    log.info("advFilingData latest file reachable.")


def check_advw_columns(metadata: dict, months: int) -> None:
    section = metadata.get("advW", {})
    latest = _latest_entry(section, within_months=months)
    if latest is None:
        log.warning(
            "advW has no entries within the last %d month(s) — skipping column check.",
            months,
        )
        return
    year, file_name = latest  # type: ignore[misc]
    url = f"{REPORTS_BASE_URL}/advW/{year}/{file_name}"
    log.info("GET %s", url)
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    if r.status_code != 200:
        _fail(f"advW URL returned HTTP {r.status_code}: {url}")

    try:
        zf = zipfile.ZipFile(io.BytesIO(r.content))
    except zipfile.BadZipFile as exc:
        _fail(f"advW ZIP is corrupt: {exc}")
        return
    csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not csv_members:
        _fail(f"advW ZIP contains no CSV: {zf.namelist()}")
    with zf.open(csv_members[0]) as fh:
        reader = csv.reader(io.TextIOWrapper(fh, encoding="utf-8", errors="replace"))
        header = next(reader, [])
    if ADVW_REQUIRED_COLUMN not in header:
        _fail(
            f"advW header missing required column '{ADVW_REQUIRED_COLUMN}'. "
            f"Observed: {header[:10]}... — "
            "update scripts/load_filing_data.py column map."
        )
    log.info("advW header OK (found '%s').", ADVW_REQUIRED_COLUMN)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--months",
        type=int,
        default=2,
        help="Look back this many months for the 'latest' entry (default: 2).",
    )
    args = parser.parse_args()

    metadata = fetch_metadata()
    check_sections(metadata)
    check_filing_data_reachable(metadata, months=args.months)
    check_advw_columns(metadata, months=args.months)
    log.info("SEC schema probe: all checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
