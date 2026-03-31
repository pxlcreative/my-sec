"""
Module B1 – Monthly ADV Part 2 PDF download and storage pipeline.

Public entry point: sync_month(month_str, db_session, sync_job_id)

All other functions are helpers used by sync_month (and exposed for testing).
"""
import csv
import io
import logging
import re
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from config import settings

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEC_FOIA_PAGE = "https://www.sec.gov/foia/docs/form-adv-archive-data.htm"
SEC_BASE_URL   = "https://www.sec.gov"

BROCHURES_DIR = Path(settings.data_dir) / "brochures"
ZIPS_DIR      = Path(settings.data_dir) / "zips" / "brochures"

# Regex that matches the ZIP file href in the FOIA page HTML.
# Captures: YYYY, MM, and optional suffix like "-part3" or "_part03".
_ZIP_HREF_RE = re.compile(
    r'href=["\']([^"\']*adv[-_]brochures[-_](\d{4})[-_](\d{2})[^"\']*\.zip)["\']',
    re.IGNORECASE,
)

# Candidate column names in the mapping CSV (SEC has varied these over time).
_CRD_COLS    = ("CRD_NUMBER", "CRD", "File_Number", "FILE_NUMBER")
_VID_COLS    = ("BROCHURE_VERSION_ID", "Brochure_Version_Id", "VERSION_ID")
_NAME_COLS   = ("BROCHURE_NAME", "Brochure_Name", "BROCHURENAME")
_DATE_COLS   = ("SUBMIT_DATE", "Submit_Date", "SUBMITDATE", "DATE_SUBMITTED")

_HTTP_TIMEOUT = 30          # seconds per request chunk
_RETRY_BACKOFF = 0.5        # seconds between download retries
_MAX_RETRIES   = 5


# ---------------------------------------------------------------------------
# 1. URL discovery
# ---------------------------------------------------------------------------

def discover_month_zip_urls(month_str: str) -> list[str]:
    """
    Fetch the SEC FOIA page HTML and return all ZIP URLs for the target month.

    month_str: "YYYY-MM" e.g. "2025-03"
    Handles multi-part ZIPs (part1 … part10+).
    Returns absolute URLs sorted by part number (or just [url] for single-part).
    """
    year, month = month_str.split("-")
    log.info("Discovering ZIPs for %s on %s", month_str, SEC_FOIA_PAGE)

    resp = requests.get(SEC_FOIA_PAGE, timeout=_HTTP_TIMEOUT)
    resp.raise_for_status()
    html = resp.text

    matched: list[str] = []
    seen: set[str] = set()
    for m in _ZIP_HREF_RE.finditer(html):
        href, yr, mo = m.group(1), m.group(2), m.group(3)
        if yr == year and mo == month:
            url = href if href.startswith("http") else SEC_BASE_URL + href
            if url not in seen:
                seen.add(url)
                matched.append(url)

    if not matched:
        log.warning("No ZIPs found for %s on FOIA page", month_str)
        return []

    # Sort: single-part (no "part" in name) first, then part1, part2, … part10+
    def _part_key(url: str) -> int:
        m = re.search(r"part(\d+)", url, re.IGNORECASE)
        return int(m.group(1)) if m else 0

    matched.sort(key=_part_key)
    log.info("Found %d ZIP(s) for %s: %s", len(matched), month_str, matched)
    return matched


# ---------------------------------------------------------------------------
# 2. Streaming file download
# ---------------------------------------------------------------------------

def download_file(url: str, dest_dir: Path) -> Path:
    """
    Stream-download *url* into *dest_dir*.

    Skips the download if a file of the same name already exists with a
    non-zero size.  Retries up to _MAX_RETRIES times with exponential backoff.
    Returns the local Path.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1].split("?")[0]
    dest = dest_dir / filename

    if dest.exists() and dest.stat().st_size > 0:
        log.info("download_file: already exists, skipping %s (%d bytes)", dest, dest.stat().st_size)
        return dest

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            log.info("Downloading %s (attempt %d)", url, attempt)
            with requests.get(url, stream=True, timeout=_HTTP_TIMEOUT) as r:
                r.raise_for_status()
                tmp = dest.with_suffix(".tmp")
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):  # 1 MiB
                        fh.write(chunk)
            tmp.rename(dest)
            log.info("Downloaded %s → %s (%d bytes)", url, dest, dest.stat().st_size)
            return dest
        except Exception as exc:
            log.warning("Download attempt %d failed for %s: %s", attempt, url, exc)
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BACKOFF * attempt)

    raise RuntimeError(f"Failed to download {url} after {_MAX_RETRIES} attempts")


# ---------------------------------------------------------------------------
# 3. Mapping CSV loader
# ---------------------------------------------------------------------------

def _find_col(header: list[str], candidates: tuple) -> str | None:
    """Return the first candidate column name found in *header* (case-insensitive)."""
    upper = {h.upper(): h for h in header}
    for c in candidates:
        if c.upper() in upper:
            return upper[c.upper()]
    return None


def load_mapping_csv(zip_path: Path) -> list[dict]:
    """
    Open *zip_path*, locate the mapping CSV, and return a list of dicts with keys:
      crd, version_id, brochure_name, submit_date
    Rows missing crd or version_id are silently skipped.
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find the mapping CSV — SEC names it IA_ADV_Brochures_YYYYMMDD.csv or similar.
        csv_names = [
            n for n in zf.namelist()
            if n.lower().endswith(".csv")
            and re.search(r"(adv|brochure)", n, re.IGNORECASE)
        ]
        if not csv_names:
            raise ValueError(f"No mapping CSV found in {zip_path.name}")

        csv_name = csv_names[0]
        log.info("load_mapping_csv: reading %s from %s", csv_name, zip_path.name)

        with zf.open(csv_name) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace")
            reader = csv.DictReader(text)
            header = reader.fieldnames or []

            col_crd  = _find_col(list(header), _CRD_COLS)
            col_vid  = _find_col(list(header), _VID_COLS)
            col_name = _find_col(list(header), _NAME_COLS)
            col_date = _find_col(list(header), _DATE_COLS)

            if not col_crd or not col_vid:
                raise ValueError(
                    f"Cannot locate CRD or version_id columns in {csv_name}. "
                    f"Header: {header}"
                )

            rows = []
            for row in reader:
                try:
                    crd = int(row[col_crd])
                    vid = int(row[col_vid])
                except (ValueError, TypeError):
                    continue
                rows.append({
                    "crd":          crd,
                    "version_id":   vid,
                    "brochure_name": row.get(col_name, "").strip() if col_name else None,
                    "submit_date":  row.get(col_date, "").strip()  if col_date else None,
                })

    log.info("load_mapping_csv: %d rows loaded from %s", len(rows), zip_path.name)
    return rows


# ---------------------------------------------------------------------------
# 4. PDF extraction
# ---------------------------------------------------------------------------

def extract_and_store_pdf(
    zip_path: Path,
    version_id: int,
    crd: int,
    submit_date: str | None,
) -> tuple[Path, int]:
    """
    Extract the PDF whose name contains *version_id* from *zip_path*.
    Save to /data/brochures/{crd}/{version_id}_{YYYYMMDD}.pdf.
    Returns (local_path, file_size_bytes).
    """
    # Date suffix for filename
    date_tag = "00000000"
    if submit_date:
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
            try:
                date_tag = datetime.strptime(submit_date, fmt).strftime("%Y%m%d")
                break
            except ValueError:
                continue

    dest_dir = BROCHURES_DIR / str(crd)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{version_id}_{date_tag}.pdf"

    if dest.exists() and dest.stat().st_size > 0:
        return dest, dest.stat().st_size

    with zipfile.ZipFile(zip_path, "r") as zf:
        # SEC names PDFs like "12345678.pdf" or "documents/12345678.pdf"
        candidates = [
            n for n in zf.namelist()
            if str(version_id) in n and n.lower().endswith(".pdf")
        ]
        if not candidates:
            raise FileNotFoundError(
                f"PDF for version_id={version_id} not found in {zip_path.name}"
            )
        pdf_entry = candidates[0]
        data = zf.read(pdf_entry)

    dest.write_bytes(data)
    return dest, len(data)


# ---------------------------------------------------------------------------
# 5. Existence check
# ---------------------------------------------------------------------------

def brochure_already_stored(version_id: int, db_session: Session) -> bool:
    from models.brochure import AdvBrochure
    return db_session.scalar(
        select(AdvBrochure).where(AdvBrochure.brochure_version_id == version_id).limit(1)
    ) is not None


# ---------------------------------------------------------------------------
# 6. Orchestrator
# ---------------------------------------------------------------------------

def sync_month(month_str: str, db_session: Session, sync_job_id: int) -> None:
    """
    Full pipeline for one month:
      discover ZIPs → download each → load mapping CSV → for each row:
        if not already stored: extract PDF → insert AdvBrochure record
    Updates SyncJob throughout.
    """
    from models.brochure import AdvBrochure
    from models.firm import Firm
    from models.sync_job import SyncJob
    from sqlalchemy import select

    job: SyncJob | None = db_session.get(SyncJob, sync_job_id)
    if job is None:
        raise ValueError(f"SyncJob {sync_job_id} not found")

    def _commit_progress(processed: int, stored: int, message: str | None = None) -> None:
        j = db_session.get(SyncJob, sync_job_id)
        if j:
            j.firms_processed = processed
            j.firms_updated   = stored
            if message:
                j.error_message = message
            db_session.commit()

    zip_urls = discover_month_zip_urls(month_str)
    if not zip_urls:
        _commit_progress(0, 0, f"No ZIPs found for {month_str}")
        return

    ZIPS_DIR.mkdir(parents=True, exist_ok=True)

    # Pre-load the set of known CRD numbers to skip orphaned brochures
    known_crds: set[int] = set(
        db_session.scalars(select(Firm.crd_number)).all()
    )

    total_processed = 0
    total_stored    = 0

    for zip_url in zip_urls:
        try:
            zip_path = download_file(zip_url, ZIPS_DIR)
        except RuntimeError as exc:
            log.error("sync_month: download failed for %s: %s", zip_url, exc)
            continue

        try:
            mapping_rows = load_mapping_csv(zip_path)
        except Exception as exc:
            log.error("sync_month: mapping CSV error in %s: %s", zip_path.name, exc)
            continue

        for row in mapping_rows:
            crd        = row["crd"]
            version_id = row["version_id"]

            total_processed += 1

            if crd not in known_crds:
                log.debug("sync_month: CRD %d not in firms table, skipping version %d", crd, version_id)
                continue

            if brochure_already_stored(version_id, db_session):
                continue

            try:
                local_path, size_bytes = extract_and_store_pdf(
                    zip_path, version_id, crd, row["submit_date"]
                )
            except (FileNotFoundError, Exception) as exc:
                log.warning(
                    "sync_month: could not extract version_id=%d crd=%d: %s",
                    version_id, crd, exc,
                )
                continue

            # Parse submit_date → date object
            submit_date_obj = None
            if row["submit_date"]:
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
                    try:
                        submit_date_obj = datetime.strptime(row["submit_date"], fmt).date()
                        break
                    except ValueError:
                        continue

            brochure = AdvBrochure(
                crd_number=crd,
                brochure_version_id=version_id,
                brochure_name=row["brochure_name"],
                date_submitted=submit_date_obj,
                source_month=month_str,
                file_path=str(local_path),
                file_size_bytes=size_bytes,
                downloaded_at=datetime.now(timezone.utc),
            )
            db_session.add(brochure)

            try:
                db_session.commit()
            except Exception as exc:
                db_session.rollback()
                log.warning("sync_month: DB insert failed version_id=%d: %s", version_id, exc)
                continue

            total_stored += 1
            log.info(
                "sync_month: stored version_id=%d crd=%d path=%s size=%d bytes",
                version_id, crd, local_path, size_bytes,
            )

            # Periodic progress flush
            if total_processed % 500 == 0:
                _commit_progress(total_processed, total_stored)

    _commit_progress(total_processed, total_stored)
    log.info(
        "sync_month %s complete: processed=%d stored=%d",
        month_str, total_processed, total_stored,
    )
