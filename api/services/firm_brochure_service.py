"""
Per-firm ADV Part 2 brochure fetch and storage.

Fetches the current Part 2 brochure listing for a single firm from the IAPD
and downloads any versions not already stored.

Entry point:
    fetch_and_store_firm_brochures(crd, db) -> int
        Returns the count of newly stored PDFs.

Brochure listing strategy
--------------------------
1. Primary: EFTS full-text search API — same endpoint used to populate raw_adv.
   The response includes a Brochures array with name, date, and version ID.
2. Fallback: Scrape https://adviserinfo.sec.gov/firm/brochure/{crd} with
   BeautifulSoup if the EFTS response contains no brochure data.

Only Part 2 brochures are downloaded (name contains "Part 2" or "Brochure",
confirmed by the BrochureType field where present).

Download URL: https://files.adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={id}
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

_EFTS_URL = "https://efts.sec.gov/LATEST/search-index?query=Info.FirmCrdNb:{crd}&forms=ADV"
_BROCHURE_PAGE_URL = "https://adviserinfo.sec.gov/firm/brochure/{crd}"
_BROCHURE_DOWNLOAD_URL = (
    "https://files.adviserinfo.sec.gov/IAPD/Content/Common/"
    "crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={version_id}"
)

_HEADERS = {"User-Agent": "MySEC/1.0 (self-hosted; research use)"}
_SLEEP_BETWEEN = 0.5
_MAX_RETRIES = 5
_RETRY_BACKOFF = 1.0


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def fetch_and_store_firm_brochures(crd: int, db: Session) -> int:
    """
    Fetch Part 2 brochures for one firm and store any new ones.
    Returns the count of newly stored PDFs.
    """
    brochures = _get_brochure_listing(crd)
    if not brochures:
        log.info("fetch_and_store_firm_brochures: no Part 2 brochures found for CRD %d", crd)
        return 0

    from models.brochure import AdvBrochure
    from services.storage_backends import get_active_backend, make_brochure_key

    backend = get_active_backend(db)

    # Pre-load stored version IDs for this firm to skip already-downloaded PDFs
    stored_vids: set[int] = set(
        db.scalars(
            select(AdvBrochure.brochure_version_id).where(AdvBrochure.crd_number == crd)
        ).all()
    )

    stored = 0
    for b in brochures:
        version_id = b["version_id"]
        if version_id in stored_vids:
            continue

        time.sleep(_SLEEP_BETWEEN)

        url = _BROCHURE_DOWNLOAD_URL.format(version_id=version_id)
        try:
            data = _download_pdf(url)
        except Exception as exc:
            log.warning(
                "fetch_and_store_firm_brochures: download failed version_id=%d crd=%d: %s",
                version_id, crd, exc,
            )
            continue

        date_tag = b["date_submitted"].replace("-", "") if b["date_submitted"] else "00000000"
        key = make_brochure_key(crd, version_id, date_tag)

        try:
            backend.put(key, data)
        except Exception as exc:
            log.warning(
                "fetch_and_store_firm_brochures: storage failed version_id=%d crd=%d: %s",
                version_id, crd, exc,
            )
            continue

        uri = backend.uri_for(key)

        submit_date = None
        if b["date_submitted"]:
            try:
                submit_date = datetime.strptime(b["date_submitted"], "%Y-%m-%d").date()
            except ValueError:
                try:
                    submit_date = datetime.strptime(b["date_submitted"], "%m/%d/%Y").date()
                except ValueError:
                    pass

        brochure = AdvBrochure(
            crd_number=crd,
            brochure_version_id=version_id,
            brochure_name=b.get("name"),
            date_submitted=submit_date,
            source_month=None,
            file_path=uri,
            file_size_bytes=len(data),
            downloaded_at=datetime.now(timezone.utc),
        )
        db.add(brochure)
        try:
            db.commit()
        except Exception as exc:
            db.rollback()
            log.warning(
                "fetch_and_store_firm_brochures: DB insert failed version_id=%d: %s",
                version_id, exc,
            )
            continue

        stored_vids.add(version_id)
        stored += 1
        log.info(
            "fetch_and_store_firm_brochures: stored version_id=%d crd=%d name=%r size=%d",
            version_id, crd, b.get("name"), len(data),
        )

    return stored


# ---------------------------------------------------------------------------
# Brochure listing: EFTS primary, HTML fallback
# ---------------------------------------------------------------------------

def _get_brochure_listing(crd: int) -> list[dict]:
    """
    Return list of Part 2 brochures for crd:
    [{"version_id": int, "name": str|None, "date_submitted": str|None}, ...]

    Tries EFTS API first; falls back to HTML scraping.
    """
    try:
        brochures = _listing_from_efts(crd)
        if brochures is not None:
            return brochures
    except Exception as exc:
        log.warning("_get_brochure_listing: EFTS failed for CRD %d: %s", crd, exc)

    # Fallback: scrape the HTML brochure page
    try:
        return _listing_from_html(crd)
    except Exception as exc:
        log.warning("_get_brochure_listing: HTML scrape failed for CRD %d: %s", crd, exc)
        return []


def _listing_from_efts(crd: int) -> list[dict] | None:
    """
    Query the EFTS search API for this CRD's ADV filing and extract Part 2 brochures.
    Returns None if the response contains no brochure data (triggers fallback).
    """
    url = _EFTS_URL.format(crd=crd)
    resp = _http_get(url)
    data = resp.json()

    hits = data.get("hits", {}).get("hits", [])
    if not hits:
        return None

    # Walk hits to find brochure entries — try several known response shapes
    all_brochures: list[dict] = []
    for hit in hits:
        source = hit.get("_source", {})

        # Shape 1: source.Brochures (array at top level)
        raw = source.get("Brochures") or []

        # Shape 2: source.FormInfo.Brochures
        if not raw:
            raw = source.get("FormInfo", {}).get("Brochures") or []

        # Shape 3: source.FilingDetail.Brochures
        if not raw:
            raw = source.get("FilingDetail", {}).get("Brochures") or []

        for item in raw:
            parsed = _parse_efts_brochure(item)
            if parsed:
                all_brochures.append(parsed)

    if not all_brochures:
        return None  # signal to use fallback

    return all_brochures


def _parse_efts_brochure(item: dict) -> dict | None:
    """
    Normalise a single brochure entry from the EFTS response.
    Returns None if this is not a Part 2 brochure.
    """
    # Try several field name conventions
    version_id = (
        item.get("BrochureVersionId")
        or item.get("BRCHR_VRSN_ID")
        or item.get("brochure_version_id")
    )
    if not version_id:
        return None
    try:
        version_id = int(version_id)
    except (TypeError, ValueError):
        return None

    name = (
        item.get("BrochureName")
        or item.get("BRCHR_NM")
        or item.get("brochure_name")
        or item.get("Name")
    )
    date_submitted = (
        item.get("DateSubmitted")
        or item.get("DATE_SUBMITTED")
        or item.get("date_submitted")
        or item.get("SubmittedOn")
    )
    brochure_type = (
        item.get("BrochureType")
        or item.get("BRCHR_TYPE")
        or item.get("brochure_type")
        or ""
    )

    if not _is_part2(name, brochure_type):
        return None

    return {"version_id": version_id, "name": name, "date_submitted": date_submitted}


def _is_part2(name: str | None, brochure_type: str | None) -> bool:
    """Return True if this brochure entry represents a Part 2 filing."""
    type_str = (brochure_type or "").lower()
    name_str = (name or "").lower()
    # Explicit type codes used by IAPD
    if "part2" in type_str or "part 2" in type_str:
        return True
    if "adv_part2" in type_str or "adv-part2" in type_str:
        return True
    # Fall back to name heuristic
    if "part 2" in name_str or "brochure" in name_str:
        return True
    return False


def _listing_from_html(crd: int) -> list[dict]:
    """
    Fallback: scrape https://adviserinfo.sec.gov/firm/brochure/{crd} for Part 2 brochures.
    Requires beautifulsoup4 (`pip install beautifulsoup4`).
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.error(
            "_listing_from_html: beautifulsoup4 not installed. "
            "Run 'pip install beautifulsoup4' to enable HTML fallback."
        )
        return []

    url = _BROCHURE_PAGE_URL.format(crd=crd)
    resp = _http_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")

    brochures: list[dict] = []

    # The page renders a table of brochures. Rows contain:
    # brochure name | date submitted | link (href contains BRCHR_VRSN_ID)
    for a_tag in soup.find_all("a", href=re.compile(r"BRCHR_VRSN_ID=(\d+)", re.IGNORECASE)):
        href = a_tag.get("href", "")
        m = re.search(r"BRCHR_VRSN_ID=(\d+)", href, re.IGNORECASE)
        if not m:
            continue
        version_id = int(m.group(1))

        # Name is either the link text or a nearby cell
        name = a_tag.get_text(strip=True) or None

        # Date: look for a sibling or parent <td> containing a date-like string
        date_submitted = None
        row = a_tag.find_parent("tr")
        if row:
            cells = row.find_all("td")
            for cell in cells:
                text = cell.get_text(strip=True)
                if re.match(r"\d{1,2}/\d{1,2}/\d{4}", text):
                    date_submitted = text
                    break

        if not _is_part2(name, None):
            continue

        brochures.append({"version_id": version_id, "name": name, "date_submitted": date_submitted})

    return brochures


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _http_get(url: str) -> requests.Response:
    """GET with User-Agent, retry on 429/503, raise on other errors."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=30)
            if resp.status_code in (429, 503):
                wait = _RETRY_BACKOFF * attempt
                log.warning("_http_get: %d from %s, waiting %.1fs", resp.status_code, url, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BACKOFF * attempt)
            else:
                raise RuntimeError(f"HTTP GET failed after {_MAX_RETRIES} attempts: {url}") from exc
    raise RuntimeError(f"HTTP GET failed after {_MAX_RETRIES} attempts: {url}")


def _download_pdf(url: str) -> bytes:
    """Download a PDF and return its bytes. Retries on transient errors."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=60)
            if resp.status_code in (429, 503):
                wait = _RETRY_BACKOFF * attempt
                log.warning("_download_pdf: %d from %s, waiting %.1fs", resp.status_code, url, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type.lower() and len(resp.content) < 100:
                raise ValueError(f"Unexpected content type: {content_type}")
            return resp.content
        except (requests.RequestException, ValueError) as exc:
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BACKOFF * attempt)
            else:
                raise RuntimeError(f"PDF download failed after {_MAX_RETRIES} attempts: {url}") from exc
    raise RuntimeError(f"PDF download failed after {_MAX_RETRIES} attempts: {url}")
