"""
Per-firm ADV Part 2 brochure fetch and storage.

Fetches the current Part 2 brochure listing for a single firm from the IAPD
and downloads any versions not already stored.

Entry point:
    fetch_and_store_firm_brochures(crd, db) -> int
        Returns the count of newly stored PDFs.

Brochure listing strategy
--------------------------
Query the IAPD firm search API:
  https://api.adviserinfo.sec.gov/search/firm/{crd}?hl=true&nrows=12&query=test&r=25&sort=score+desc&wt=json

The response contains hits[0]._source.iacontent (JSON string) with:
  brochures.brochuredetails: [{brochureVersionID, brochureName, dateSubmitted}, ...]

dateSubmitted is in M/D/YYYY format.

Download URL: https://files.adviserinfo.sec.gov/IAPD/Content/Common/crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={id}
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

_IAPD_FIRM_URL = (
    "https://api.adviserinfo.sec.gov/search/firm/{crd}"
    "?hl=true&nrows=12&query=test&r=25&sort=score+desc&wt=json"
)
_BROCHURE_DOWNLOAD_URL = (
    "https://files.adviserinfo.sec.gov/IAPD/Content/Common/"
    "crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={version_id}"
)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://adviserinfo.sec.gov/",
    "Accept": "application/pdf,*/*",
}
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
            for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    submit_date = datetime.strptime(b["date_submitted"], fmt).date()
                    break
                except ValueError:
                    continue

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
# Brochure listing via IAPD firm search API
# ---------------------------------------------------------------------------

def _get_brochure_listing(crd: int) -> list[dict]:
    """
    Return list of Part 2 brochures for crd:
    [{"version_id": int, "name": str|None, "date_submitted": str|None}, ...]
    """
    try:
        return _listing_from_iapd_api(crd)
    except Exception as exc:
        log.warning("_get_brochure_listing: IAPD API failed for CRD %d: %s", crd, exc)
        return []


def _listing_from_iapd_api(crd: int) -> list[dict]:
    """
    Query the IAPD firm search API and extract Part 2 brochure details.

    Response shape:
      hits.hits[0]._source.iacontent  (JSON string)
        → brochures.brochuredetails
          → [{brochureVersionID, brochureName, dateSubmitted}, ...]

    dateSubmitted is in M/D/YYYY format.
    """
    url = _IAPD_FIRM_URL.format(crd=crd)
    resp = _http_get(url)
    data = resp.json()

    hits = data.get("hits", {}).get("hits", [])
    if not hits:
        log.info("_listing_from_iapd_api: no hits for CRD %d", crd)
        return []

    iacontent_raw = hits[0].get("_source", {}).get("iacontent")
    if not iacontent_raw:
        log.info("_listing_from_iapd_api: no iacontent for CRD %d", crd)
        return []

    try:
        iacontent = json.loads(iacontent_raw)
    except (json.JSONDecodeError, TypeError) as exc:
        log.warning("_listing_from_iapd_api: failed to parse iacontent for CRD %d: %s", crd, exc)
        return []

    details = iacontent.get("brochures", {}).get("brochuredetails") or []

    brochures: list[dict] = []
    for item in details:
        version_id = item.get("brochureVersionID")
        if not version_id:
            continue
        try:
            version_id = int(version_id)
        except (TypeError, ValueError):
            continue

        brochures.append({
            "version_id": version_id,
            "name": item.get("brochureName") or None,
            "date_submitted": item.get("dateSubmitted") or None,
        })

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
