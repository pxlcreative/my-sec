"""
IAPD (Investment Adviser Public Disclosure) API client.

Fetches live adviser data from the SEC's IAPD public disclosure API
(api.adviserinfo.sec.gov) and maps it to our canonical Firm schema.

NOTE: The public IAPD API returns a summary record (iacontent) with basic
identification, address, and registration-status fields. It does NOT expose
Form ADV Part 1A checkbox data (client types, compensation types, strategies,
affiliations) — those are only in the PDF/HTML rendered form.
"""
import json
import logging
import time
from datetime import date

import requests

log = logging.getLogger(__name__)

_IAPD_BASE = "https://api.adviserinfo.sec.gov/search/firm"
_RATE_LIMIT_SLEEP = 2.0
_RETRY_BACKOFF_BASE = 2.0
_MAX_RETRIES = 3
_HEADERS = {"User-Agent": "MySEC/1.0 (private research tool; dan@pxlcreative.com)"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get(d: dict, *keys):
    """Safely traverse nested dicts. Returns None if any key is missing."""
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def _int_or_none(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _date_or_none(v) -> date | None:
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%Y %I:%M:%S %p", "%Y%m%d"):
        try:
            import datetime as _dt
            return _dt.datetime.strptime(v, fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


# ---------------------------------------------------------------------------
# 1. fetch_firm
# ---------------------------------------------------------------------------

def fetch_firm(crd_number: int) -> dict:
    """
    Fetch the IAPD public disclosure record for *crd_number*.

    Calls api.adviserinfo.sec.gov/search/firm/{crd} and returns the parsed
    iacontent dict. Raises ValueError if no record found; RuntimeError after
    exhausted retries.
    """
    url = f"{_IAPD_BASE}/{crd_number}"

    for attempt in range(1, _MAX_RETRIES + 1):
        time.sleep(_RATE_LIMIT_SLEEP)
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=20)

            if resp.status_code in (429, 503):
                wait = _RETRY_BACKOFF_BASE ** attempt
                log.warning(
                    "fetch_firm(%d): HTTP %d on attempt %d — sleeping %.1fs",
                    crd_number, resp.status_code, attempt, wait,
                )
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            hits = _get(data, "hits", "hits") or []
            if not hits:
                raise ValueError(f"No IAPD results for CRD {crd_number}")

            source = hits[0].get("_source", {})
            raw_content = source.get("iacontent", "{}")
            iacontent = json.loads(raw_content) if isinstance(raw_content, str) else raw_content
            return iacontent

        except ValueError:
            raise
        except Exception as exc:
            if attempt == _MAX_RETRIES:
                raise RuntimeError(
                    f"fetch_firm({crd_number}) failed after {_MAX_RETRIES} attempts: {exc}"
                ) from exc
            wait = _RETRY_BACKOFF_BASE ** attempt
            log.warning(
                "fetch_firm(%d): error on attempt %d (%s) — retrying in %.1fs",
                crd_number, attempt, exc, wait,
            )
            time.sleep(wait)

    raise RuntimeError(f"fetch_firm({crd_number}): exhausted retries")


# ---------------------------------------------------------------------------
# 2. extract_firm_fields
# ---------------------------------------------------------------------------

_STATUS_MAP = {
    "approved": "Registered",
    "active": "Registered",
    "withdrawn": "Withdrawn",
    "not approved": "Withdrawn",
    "inactive": "Inactive",
}


def extract_firm_fields(raw: dict) -> dict:
    """
    Map IAPD iacontent JSON to canonical Firm schema column names.

    The public IAPD API (api.adviserinfo.sec.gov) returns: firm name, SEC
    number, last filing date, registration status, and office address.
    AUM and employee counts are NOT available from this API — they come
    from the monthly advFilingData CSV sync.

    Only returns keys with non-None values.
    """
    basic = raw.get("basicInformation") or {}
    address = (_get(raw, "iaFirmAddressDetails", "officeAddress") or {})
    reg_entries = raw.get("registrationStatus") or []

    # Derive registration status from the SEC jurisdiction entry
    status: str | None = None
    sec_entry = next(
        (r for r in reg_entries if isinstance(r, dict) and r.get("secJurisdiction") == "SEC"),
        reg_entries[0] if reg_entries else None,
    )
    if sec_entry:
        raw_status = (sec_entry.get("status") or "").lower()
        status = _STATUS_MAP.get(raw_status)

    zip_raw = address.get("postalCode") or ""
    zip5 = zip_raw[:5] if zip_raw else None

    filing_date = _date_or_none(basic.get("advFilingDate"))

    fields = {
        "crd_number":          _int_or_none(basic.get("firmId")),
        "legal_name":          basic.get("firmName"),
        "sec_number":          basic.get("iaSECNumber"),
        # Store as ISO string so the dict remains JSON-serialisable for JSONB snapshots.
        # firm_refresh_service converts this back to date() when setting the ORM column.
        "last_filing_date":    filing_date.isoformat() if filing_date else None,
        "registration_status": status,
        "main_street1":        address.get("street1"),
        "main_city":           address.get("city"),
        "main_state":          address.get("state"),
        "main_zip":            zip5,
        "main_country":        address.get("country"),
    }

    return {k: v for k, v in fields.items() if v is not None}
