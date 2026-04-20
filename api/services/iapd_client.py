"""
IAPD (Investment Adviser Public Disclosure) API client.

Fetches live ADV data from SEC's EFTS search API and maps it to our
canonical Firm schema field names.
"""
import logging
import time
from datetime import date

import requests

log = logging.getLogger(__name__)

_EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
_RATE_LIMIT_SLEEP = 0.5      # seconds between every call
_RETRY_BACKOFF_BASE = 2.0    # exponential base for 429/503 retries
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
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return date.fromisoformat(v) if fmt == "%Y-%m-%d" else __import__("datetime").datetime.strptime(v, fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


# ---------------------------------------------------------------------------
# 1. fetch_firm
# ---------------------------------------------------------------------------

def fetch_firm(crd_number: int) -> dict:
    """
    Fetch raw IAPD ADV JSON for *crd_number* from SEC EFTS.

    Returns the _source dict from the first hit.
    Raises ValueError if no results; RuntimeError after exhausted retries.
    Rate-limits at 0.5 s per call; retries on 429/503 with exponential backoff.
    """
    params = {
        "query": f"Info.FirmCrdNb:{crd_number}",
        "forms": "ADV",
    }

    for attempt in range(1, _MAX_RETRIES + 1):
        time.sleep(_RATE_LIMIT_SLEEP)
        try:
            resp = requests.get(_EFTS_URL, params=params, headers=_HEADERS, timeout=20)

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
            return hits[0]["_source"]

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

def extract_firm_fields(raw: dict) -> dict:
    """
    Map IAPD JSON paths to canonical Firm schema column names.

    Only returns keys with non-None values so callers can do a clean
    `Firm.__dict__.update(fields)` or comparison.
    """
    aum_total = _int_or_none(_get(raw, "FormInfo", "Part1A", "Item5F", "Q5F2C"))
    aum_disc  = _int_or_none(_get(raw, "FormInfo", "Part1A", "Item5F", "Q5F2A"))
    aum_ndisc = _int_or_none(_get(raw, "FormInfo", "Part1A", "Item5F", "Q5F2B"))

    # AUM values are sometimes stored in millions; convert if suspiciously small
    # (keep raw — caller decides; we return whatever IAPD gives)

    fields = {
        "crd_number":             _int_or_none(_get(raw, "Info", "FirmCrdNb")),
        "legal_name":             _get(raw, "Info", "Nm"),
        "business_name":          _get(raw, "Info", "BusNm"),
        "registration_status":    _get(raw, "Info", "RegistrationStatus"),
        "sec_number":             _get(raw, "Info", "SECNumber"),
        "last_filing_date":       _date_or_none(_get(raw, "Info", "LastADVFilingDate")),
        "aum_total":              aum_total,
        "aum_discretionary":      aum_disc,
        "aum_non_discretionary":  aum_ndisc,
        "num_accounts":           _int_or_none(
                                      _get(raw, "FormInfo", "Part1A", "Item5D", "Q5D2")
                                  ),
        "num_employees":          _int_or_none(
                                      _get(raw, "FormInfo", "Part1A", "Item5B", "Q5B1")
                                  ),
        "main_street1":           _get(raw, "FormInfo", "Part1", "Item1", "MainAddress", "Street1"),
        "main_street2":           _get(raw, "FormInfo", "Part1", "Item1", "MainAddress", "Street2"),
        "main_city":              _get(raw, "FormInfo", "Part1", "Item1", "MainAddress", "City"),
        "main_state":             _get(raw, "FormInfo", "Part1", "Item1", "MainAddress", "State"),
        "main_zip":               _get(raw, "FormInfo", "Part1", "Item1", "MainAddress", "ZipCode"),
        "main_country":           _get(raw, "FormInfo", "Part1", "Item1", "MainAddress", "Country"),
        "phone":                  _get(raw, "FormInfo", "Part1", "Item1", "PhoneNumber"),
        "website":                _get(raw, "FormInfo", "Part1", "Item1", "WebAddress"),
        "org_type":               _get(raw, "FormInfo", "Part1", "Item1", "OrgType"),
        "fiscal_year_end":        _get(raw, "FormInfo", "Part1A", "Item1", "FiscalYearEnd"),
    }

    # Drop None values so callers can distinguish "not present in IAPD" from "explicitly null"
    return {k: v for k, v in fields.items() if v is not None}
