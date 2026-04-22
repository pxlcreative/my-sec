"""
Unit tests for services.iapd_client.

Covers:
- fetch_firm happy path (single attempt)
- retry on 429 / 503 with exponential backoff
- retry on generic exception, exhaustion → RuntimeError
- ValueError for empty hits (NOT retried)
- extract_firm_fields for all four response shapes:
    - normal Registered response
    - Withdrawn status
    - EDGAR-format (no registrationStatus) → field omitted
    - Approved maps to Registered
- Date parsing fallback across SEC's formats
- ZIP+4 truncation to 5 digits
"""
from __future__ import annotations

import json
from unittest.mock import Mock, patch

import pytest


# ── fetch_firm ──────────────────────────────────────────────────────────────

def _fake_response(status_code: int, json_body: dict | None = None):
    resp = Mock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.raise_for_status = Mock()
    if status_code >= 400 and status_code not in (429, 503):
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    return resp


def _make_hits_payload(iacontent: dict) -> dict:
    """Wrap an iacontent dict in the envelope fetch_firm expects."""
    return {"hits": {"hits": [{"_source": {"iacontent": json.dumps(iacontent)}}]}}


class TestFetchFirm:
    @patch("services.iapd_client.requests.get")
    @patch("services.iapd_client.time.sleep")
    def test_happy_path_returns_iacontent(self, _sleep, mock_get, iapd_fixtures):
        mock_get.return_value = _fake_response(
            200, _make_hits_payload(iapd_fixtures["registered_firm"])
        )
        from services.iapd_client import fetch_firm

        result = fetch_firm(100001)
        assert result["basicInformation"]["firmId"] == 100001
        assert mock_get.call_count == 1

    @patch("services.iapd_client.requests.get")
    @patch("services.iapd_client.time.sleep")
    def test_retries_on_429(self, _sleep, mock_get, iapd_fixtures):
        mock_get.side_effect = [
            _fake_response(429),
            _fake_response(200, _make_hits_payload(iapd_fixtures["registered_firm"])),
        ]
        from services.iapd_client import fetch_firm

        result = fetch_firm(100001)
        assert result["basicInformation"]["firmId"] == 100001
        assert mock_get.call_count == 2

    @patch("services.iapd_client.requests.get")
    @patch("services.iapd_client.time.sleep")
    def test_retries_on_503(self, _sleep, mock_get, iapd_fixtures):
        mock_get.side_effect = [
            _fake_response(503),
            _fake_response(503),
            _fake_response(200, _make_hits_payload(iapd_fixtures["registered_firm"])),
        ]
        from services.iapd_client import fetch_firm

        result = fetch_firm(100001)
        assert result["basicInformation"]["firmId"] == 100001
        assert mock_get.call_count == 3

    @patch("services.iapd_client.requests.get")
    @patch("services.iapd_client.time.sleep")
    def test_raises_runtime_error_after_exhausted_retries(self, _sleep, mock_get):
        # All 3 attempts throw; after last, fetch_firm raises RuntimeError.
        mock_get.side_effect = Exception("connection reset")
        from services.iapd_client import fetch_firm

        with pytest.raises(RuntimeError, match="failed after 3 attempts"):
            fetch_firm(100001)

        assert mock_get.call_count == 3

    @patch("services.iapd_client.requests.get")
    @patch("services.iapd_client.time.sleep")
    def test_value_error_on_empty_hits_not_retried(self, _sleep, mock_get):
        mock_get.return_value = _fake_response(200, {"hits": {"hits": []}})
        from services.iapd_client import fetch_firm

        with pytest.raises(ValueError, match="No IAPD results"):
            fetch_firm(100001)

        # Exactly 1 call — ValueError short-circuits retry.
        assert mock_get.call_count == 1

    @patch("services.iapd_client.requests.get")
    @patch("services.iapd_client.time.sleep")
    def test_user_agent_header_sent(self, _sleep, mock_get, iapd_fixtures):
        mock_get.return_value = _fake_response(
            200, _make_hits_payload(iapd_fixtures["registered_firm"])
        )
        from services.iapd_client import fetch_firm

        fetch_firm(100001)
        _, kwargs = mock_get.call_args
        assert "User-Agent" in kwargs["headers"]
        assert "MySEC" in kwargs["headers"]["User-Agent"]


# ── extract_firm_fields ─────────────────────────────────────────────────────

class TestExtractFirmFields:
    def test_registered_firm_all_fields(self, iapd_fixtures):
        from services.iapd_client import extract_firm_fields
        fields = extract_firm_fields(iapd_fixtures["registered_firm"])

        assert fields["crd_number"] == 100001
        assert fields["legal_name"] == "Acme Capital Management LLC"
        assert fields["sec_number"] == "801-12345"
        assert fields["registration_status"] == "Registered"
        assert fields["main_state"] == "NY"
        assert fields["main_zip"] == "10001"  # truncated from "10001-1234"
        assert fields["main_country"] == "USA"
        assert fields["last_filing_date"] == "2025-03-15"

    def test_withdrawn_firm(self, iapd_fixtures):
        from services.iapd_client import extract_firm_fields
        fields = extract_firm_fields(iapd_fixtures["withdrawn_firm"])
        assert fields["registration_status"] == "Withdrawn"

    def test_edgar_format_has_no_registration_status(self, iapd_fixtures):
        """EDGAR-format responses omit registrationStatus; extract_firm_fields
        leaves the key off entirely so firm_refresh_service can infer Inactive."""
        from services.iapd_client import extract_firm_fields
        fields = extract_firm_fields(iapd_fixtures["edgar_old_firm"])
        assert "registration_status" not in fields

    def test_active_status_maps_to_registered(self, iapd_fixtures):
        from services.iapd_client import extract_firm_fields
        fields = extract_firm_fields(iapd_fixtures["aum_change_firm"])
        assert fields["registration_status"] == "Registered"

    def test_only_non_none_fields_returned(self):
        """Fields with None values should be stripped."""
        from services.iapd_client import extract_firm_fields
        fields = extract_firm_fields({
            "basicInformation": {"firmId": 999, "firmName": "Minimal"},
            "iaFirmAddressDetails": {"officeAddress": {}},
            "registrationStatus": [],
        })
        assert fields == {"crd_number": 999, "legal_name": "Minimal"}

    def test_empty_iacontent_returns_empty_dict(self):
        from services.iapd_client import extract_firm_fields
        assert extract_firm_fields({}) == {}

    def test_zip_plus_four_truncates_to_5(self):
        from services.iapd_client import extract_firm_fields
        fields = extract_firm_fields({
            "basicInformation": {"firmId": 1, "firmName": "X"},
            "iaFirmAddressDetails": {"officeAddress": {"postalCode": "94105-1234"}},
            "registrationStatus": [],
        })
        assert fields["main_zip"] == "94105"

    def test_short_zip_passes_through(self):
        from services.iapd_client import extract_firm_fields
        fields = extract_firm_fields({
            "basicInformation": {"firmId": 1, "firmName": "X"},
            "iaFirmAddressDetails": {"officeAddress": {"postalCode": "94105"}},
            "registrationStatus": [],
        })
        assert fields["main_zip"] == "94105"


# ── date parsing ────────────────────────────────────────────────────────────

class TestDateOrNone:
    @pytest.mark.parametrize("value,expected", [
        ("2025-03-15", "2025-03-15"),
        ("03/15/2025", "2025-03-15"),
        ("20250315", "2025-03-15"),
        ("03/15/2025 02:30:00 PM", "2025-03-15"),
    ])
    def test_parses_known_formats(self, value, expected):
        from services.iapd_client import _date_or_none
        result = _date_or_none(value)
        assert result is not None
        assert result.isoformat() == expected

    @pytest.mark.parametrize("value", ["", None, "garbage", "13/45/2025"])
    def test_returns_none_for_invalid(self, value):
        from services.iapd_client import _date_or_none
        assert _date_or_none(value) is None
