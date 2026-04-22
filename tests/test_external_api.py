"""
Tests for the external API (Bearer token auth + rate limiting).

GET /api/external/firms/{crd}/brochures
GET /api/external/firms/{crd}/brochure
GET /api/external/platforms/{id}/firms
POST /api/external/match/bulk
"""
from __future__ import annotations



# ---------------------------------------------------------------------------
# Auth guard — all external routes require Authorization: Bearer <key>
# ---------------------------------------------------------------------------

class TestAuthGuard:
    EXTERNAL_ENDPOINTS = [
        ("GET",  "/api/external/firms/100001/brochures"),
        ("GET",  "/api/external/firms/100001/brochure"),
        ("GET",  "/api/external/platforms/1/firms"),
        ("POST", "/api/external/match/bulk"),
    ]

    def test_missing_auth_header_returns_401(self, client):
        for method, path in self.EXTERNAL_ENDPOINTS:
            r = client.request(method, path)
            assert r.status_code in (401, 403), (
                f"{method} {path} should return 401/403 without auth, got {r.status_code}"
            )

    def test_invalid_token_returns_401(self, client):
        bad_header = {"Authorization": "Bearer not-a-real-key-abcdef1234567890"}
        for method, path in self.EXTERNAL_ENDPOINTS:
            r = client.request(method, path, headers=bad_header)
            assert r.status_code in (401, 403), (
                f"{method} {path} should reject invalid token, got {r.status_code}"
            )

    def test_valid_token_passes_auth_guard(self, client, seeded_firms, api_key_header):
        """A valid key should get past auth (may still 404 on missing data)."""
        r = client.get("/api/external/firms/100001/brochures", headers=api_key_header)
        # 200 (no brochures = []) or 404 (firm detail) — either is fine; 401 is not
        assert r.status_code != 401
        assert r.status_code != 403


# ---------------------------------------------------------------------------
# Brochure list endpoint
# ---------------------------------------------------------------------------

class TestExternalBrochureList:
    def test_brochure_list_empty_for_firm_without_brochures(self, client, seeded_firms, api_key_header):
        r = client.get("/api/external/firms/100001/brochures", headers=api_key_header)
        assert r.status_code == 200
        assert r.json() == []

    def test_brochure_list_404_for_missing_firm(self, client, api_key_header):
        r = client.get("/api/external/firms/999999999/brochures", headers=api_key_header)
        assert r.status_code == 404
        assert "detail" in r.json()


# ---------------------------------------------------------------------------
# Bulk match via external API
# ---------------------------------------------------------------------------

class TestExternalBulkMatch:
    def test_bulk_match_with_valid_key(self, client, seeded_firms, api_key_header):
        payload = {"records": [{"name": "Acme Capital Management", "state": "NY"}]}
        r = client.post("/api/external/match/bulk", json=payload, headers=api_key_header)
        assert r.status_code == 200
        body = r.json()
        assert "results" in body
        assert "stats" in body

    def test_bulk_match_empty_records(self, client, api_key_header):
        payload = {"records": []}
        r = client.post("/api/external/match/bulk", json=payload, headers=api_key_header)
        assert r.status_code == 200
        assert r.json()["stats"]["total"] == 0


# ---------------------------------------------------------------------------
# Platform firms via external API
# ---------------------------------------------------------------------------

class TestExternalPlatformFirms:
    def test_platform_firms_returns_list(self, client, seeded_platform, api_key_header):
        r = client.get(f"/api/external/platforms/{seeded_platform.id}/firms", headers=api_key_header)
        assert r.status_code == 200
        # No firms tagged yet — empty paginated result
        body = r.json()
        assert "results" in body or isinstance(body, list)

    def test_platform_firms_404_for_missing_platform(self, client, api_key_header):
        r = client.get("/api/external/platforms/999999/firms", headers=api_key_header)
        assert r.status_code == 404
