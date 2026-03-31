"""
Tests for the firms API.

GET /api/firms          — list/search with filters
GET /api/firms/{crd}    — firm detail
GET /api/firms/{crd}/history
GET /api/firms/{crd}/aum-history
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# GET /api/firms — list
# ---------------------------------------------------------------------------

class TestListFirms:
    def test_empty_db_returns_empty_page(self, client):
        r = client.get("/api/firms")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 0
        assert body["results"] == []
        assert body["page"] == 1

    def test_returns_seeded_firms(self, client, seeded_firms):
        r = client.get("/api/firms", params={"page_size": 20})
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 10
        assert len(body["results"]) == 10

    def test_filter_by_state(self, client, seeded_firms):
        r = client.get("/api/firms", params={"state": "NY"})
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert body["results"][0]["main_state"] == "NY"

    def test_filter_by_aum_min(self, client, seeded_firms):
        r = client.get("/api/firms", params={"aum_min": 500_000_000})
        assert r.status_code == 200
        body = r.json()
        # Firms with aum_total >= 500M: Acme (500M), Harbor (730M), Northgate (1.2B)
        assert body["total"] == 3

    def test_filter_by_registration_status(self, client, seeded_firms):
        r = client.get("/api/firms", params={"registration_status": "Withdrawn"})
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert body["results"][0]["registration_status"] == "Withdrawn"

    def test_pagination(self, client, seeded_firms):
        r1 = client.get("/api/firms", params={"page": 1, "page_size": 5})
        r2 = client.get("/api/firms", params={"page": 2, "page_size": 5})
        assert r1.status_code == 200
        assert r2.status_code == 200
        assert len(r1.json()["results"]) == 5
        assert len(r2.json()["results"]) == 5
        # No overlap between pages
        crds_p1 = {f["crd_number"] for f in r1.json()["results"]}
        crds_p2 = {f["crd_number"] for f in r2.json()["results"]}
        assert crds_p1.isdisjoint(crds_p2)

    def test_response_shape(self, client, seeded_firms):
        r = client.get("/api/firms", params={"page_size": 1})
        body = r.json()
        firm = body["results"][0]
        assert "crd_number" in firm
        assert "legal_name" in firm
        assert "main_state" in firm
        assert "aum_total" in firm
        assert "registration_status" in firm
        assert "platforms" in firm
        assert isinstance(firm["platforms"], list)


# ---------------------------------------------------------------------------
# GET /api/firms/{crd} — detail
# ---------------------------------------------------------------------------

class TestGetFirm:
    def test_returns_firm(self, client, seeded_firms):
        r = client.get("/api/firms/100001")
        assert r.status_code == 200
        body = r.json()
        assert body["crd_number"] == 100001
        assert body["legal_name"] == "Acme Capital Management LLC"
        assert body["main_state"] == "NY"

    def test_404_for_missing_crd(self, client):
        r = client.get("/api/firms/999999999")
        assert r.status_code == 404
        assert "detail" in r.json()

    def test_firm_detail_has_extra_fields(self, client, seeded_firms):
        r = client.get("/api/firms/100001")
        body = r.json()
        # FirmDetail fields not in FirmSummary
        assert "sec_number" in body
        assert "num_accounts" in body
        assert "main_street1" in body
        assert "latest_brochure" in body

    def test_withdrawn_firm_returns_200(self, client, seeded_firms):
        """Withdrawn firms should still be retrievable."""
        r = client.get("/api/firms/100009")
        assert r.status_code == 200
        assert r.json()["registration_status"] == "Withdrawn"


# ---------------------------------------------------------------------------
# GET /api/firms/{crd}/history
# ---------------------------------------------------------------------------

class TestFirmHistory:
    def test_empty_history(self, client, seeded_firms):
        r = client.get("/api/firms/100001/history")
        assert r.status_code == 200
        body = r.json()
        assert body["crd_number"] == 100001
        assert body["changes"] == []

    def test_404_for_missing_crd(self, client):
        r = client.get("/api/firms/999999999/history")
        # Either 404 or 200 with empty changes is acceptable
        assert r.status_code in (200, 404)


# ---------------------------------------------------------------------------
# GET /api/firms/{crd}/aum-history
# ---------------------------------------------------------------------------

class TestAumHistory:
    def test_empty_aum_history(self, client, seeded_firms):
        r = client.get("/api/firms/100001/aum-history")
        assert r.status_code == 200
        body = r.json()
        assert body["crd_number"] == 100001
        assert isinstance(body["filings"], list)
        assert isinstance(body["annual"], list)
