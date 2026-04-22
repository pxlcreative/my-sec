"""
Tests for the bulk match API.

POST /api/match/bulk   — sync path (≤100 records)
GET  /api/match/jobs/{id}
"""
from __future__ import annotations



VALID_RECORDS = [
    {"name": "Acme Capital Management", "city": "New York", "state": "NY"},
    {"name": "Blue Ridge Advisors",      "city": "Atlanta",  "state": "GA"},
    {"name": "Nonexistent Firm XYZ",     "city": "Nowhere",  "state": "ZZ"},
]


class TestBulkMatchSync:
    def test_sync_path_returns_results(self, client, seeded_firms):
        r = client.post("/api/match/bulk", json={"records": VALID_RECORDS})
        assert r.status_code == 200
        body = r.json()
        assert "results" in body
        assert "stats" in body
        assert len(body["results"]) == 3

    def test_stats_keys_present(self, client, seeded_firms):
        r = client.post("/api/match/bulk", json={"records": VALID_RECORDS})
        stats = r.json()["stats"]
        assert "total" in stats
        assert "confirmed" in stats
        assert "probable" in stats
        assert "possible" in stats
        assert "no_match" in stats
        assert stats["total"] == 3

    def test_result_shape(self, client, seeded_firms):
        r = client.post("/api/match/bulk", json={"records": VALID_RECORDS[:1]})
        result = r.json()["results"][0]
        assert "input_name" in result
        assert "best_score" in result
        assert "best_status" in result
        assert "candidates" in result

    def test_empty_records_returns_zero_total(self, client):
        r = client.post("/api/match/bulk", json={"records": []})
        assert r.status_code == 200
        body = r.json()
        assert body["stats"]["total"] == 0
        assert body["results"] == []

    def test_no_match_for_nonexistent_firm(self, client, seeded_firms):
        r = client.post(
            "/api/match/bulk",
            json={"records": [{"name": "ZZZ Completely Fake LLC XYZ"}]},
        )
        assert r.status_code == 200
        result = r.json()["results"][0]
        assert result["best_status"] == "no_match"

    def test_custom_min_score(self, client, seeded_firms):
        r = client.post(
            "/api/match/bulk",
            json={
                "records": VALID_RECORDS,
                "options": {"min_score": 95},
            },
        )
        assert r.status_code == 200

    def test_exceeds_max_records_returns_400(self, client):
        records = [{"name": f"Firm {i}"} for i in range(10_001)]
        r = client.post("/api/match/bulk", json={"records": records})
        assert r.status_code == 400

    def test_sync_path_used_for_100_records(self, client, seeded_firms):
        """Exactly 100 records should use the sync path (not async)."""
        records = [{"name": f"Firm {i}", "state": "NY"} for i in range(100)]
        r = client.post("/api/match/bulk", json={"records": records})
        assert r.status_code == 200
        # Sync response has results/stats, not job_id
        body = r.json()
        assert "results" in body
        assert "job_id" not in body


class TestMatchJobNotFound:
    def test_404_for_missing_job(self, client):
        r = client.get("/api/match/jobs/999999")
        assert r.status_code == 404
        assert "detail" in r.json()
