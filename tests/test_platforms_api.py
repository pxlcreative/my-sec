"""
Route-level tests for /api/platforms and /api/firms/{crd}/platforms.

Covers:
- GET  /api/platforms (list)
- POST /api/platforms (create)
- PATCH /api/platforms/{id}
- DELETE /api/platforms/{id}
- GET  /api/platforms/{id}/firms (paginated firm list)
- GET  /api/firms/{crd}/platforms
- POST /api/firms/{crd}/platforms (add single tag)
- PUT  /api/firms/{crd}/platforms (replace tags)
- DELETE /api/firms/{crd}/platforms/{platform_id}
- POST /api/match/bulk-tag
"""
from __future__ import annotations



# ── CRUD: /api/platforms ────────────────────────────────────────────────────

class TestPlatformCrud:
    def test_list_empty(self, client):
        r = client.get("/api/platforms")
        assert r.status_code == 200
        assert r.json() == []

    def test_create_returns_201(self, client):
        r = client.post("/api/platforms", json={
            "name": "TestPlatform", "description": "x", "save_brochures": False,
        })
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["name"] == "TestPlatform"
        assert body["save_brochures"] is False
        assert "id" in body

    def test_list_after_create(self, client):
        client.post("/api/platforms", json={
            "name": "A", "description": None, "save_brochures": False,
        })
        client.post("/api/platforms", json={
            "name": "B", "description": None, "save_brochures": True,
        })
        r = client.get("/api/platforms")
        assert r.status_code == 200
        names = [p["name"] for p in r.json()]
        assert set(names) >= {"A", "B"}

    def test_create_rejects_empty_name(self, client):
        r = client.post("/api/platforms", json={
            "name": "", "description": None, "save_brochures": False,
        })
        assert r.status_code == 422

    def test_patch_toggles_save_brochures(self, client):
        created = client.post("/api/platforms", json={
            "name": "Toggle", "description": None, "save_brochures": False,
        }).json()

        r = client.patch(f"/api/platforms/{created['id']}", json={
            "save_brochures": True,
        })
        assert r.status_code == 200
        assert r.json()["save_brochures"] is True

    def test_delete_removes_platform(self, client):
        created = client.post("/api/platforms", json={
            "name": "Gone", "description": None, "save_brochures": False,
        }).json()

        r = client.delete(f"/api/platforms/{created['id']}")
        assert r.status_code == 204

        # Subsequent fetch should not include it.
        listing = client.get("/api/platforms").json()
        assert not any(p["id"] == created["id"] for p in listing)

    def test_delete_missing_platform_returns_404(self, client):
        r = client.delete("/api/platforms/999999")
        assert r.status_code == 404


# ── /api/platforms/{id}/firms ───────────────────────────────────────────────

class TestPlatformFirms:
    def test_empty_platform_has_no_firms(self, client, seeded_platform):
        r = client.get(f"/api/platforms/{seeded_platform.id}/firms")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 0
        assert body["results"] == []

    def test_404_for_missing_platform(self, client):
        r = client.get("/api/platforms/999999/firms")
        assert r.status_code == 404


# ── /api/firms/{crd}/platforms ──────────────────────────────────────────────

class TestFirmPlatformTags:
    def test_untagged_firm_returns_empty(self, client, seeded_firms, seeded_platform):
        r = client.get(f"/api/firms/{seeded_firms[0]}/platforms")
        assert r.status_code == 200
        assert r.json() == []

    def test_add_tag_then_list(self, client, seeded_firms, seeded_platform):
        crd = seeded_firms[0]
        r = client.post(f"/api/firms/{crd}/platforms", json={
            "platform_id": seeded_platform.id,
        })
        assert r.status_code == 201
        tags = r.json()
        assert len(tags) == 1
        assert tags[0]["platform_id"] == seeded_platform.id
        assert tags[0]["platform_name"] == seeded_platform.name

    def test_put_replaces_tag_set(self, client, seeded_firms, db):
        """PUT with [] removes existing tags."""
        from models.platform import PlatformDefinition

        crd = seeded_firms[0]
        p1 = PlatformDefinition(name="p1", description=None)
        p2 = PlatformDefinition(name="p2", description=None)
        db.add_all([p1, p2])
        db.flush()

        client.post(f"/api/firms/{crd}/platforms", json={"platform_id": p1.id})
        client.post(f"/api/firms/{crd}/platforms", json={"platform_id": p2.id})

        # Replace with only p1.
        r = client.put(f"/api/firms/{crd}/platforms", json={"platform_ids": [p1.id]})
        assert r.status_code == 200
        ids = {t["platform_id"] for t in r.json()}
        assert ids == {p1.id}

    def test_delete_removes_single_tag(self, client, seeded_firms, seeded_platform):
        crd = seeded_firms[0]
        client.post(f"/api/firms/{crd}/platforms", json={
            "platform_id": seeded_platform.id,
        })

        r = client.delete(f"/api/firms/{crd}/platforms/{seeded_platform.id}")
        assert r.status_code == 204

        r2 = client.get(f"/api/firms/{crd}/platforms")
        assert r2.json() == []


# ── /api/match/bulk-tag ─────────────────────────────────────────────────────

class TestBulkTag:
    def test_inserts_new_tags(self, client, seeded_firms, seeded_platform):
        records = [
            {"crd_number": seeded_firms[0], "platform_id": seeded_platform.id},
            {"crd_number": seeded_firms[1], "platform_id": seeded_platform.id},
        ]
        r = client.post("/api/match/bulk-tag", json={"records": records})
        assert r.status_code == 200
        body = r.json()
        assert body["inserted"] == 2
        assert body["skipped"] == 0

    def test_skips_duplicates(self, client, seeded_firms, seeded_platform):
        records = [
            {"crd_number": seeded_firms[0], "platform_id": seeded_platform.id},
        ]
        client.post("/api/match/bulk-tag", json={"records": records})
        r = client.post("/api/match/bulk-tag", json={"records": records})
        assert r.status_code == 200
        assert r.json()["skipped"] == 1

    def test_empty_records_returns_422(self, client):
        r = client.post("/api/match/bulk-tag", json={"records": []})
        assert r.status_code == 422
