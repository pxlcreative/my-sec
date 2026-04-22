"""
Tests for the export API.

POST /api/export/firms          — sync CSV and XLSX export
POST /api/export/templates      — save preset
GET  /api/export/templates      — list presets
"""
from __future__ import annotations

import csv
import io


DEFAULT_FIELDS = ["crd_number", "legal_name", "main_state", "aum_total", "registration_status"]

BASE_REQUEST = {
    "format": "csv",
    "filter": {},
    "field_selection": DEFAULT_FIELDS,
}


class TestCsvExport:
    def test_empty_db_returns_empty_csv(self, client):
        r = client.post("/api/export/firms", json=BASE_REQUEST)
        assert r.status_code == 200
        assert "text/csv" in r.headers["content-type"]
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        assert rows == []

    def test_csv_has_header_row(self, client):
        r = client.post("/api/export/firms", json=BASE_REQUEST)
        assert r.status_code == 200
        lines = r.text.strip().splitlines()
        # At minimum the header should be present
        assert len(lines) >= 1
        header = lines[0].split(",")
        assert "CRD_NUMBER" in header
        assert "LEGAL_NAME" in header

    def test_csv_contains_seeded_firms(self, client, seeded_firms):
        r = client.post("/api/export/firms", json=BASE_REQUEST)
        assert r.status_code == 200
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        assert len(rows) == 10

    def test_filter_by_state(self, client, seeded_firms):
        req = {**BASE_REQUEST, "filter": {"states": ["NY"]}}
        r = client.post("/api/export/firms", json=req)
        assert r.status_code == 200
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["STATE"] == "NY"

    def test_filter_by_registration_status(self, client, seeded_firms):
        req = {**BASE_REQUEST, "filter": {"registration_status": "Withdrawn"}}
        r = client.post("/api/export/firms", json=req)
        assert r.status_code == 200
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        assert len(rows) == 1

    def test_content_disposition_header(self, client):
        r = client.post("/api/export/firms", json=BASE_REQUEST)
        assert r.status_code == 200
        assert "attachment" in r.headers.get("content-disposition", "")
        assert ".csv" in r.headers.get("content-disposition", "")


class TestJsonExport:
    def test_json_export_returns_array(self, client, seeded_firms):
        req = {**BASE_REQUEST, "format": "json"}
        r = client.post("/api/export/firms", json=req)
        assert r.status_code == 200
        assert "application/json" in r.headers["content-type"]
        data = r.json()
        assert isinstance(data, list)
        assert len(data) == 10

    def test_json_empty_returns_empty_array(self, client):
        req = {**BASE_REQUEST, "format": "json"}
        r = client.post("/api/export/firms", json=req)
        assert r.status_code == 200
        assert r.json() == []


class TestXlsxExport:
    def test_xlsx_export_returns_bytes(self, client, seeded_firms):
        req = {**BASE_REQUEST, "format": "xlsx"}
        r = client.post("/api/export/firms", json=req)
        assert r.status_code == 200
        assert "spreadsheet" in r.headers["content-type"] or "octet-stream" in r.headers["content-type"]
        # Valid XLSX files start with PK (ZIP magic bytes)
        assert r.content[:2] == b"PK"


class TestExportTemplates:
    def test_list_templates_empty(self, client):
        r = client.get("/api/export/templates")
        assert r.status_code == 200
        assert r.json() == []

    def test_create_template(self, client):
        payload = {
            "name": "My NY Export",
            "format": "csv",
            "filter_criteria": {"states": ["NY"]},
            "field_selection": DEFAULT_FIELDS,
        }
        r = client.post("/api/export/templates", json=payload)
        assert r.status_code == 201
        body = r.json()
        assert body["name"] == "My NY Export"
        assert body["format"] == "csv"
        assert body["id"] is not None

    def test_list_templates_after_create(self, client):
        payload = {
            "name": "Template A",
            "format": "json",
            "filter_criteria": {},
            "field_selection": DEFAULT_FIELDS,
        }
        client.post("/api/export/templates", json=payload)
        r = client.get("/api/export/templates")
        assert r.status_code == 200
        assert len(r.json()) >= 1

    def test_duplicate_template_name_returns_409(self, client):
        payload = {
            "name": "Unique Name",
            "format": "csv",
            "filter_criteria": {},
            "field_selection": [],
        }
        r1 = client.post("/api/export/templates", json=payload)
        assert r1.status_code == 201
        r2 = client.post("/api/export/templates", json=payload)
        assert r2.status_code == 409
