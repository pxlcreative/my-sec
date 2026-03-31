"""
Export the OpenAPI spec from the running FastAPI app to docs/openapi.json.

Usage (from project root):
    docker compose exec api python scripts/export_openapi.py

Or without Docker (if running locally):
    cd api && python ../scripts/export_openapi.py

The script imports the FastAPI app directly and extracts the OpenAPI schema
without requiring a live HTTP server.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Allow importing from api/ when run from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

# Minimal env vars so settings loads without a real .env
os.environ.setdefault("DATABASE_URL", "postgresql://localhost/sec_adviser")
os.environ.setdefault("SECRET_KEY", "export-only-not-used")

from main import app  # noqa: E402

schema = app.openapi()

docs_dir = Path(__file__).parent.parent / "docs"
docs_dir.mkdir(exist_ok=True)
output_path = docs_dir / "openapi.json"

output_path.write_text(json.dumps(schema, indent=2))
print(f"OpenAPI spec written to {output_path} ({output_path.stat().st_size:,} bytes)")
print(f"  {len(schema.get('paths', {}))} paths")
print(f"  {len(schema.get('components', {}).get('schemas', {}))} schemas")
