"""
tests/test_es_search.py

Integration test for Elasticsearch firm indexing and fuzzy search.

Requires a running ES instance (set ELASTICSEARCH_URL or use docker compose).
Run with:
    docker compose run --rm api python -m pytest tests/test_es_search.py -v
"""
import os
import sys
import time
from pathlib import Path

import pytest

# Make api/ importable
sys.path.insert(0, str(Path(__file__).parent.parent / "api"))

# Skip entire module if ES is not reachable
es_url = os.environ.get("ELASTICSEARCH_URL", "http://elasticsearch:9200")
es = pytest.importorskip("elasticsearch", reason="elasticsearch package not installed")


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

MOCK_FIRMS = [
    {
        "crd_number": 900001,
        "legal_name": "Acme Capital Management LLC",
        "business_name": "Acme Capital",
        "main_street1": "100 Park Ave",
        "main_city": "New York",
        "main_state": "NY",
        "main_zip": "10001",
        "registration_status": "Registered",
    },
    {
        "crd_number": 900002,
        "legal_name": "Blue Ridge Wealth Advisors Inc",
        "business_name": "Blue Ridge Wealth",
        "main_street1": "200 Main St",
        "main_city": "Charlotte",
        "main_state": "NC",
        "main_zip": "28201",
        "registration_status": "Registered",
    },
    {
        "crd_number": 900003,
        "legal_name": "Summit Peak Investment Partners LP",
        "business_name": "Summit Peak",
        "main_street1": "50 Alpine Way",
        "main_city": "Denver",
        "main_state": "CO",
        "main_zip": "80201",
        "registration_status": "Registered",
    },
    {
        "crd_number": 900004,
        "legal_name": "Harbor Light Asset Management Corp",
        "business_name": "Harbor Light",
        "main_street1": "1 Harbor Dr",
        "main_city": "Boston",
        "main_state": "MA",
        "main_zip": "02101",
        "registration_status": "Registered",
    },
    {
        "crd_number": 900005,
        "legal_name": "Pinnacle Financial Group LLC",
        "business_name": "Pinnacle Financial",
        "main_street1": "300 Commerce Blvd",
        "main_city": "Atlanta",
        "main_state": "GA",
        "main_zip": "30301",
        "registration_status": "Withdrawn",
    },
]

_TEST_INDEX = "firms_test"


@pytest.fixture(scope="module")
def es_client():
    """Return an ES client, skip the module if ES is unreachable."""
    from elasticsearch import Elasticsearch
    client = Elasticsearch(es_url, request_timeout=5)
    try:
        client.cluster.health(timeout="5s")
    except Exception as exc:
        pytest.skip(f"Elasticsearch not reachable at {es_url}: {exc}")
    return client


@pytest.fixture(scope="module", autouse=True)
def setup_test_index(es_client):
    """Create a clean test index, index mock firms, then delete on teardown."""
    # Monkey-patch es_client module to use test index
    import services.es_client as esc
    original_index = esc.FIRMS_INDEX
    original_client = esc._client
    esc.FIRMS_INDEX = _TEST_INDEX
    esc._client = es_client

    # Fresh index
    if es_client.indices.exists(index=_TEST_INDEX):
        es_client.indices.delete(index=_TEST_INDEX)
    esc.create_index_if_not_exists()
    esc.bulk_index_firms(MOCK_FIRMS)

    # Wait for ES to make documents searchable
    es_client.indices.refresh(index=_TEST_INDEX)
    time.sleep(0.5)

    yield

    # Teardown
    es_client.indices.delete(index=_TEST_INDEX, ignore_unavailable=True)
    esc.FIRMS_INDEX = original_index
    esc._client = original_client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFuzzySearch:
    def test_exact_name_match(self):
        """Exact legal name returns the correct firm as top hit."""
        from services.es_client import search_firms
        hits = search_firms("Acme Capital Management", size=5)
        assert hits, "Expected at least one result"
        assert hits[0]["crd_number"] == 900001

    def test_misspelled_name_fuzzy(self):
        """A slightly misspelled name still finds the correct firm."""
        from services.es_client import search_firms
        # "Acme Capitl Managment" — two typos
        hits = search_firms("Acme Capitl Managment", size=5)
        assert hits, "Expected at least one result"
        crds = [h["crd_number"] for h in hits]
        assert 900001 in crds, f"Expected CRD 900001 in results, got {crds}"

    def test_business_name_match(self):
        """Searching by business name (without legal suffix) works."""
        from services.es_client import search_firms
        hits = search_firms("Blue Ridge Wealth", size=5)
        assert hits
        assert hits[0]["crd_number"] == 900002

    def test_stripped_suffix_match(self):
        """Searching without the LLC/Inc suffix still returns the firm."""
        from services.es_client import search_firms
        hits = search_firms("Harbor Light Asset Management", size=5)
        assert hits
        assert hits[0]["crd_number"] == 900004

    def test_state_filter_narrows_results(self):
        """Adding state filter boosts the correct state hit to the top."""
        from services.es_client import search_firms
        # Both "Summit Peak" (CO) and anything else would match; state should boost CO
        hits = search_firms("Summit Peak", state="CO", size=5)
        assert hits
        assert hits[0]["crd_number"] == 900003

    def test_no_match_returns_empty(self):
        """A query with no plausible match returns empty list."""
        from services.es_client import search_firms
        hits = search_firms("Zxqwerty Nonexistent Advisory Partners", size=5)
        assert hits == [] or all(h["_score"] < 0.5 for h in hits)

    def test_result_schema(self):
        """Each hit contains the required fields."""
        from services.es_client import search_firms
        required = {
            "crd_number", "legal_name", "business_name",
            "main_city", "main_state", "main_zip",
            "registration_status", "_score",
        }
        hits = search_firms("Pinnacle Financial", size=3)
        assert hits
        assert required.issubset(hits[0].keys())

    def test_normalize_name_strips_suffixes(self):
        """normalize_name removes common legal suffixes."""
        from services.es_client import normalize_name
        assert normalize_name("Acme Capital Management LLC") == "acme capital management"
        assert normalize_name("Blue Ridge Wealth Advisors Inc") == "blue ridge wealth advisors"
        assert normalize_name("Summit Peak Investment Partners LP") == "summit peak investment partners"
        assert normalize_name("Harbor Light Asset Management Corp") == "harbor light asset management"
