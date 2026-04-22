"""
Elasticsearch client and index helpers for firm search.

Design decisions:
- The client is created once at import time (connection-pooled by the ES library).
- All public functions catch ES exceptions and surface them as plain Python
  exceptions so callers can decide whether to fall back to Postgres.
- Index name is a module constant so it's easy to change for re-indexing.
"""
import logging
import re
from typing import Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from config import settings

log = logging.getLogger(__name__)

FIRMS_INDEX = "firms"

# Suffixes to strip when normalising firm names for indexing.
_SUFFIX_RE = re.compile(
    r"\b(llc|lp|l\.p\.|l\.l\.c\.|inc|corp|corporation|"
    r"ltd|limited|co|company|plc|llp|pllc|pa|pc|na)\b\.?$",
    re.IGNORECASE,
)
_PUNCT_RE = re.compile(r"[^\w\s]")


def normalize_name(name: str | None) -> str:
    """Strip common legal suffixes, punctuation, and lowercase."""
    if not name:
        return ""
    s = name.strip()
    s = _SUFFIX_RE.sub("", s).strip()
    s = _PUNCT_RE.sub("", s)
    return s.lower().strip()


# ---------------------------------------------------------------------------
# Client singleton
# ---------------------------------------------------------------------------

def _make_client() -> Elasticsearch:
    return Elasticsearch(settings.elasticsearch_url, request_timeout=10)


_client: Elasticsearch | None = None


def get_client() -> Elasticsearch:
    global _client
    if _client is None:
        _client = _make_client()
    return _client


# ---------------------------------------------------------------------------
# Index mapping
# ---------------------------------------------------------------------------

FIRMS_MAPPING: dict[str, Any] = {
    "mappings": {
        "properties": {
            "crd_number":          {"type": "integer"},
            "legal_name":          {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "normalized_name":     {"type": "text", "analyzer": "standard"},
            "business_name":       {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "main_street1":        {"type": "text", "analyzer": "standard"},
            "main_city":           {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 128}},
            },
            "main_state":          {"type": "keyword"},
            "main_zip":            {"type": "keyword"},
            "registration_status": {"type": "keyword"},
            "platforms":           {"type": "keyword"},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,  # single-node dev; increase in prod
    },
}


def create_index_if_not_exists() -> None:
    """Create the firms index with the mapping. Silently skips if it already exists."""
    es = get_client()
    if not es.indices.exists(index=FIRMS_INDEX):
        es.indices.create(index=FIRMS_INDEX, body=FIRMS_MAPPING)
        log.info("Created Elasticsearch index '%s'", FIRMS_INDEX)
    else:
        log.debug("Elasticsearch index '%s' already exists", FIRMS_INDEX)


# ---------------------------------------------------------------------------
# Single-document index
# ---------------------------------------------------------------------------

def _firm_to_doc(firm_dict: dict) -> dict:
    """Convert a firm dict (from ORM or Postgres row) into an ES document."""
    legal = firm_dict.get("legal_name") or ""
    business = firm_dict.get("business_name") or ""
    return {
        "crd_number":          firm_dict.get("crd_number"),
        "legal_name":          legal,
        "normalized_name":     normalize_name(legal) or normalize_name(business),
        "business_name":       business or None,
        "main_street1":        firm_dict.get("main_street1"),
        "main_city":           firm_dict.get("main_city"),
        "main_state":          firm_dict.get("main_state"),
        "main_zip":            firm_dict.get("main_zip"),
        "registration_status": firm_dict.get("registration_status"),
    }


def index_firm(firm_dict: dict) -> None:
    """Index or replace a single firm document."""
    crd = firm_dict["crd_number"]
    doc = _firm_to_doc(firm_dict)
    get_client().index(index=FIRMS_INDEX, id=str(crd), document=doc)


# ---------------------------------------------------------------------------
# Bulk index
# ---------------------------------------------------------------------------

def bulk_index_firms(firm_dicts: list[dict], batch_size: int = 500) -> int:
    """
    Bulk-index a list of firm dicts.
    Returns total number of documents successfully indexed.
    """
    es = get_client()
    total = 0

    def _actions():
        for firm in firm_dicts:
            yield {
                "_index": FIRMS_INDEX,
                "_id":    str(firm["crd_number"]),
                "_source": _firm_to_doc(firm),
            }

    # helpers.bulk handles chunking internally; we pass chunk_size explicitly.
    success, errors = bulk(
        es,
        _actions(),
        chunk_size=batch_size,
        raise_on_error=False,
        stats_only=False,
    )
    if errors:
        log.warning("bulk_index_firms: %d errors (first: %s)", len(errors), errors[0])
    total = success
    log.debug("bulk_index_firms: indexed %d documents", total)
    return total


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search_firms(
    query: str,
    city: str | None = None,
    state: str | None = None,
    size: int = 10,
) -> list[dict]:
    """
    Multi-match fuzzy search as specified in Section 2d.

    Returns a list of hit dicts:
      {crd_number, legal_name, business_name, main_city, main_state,
       main_zip, registration_status, _score}
    """
    must: list[dict] = [
        {"match": {"legal_name":   {"query": query, "fuzziness": "AUTO", "boost": 3}}},
        {"match": {"business_name": {"query": query, "fuzziness": "AUTO", "boost": 2}}},
    ]

    # Also match the stripped/normalised name for robustness
    should: list[dict] = [
        {"match": {"normalized_name": {"query": normalize_name(query), "boost": 2}}},
    ]
    if city:
        should.append({"match": {"main_city": city}})
    if state:
        should.append({"term": {"main_state": state.upper()}})

    es_query = {
        "query": {
            "bool": {
                "must":   must,
                "should": should,
            }
        },
        "size": size,
    }

    resp = get_client().search(index=FIRMS_INDEX, body=es_query)
    hits = resp["hits"]["hits"]

    results = []
    for hit in hits:
        src = hit["_source"]
        results.append(
            {
                "crd_number":          src.get("crd_number"),
                "legal_name":          src.get("legal_name"),
                "business_name":       src.get("business_name"),
                "main_city":           src.get("main_city"),
                "main_state":          src.get("main_state"),
                "main_zip":            src.get("main_zip"),
                "registration_status": src.get("registration_status"),
                "_score":              hit["_score"],
            }
        )
    return results
