"""
Module D: Two-stage bulk name+address → CRD matching pipeline.

Stage 1: Elasticsearch fuzzy multi-match retrieves candidates.
Stage 2: rapidfuzz token_sort_ratio + address component scoring refines them.
"""
import logging
import re

from rapidfuzz.fuzz import token_sort_ratio

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Name normalisation  (mirrors es_client.normalize_name but is self-contained
# so matcher can be used independently of the ES module)
# ---------------------------------------------------------------------------

_SUFFIX_RE = re.compile(
    r"\b(llc|lp|l\.p\.|l\.l\.c\.|inc|corp|corporation|"
    r"ltd|limited|co|company|plc|llp|pllc|pa|pc|na|"
    r"associates|advisors|advisers|group|partners|management|"
    r"capital|financial|investments?|services?|solutions?)\b\.?",
    re.IGNORECASE,
)
_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]")


def normalize_name(name: str | None) -> str:
    """
    Strip common legal suffixes/business words, remove punctuation,
    collapse whitespace, lowercase.
    """
    if not name:
        return ""
    s = name.strip()
    s = _SUFFIX_RE.sub(" ", s)
    s = _PUNCT_RE.sub("", s)
    s = _WHITESPACE_RE.sub(" ", s).strip()
    return s.lower()


# ---------------------------------------------------------------------------
# State normalisation
# ---------------------------------------------------------------------------

_STATE_NAME_TO_ABBR: dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC", "washington dc": "DC", "washington d.c.": "DC",
    # territories
    "puerto rico": "PR", "guam": "GU", "virgin islands": "VI",
    "american samoa": "AS", "northern mariana islands": "MP",
}

_VALID_ABBRS = set(_STATE_NAME_TO_ABBR.values())


def normalize_state(state: str | None) -> str:
    """
    Map a full state name to its 2-letter abbreviation.
    If already a 2-letter code (case-insensitive), return it uppercased.
    Returns '' for unknown/empty input.
    """
    if not state:
        return ""
    s = state.strip()
    if len(s) == 2:
        return s.upper()
    abbr = _STATE_NAME_TO_ABBR.get(s.lower())
    return abbr or s.upper()[:2]


# ---------------------------------------------------------------------------
# Stage 1 — ES candidate retrieval
# ---------------------------------------------------------------------------

def es_candidate_search(
    name: str,
    city: str | None = None,
    state: str | None = None,
    size: int = 5,
) -> list[dict]:
    """
    Query Elasticsearch for fuzzy firm name candidates.
    Returns list of raw hit dicts (crd_number, legal_name, …, _score).
    Raises on ES connectivity failure so callers can handle gracefully.
    """
    from services.es_client import search_firms as es_search
    return es_search(name, city=city, state=normalize_state(state), size=size)


# ---------------------------------------------------------------------------
# Stage 2 — Score computation
# ---------------------------------------------------------------------------

def compute_match_score(input_row: dict, candidate: dict) -> float:
    """
    Weighted score per spec Section 2d / Module D:
      name  × 0.60
      city  × 0.15
      state × 0.15
      zip   × 0.10

    All inputs are normalised before comparison.
    """
    norm_input = normalize_name(input_row.get("name", ""))
    norm_cand_legal = normalize_name(candidate.get("legal_name", ""))
    norm_cand_biz = normalize_name(candidate.get("business_name") or "")

    # Use the better of legal_name and business_name for the name score
    name_score = max(
        token_sort_ratio(norm_input, norm_cand_legal),
        token_sort_ratio(norm_input, norm_cand_biz) if norm_cand_biz else 0,
    )

    input_city = (input_row.get("city") or "").strip().lower()
    cand_city = (candidate.get("main_city") or "").strip().lower()
    city_score = 100.0 if input_city and cand_city and input_city == cand_city else 0.0

    input_state = normalize_state(input_row.get("state") or "")
    cand_state = normalize_state(candidate.get("main_state") or "")
    state_score = 100.0 if input_state and cand_state and input_state == cand_state else 0.0

    input_zip = (input_row.get("zip") or "").strip().replace("-", "")[:5]
    cand_zip = (candidate.get("main_zip") or "").strip().replace("-", "")[:5]
    zip_score = 100.0 if input_zip and cand_zip and input_zip == cand_zip else 0.0

    return (
        name_score  * 0.60
        + city_score  * 0.15
        + state_score * 0.15
        + zip_score   * 0.10
    )


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_match(score: float) -> str:
    if score >= 90:
        return "confirmed"
    if score >= 70:
        return "probable"
    if score >= 50:
        return "possible"
    return "no_match"


# ---------------------------------------------------------------------------
# match_record — one input row
# ---------------------------------------------------------------------------

def match_record(
    input_row: dict,
    min_score: float = 50.0,
    max_candidates: int = 3,
) -> list[dict]:
    """
    Run ES search → score → filter → sort → return top max_candidates.

    Returns a list of candidate dicts, each with added 'score' and 'status' keys.
    Empty list if nothing meets min_score.
    """
    try:
        candidates = es_candidate_search(
            name=input_row.get("name", ""),
            city=input_row.get("city"),
            state=input_row.get("state"),
            size=max(max_candidates * 3, 10),  # over-fetch so scoring can filter
        )
    except Exception as exc:
        log.warning("ES unavailable during match_record: %s", exc)
        return []

    scored = []
    for c in candidates:
        score = compute_match_score(input_row, c)
        if score >= min_score:
            scored.append({**c, "score": round(score, 2), "status": classify_match(score)})

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:max_candidates]


# ---------------------------------------------------------------------------
# match_batch — list of input rows
# ---------------------------------------------------------------------------

def match_batch(
    records: list[dict],
    min_score: float = 50.0,
    max_candidates: int = 3,
) -> dict:
    """
    Run match_record for every record. Returns:
      {
        "results":  [MatchResult dicts],
        "stats":    {total, confirmed, probable, possible, no_match},
      }
    """
    stats = {"total": 0, "confirmed": 0, "probable": 0, "possible": 0, "no_match": 0}
    results = []

    for row in records:
        stats["total"] += 1
        candidates = match_record(row, min_score=min_score, max_candidates=max_candidates)

        best_score = candidates[0]["score"] if candidates else 0.0
        best_status = candidates[0]["status"] if candidates else "no_match"
        stats[best_status] += 1

        results.append(
            {
                "id":           row.get("id"),
                "input_name":   row.get("name", ""),
                "input_city":   row.get("city"),
                "input_state":  row.get("state"),
                "input_zip":    row.get("zip"),
                "best_score":   best_score,
                "best_status":  best_status,
                "candidates":   candidates,
            }
        )

    return {"results": results, "stats": stats}
