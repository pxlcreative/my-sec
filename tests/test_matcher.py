"""
tests/test_matcher.py

Unit tests for the matching pipeline (no ES or DB required).

Scoring tests use a mock ES search so the full pipeline can be exercised
in isolation.
"""
import sys
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).parent.parent / "api"))

from services.matcher import (
    classify_match,
    compute_match_score,
    match_batch,
    match_record,
    normalize_name,
    normalize_state,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _candidate(
    crd: int = 12345,
    legal_name: str = "Acme Capital Management LLC",
    business_name: str | None = "Acme Capital",
    city: str = "New York",
    state: str = "NY",
    zip_: str = "10001",
    status: str = "Registered",
) -> dict:
    return {
        "crd_number": crd,
        "legal_name": legal_name,
        "business_name": business_name,
        "main_city": city,
        "main_state": state,
        "main_zip": zip_,
        "registration_status": status,
        "_score": 5.0,
    }


def _input(
    name: str = "Acme Capital Management LLC",
    city: str = "New York",
    state: str = "NY",
    zip_: str = "10001",
) -> dict:
    return {"name": name, "city": city, "state": state, "zip": zip_}


# ---------------------------------------------------------------------------
# normalize_name
# ---------------------------------------------------------------------------

class TestNormalizeName:
    def test_strips_llc(self):
        assert "acme capital" in normalize_name("Acme Capital LLC")

    def test_strips_inc(self):
        assert "blue ridge wealth" in normalize_name("Blue Ridge Wealth Advisors Inc")

    def test_strips_lp(self):
        result = normalize_name("Summit Peak Investment Partners LP")
        assert "lp" not in result
        assert "summit peak" in result

    def test_strips_corp(self):
        result = normalize_name("Harbor Light Asset Management Corp")
        assert "corp" not in result

    def test_empty_string(self):
        assert normalize_name("") == ""

    def test_none(self):
        assert normalize_name(None) == ""

    def test_lowercases(self):
        assert normalize_name("ACME CAPITAL LLC") == normalize_name("acme capital llc")

    def test_removes_punctuation(self):
        result = normalize_name("A.B.C. Partners, LLC")
        assert "." not in result
        assert "," not in result


# ---------------------------------------------------------------------------
# normalize_state
# ---------------------------------------------------------------------------

class TestNormalizeState:
    def test_abbreviation_passthrough(self):
        assert normalize_state("NY") == "NY"
        assert normalize_state("ca") == "CA"

    def test_full_name_new_york(self):
        assert normalize_state("New York") == "NY"

    def test_full_name_california(self):
        assert normalize_state("California") == "CA"

    def test_full_name_north_carolina(self):
        assert normalize_state("North Carolina") == "NC"

    def test_full_name_texas(self):
        assert normalize_state("Texas") == "TX"

    def test_washington_dc(self):
        assert normalize_state("District of Columbia") == "DC"

    def test_empty_returns_empty(self):
        assert normalize_state("") == ""

    def test_none_returns_empty(self):
        assert normalize_state(None) == ""


# ---------------------------------------------------------------------------
# compute_match_score
# ---------------------------------------------------------------------------

class TestComputeMatchScore:
    def test_exact_match_all_fields_scores_near_100(self):
        score = compute_match_score(
            _input("Acme Capital Management LLC", "New York", "NY", "10001"),
            _candidate(legal_name="Acme Capital Management LLC", city="New York",
                       state="NY", zip_="10001"),
        )
        assert score >= 90, f"Expected ≥90, got {score}"

    def test_misspelled_name_correct_city_state_scores_above_70(self):
        # Two character transpositions in the name
        score = compute_match_score(
            _input("Acme Capitl Managment", "New York", "NY", "10001"),
            _candidate(legal_name="Acme Capital Management LLC", city="New York",
                       state="NY", zip_="10001"),
        )
        assert score >= 70, f"Expected ≥70, got {score}"

    def test_wrong_city_drops_score(self):
        score_correct_city = compute_match_score(
            _input("Acme Capital Management", "New York", "NY", "10001"),
            _candidate(city="New York", state="NY", zip_="10001"),
        )
        score_wrong_city = compute_match_score(
            _input("Acme Capital Management", "Los Angeles", "NY", "10001"),
            _candidate(city="New York", state="NY", zip_="10001"),
        )
        assert score_wrong_city < score_correct_city
        # City weight is 0.15 × 100 = 15 points
        assert abs(score_correct_city - score_wrong_city) >= 14

    def test_wrong_state_drops_score(self):
        score_correct = compute_match_score(
            _input(state="NY"), _candidate(state="NY"),
        )
        score_wrong = compute_match_score(
            _input(state="CA"), _candidate(state="NY"),
        )
        assert score_wrong < score_correct
        assert abs(score_correct - score_wrong) >= 14

    def test_wrong_zip_drops_score(self):
        score_correct = compute_match_score(
            _input(zip_="10001"), _candidate(zip_="10001"),
        )
        score_wrong = compute_match_score(
            _input(zip_="90210"), _candidate(zip_="10001"),
        )
        assert score_wrong < score_correct

    def test_full_name_state_match(self):
        """State given as full name should still match correctly."""
        score = compute_match_score(
            _input("Acme Capital Management LLC", "New York", "New York", "10001"),
            _candidate(city="New York", state="NY", zip_="10001"),
        )
        assert score >= 90, f"Expected ≥90 with full state name, got {score}"

    def test_business_name_match(self):
        """Input name matching the business_name (not legal_name) still scores well."""
        score = compute_match_score(
            _input("Acme Capital"),
            _candidate(legal_name="Acme Capital Management LLC",
                       business_name="Acme Capital",
                       city="New York", state="NY", zip_="10001"),
        )
        assert score >= 60, f"Expected ≥60 for business name match, got {score}"

    def test_zip_prefix_match(self):
        """ZIP+4 format should match on first 5 digits."""
        score_with_ext = compute_match_score(
            _input(zip_="10001-1234"), _candidate(zip_="10001"),
        )
        score_without_ext = compute_match_score(
            _input(zip_="10001"), _candidate(zip_="10001"),
        )
        assert score_with_ext == score_without_ext


# ---------------------------------------------------------------------------
# classify_match
# ---------------------------------------------------------------------------

class TestClassifyMatch:
    def test_confirmed(self):
        assert classify_match(95.0) == "confirmed"
        assert classify_match(90.0) == "confirmed"

    def test_probable(self):
        assert classify_match(89.9) == "probable"
        assert classify_match(70.0) == "probable"

    def test_possible(self):
        assert classify_match(69.9) == "possible"
        assert classify_match(50.0) == "possible"

    def test_no_match(self):
        assert classify_match(49.9) == "no_match"
        assert classify_match(0.0) == "no_match"


# ---------------------------------------------------------------------------
# match_record and match_batch (with mocked ES)
# ---------------------------------------------------------------------------

_MOCK_CANDIDATES = [
    _candidate(crd=1, legal_name="Acme Capital Management LLC",
               city="New York", state="NY", zip_="10001"),
    _candidate(crd=2, legal_name="Blue Ridge Wealth Advisors Inc",
               city="Charlotte", state="NC", zip_="28201"),
    _candidate(crd=3, legal_name="Unrelated Firm Corp",
               city="Dallas", state="TX", zip_="75201"),
]


class TestMatchRecord:
    def test_exact_match_returns_correct_crd(self):
        with patch("services.matcher.es_candidate_search", return_value=_MOCK_CANDIDATES):
            results = match_record(
                _input("Acme Capital Management LLC", "New York", "NY", "10001"),
                min_score=50,
                max_candidates=3,
            )
        assert results, "Expected at least one result"
        assert results[0]["crd_number"] == 1
        assert results[0]["status"] == "confirmed"

    def test_results_sorted_by_score_descending(self):
        with patch("services.matcher.es_candidate_search", return_value=_MOCK_CANDIDATES):
            results = match_record(_input(), min_score=0, max_candidates=3)
        scores = [r["score"] for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_min_score_filters_low_candidates(self):
        with patch("services.matcher.es_candidate_search", return_value=_MOCK_CANDIDATES):
            results = match_record(_input(), min_score=90, max_candidates=3)
        assert all(r["score"] >= 90 for r in results)

    def test_max_candidates_limits_output(self):
        with patch("services.matcher.es_candidate_search", return_value=_MOCK_CANDIDATES):
            results = match_record(_input(), min_score=0, max_candidates=2)
        assert len(results) <= 2

    def test_es_failure_returns_empty(self):
        with patch("services.matcher.es_candidate_search",
                   side_effect=ConnectionError("ES down")):
            results = match_record(_input())
        assert results == []


class TestMatchBatch:
    def test_stats_counts_are_correct(self):
        records = [
            {"name": "Acme Capital Management LLC", "city": "New York",
             "state": "NY", "zip": "10001"},
            {"name": "Utter Nonsense XYZ", "city": "Nowhere",
             "state": "ZZ", "zip": "00000"},
        ]
        candidates_map = {
            "Acme Capital Management LLC": _MOCK_CANDIDATES,
            "Utter Nonsense XYZ": [],
        }

        def fake_es(name, city=None, state=None, size=5):
            return candidates_map.get(name, [])

        with patch("services.matcher.es_candidate_search", side_effect=fake_es):
            output = match_batch(records, min_score=50, max_candidates=3)

        assert output["stats"]["total"] == 2
        assert output["stats"]["no_match"] >= 1
        assert len(output["results"]) == 2

    def test_result_fields_present(self):
        with patch("services.matcher.es_candidate_search", return_value=_MOCK_CANDIDATES):
            output = match_batch([_input()], min_score=0)

        r = output["results"][0]
        for field in ("id", "input_name", "input_city", "input_state",
                      "input_zip", "best_score", "best_status", "candidates"):
            assert field in r, f"Missing field: {field}"
