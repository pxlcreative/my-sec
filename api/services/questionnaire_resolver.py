"""
Field path registry and resolver for questionnaire auto-population.

Available field paths:
  firm.*          — direct Firm model attributes
  disclosures.*   — FirmDisclosuresSummary attributes + total_count
  aum_history.*   — computed from list of FirmAumHistory
  raw_adv.<path>  — dot-path into firm.raw_adv JSONB (e.g. raw_adv.FormInfo.Part1A.Item5F.Q5F2C)
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


@dataclass
class FieldDef:
    label: str
    category: str   # "Firm Info", "AUM", "Disclosures", "AUM History", "Raw ADV"
    field_type: str  # "text", "money", "number", "date", "computed"
    example: str = ""


# ---------------------------------------------------------------------------
# Registry — all available field paths
# ---------------------------------------------------------------------------

FIELD_REGISTRY: dict[str, FieldDef] = {
    # Firm Info
    "firm.legal_name":          FieldDef("Legal Name",            "Firm Info",   "text",     "Acme Advisers LLC"),
    "firm.business_name":       FieldDef("Business Name",         "Firm Info",   "text",     "Acme Advisers"),
    "firm.crd_number":          FieldDef("CRD Number",            "Firm Info",   "number",   "123456"),
    "firm.sec_number":          FieldDef("SEC Number",            "Firm Info",   "text",     "801-12345"),
    "firm.registration_status": FieldDef("Registration Status",   "Firm Info",   "text",     "Registered"),
    "firm.org_type":            FieldDef("Organization Type",     "Firm Info",   "text",     "Corporation"),
    "firm.fiscal_year_end":     FieldDef("Fiscal Year End",       "Firm Info",   "text",     "December"),
    "firm.phone":               FieldDef("Phone",                 "Firm Info",   "text",     "212-555-0100"),
    "firm.website":             FieldDef("Website",               "Firm Info",   "text",     "https://example.com"),
    "firm.last_filing_date":    FieldDef("Last Filing Date",      "Firm Info",   "date",     "2025-03-31"),
    "firm.num_employees":       FieldDef("Number of Employees",   "Firm Info",   "number",   "42"),
    "firm.main_street1":        FieldDef("Street Address",        "Firm Info",   "text",     "100 Main St"),
    "firm.main_street2":        FieldDef("Street Address 2",      "Firm Info",   "text",     "Suite 200"),
    "firm.main_city":           FieldDef("City",                  "Firm Info",   "text",     "New York"),
    "firm.main_state":          FieldDef("State",                 "Firm Info",   "text",     "NY"),
    "firm.main_zip":            FieldDef("ZIP Code",              "Firm Info",   "text",     "10001"),
    "firm.main_country":        FieldDef("Country",               "Firm Info",   "text",     "United States"),
    "firm.address_full":        FieldDef("Full Address",          "Firm Info",   "computed", "100 Main St, New York, NY 10001"),
    # AUM
    "firm.aum_total":               FieldDef("Total AUM",              "AUM", "money",  "$1,200,000,000"),
    "firm.aum_discretionary":       FieldDef("Discretionary AUM",      "AUM", "money",  "$900,000,000"),
    "firm.aum_non_discretionary":   FieldDef("Non-Discretionary AUM",  "AUM", "money",  "$300,000,000"),
    "firm.num_accounts":            FieldDef("Number of Accounts",     "AUM", "number", "850"),
    "firm.aum_2023":                FieldDef("AUM 2023",               "AUM", "money",  "$1,100,000,000"),
    "firm.aum_2024":                FieldDef("AUM 2024",               "AUM", "money",  "$1,200,000,000"),
    # Disclosures
    "disclosures.criminal_count":    FieldDef("Criminal Disclosures",    "Disclosures", "number",   "0"),
    "disclosures.regulatory_count":  FieldDef("Regulatory Disclosures",  "Disclosures", "number",   "0"),
    "disclosures.civil_count":       FieldDef("Civil Disclosures",       "Disclosures", "number",   "0"),
    "disclosures.customer_count":    FieldDef("Customer Disclosures",    "Disclosures", "number",   "0"),
    "disclosures.total_count":       FieldDef("Total Disclosures",       "Disclosures", "computed", "0"),
    "disclosures.has_disclosures":   FieldDef("Has Any Disclosures",     "Disclosures", "computed", "No disclosures on record"),
    # AUM History
    "aum_history.latest_total":        FieldDef("Latest AUM (history)",        "AUM History", "money",    "$1,200,000,000"),
    "aum_history.latest_filing_date":  FieldDef("Latest Filing Date (history)", "AUM History", "date",     "2025-03-31"),
    "aum_history.count":               FieldDef("Number of AUM Records",        "AUM History", "number",   "12"),
    # Raw ADV (special: resolved via dot-path)
    "raw_adv.*": FieldDef(
        "Raw ADV Field (dot-path)",
        "Raw ADV",
        "text",
        "raw_adv.FormInfo.Part1A.Item5F.Q5F2C",
    ),
}


def get_field_registry() -> dict[str, FieldDef]:
    return FIELD_REGISTRY


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_money(v: Any) -> str:
    try:
        return f"${int(v):,.0f}" if v is not None else "N/A"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_date(v: Any) -> str:
    if v is None:
        return "N/A"
    if isinstance(v, (date, datetime)):
        return v.strftime("%Y-%m-%d")
    return str(v)


def _deep_get(obj: Any, path: str) -> Any:
    """Navigate a nested dict/list using dot-notation path."""
    if obj is None:
        return None
    for key in path.split("."):
        if isinstance(obj, dict):
            obj = obj.get(key)
        elif isinstance(obj, list):
            try:
                obj = obj[int(key)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if obj is None:
            return None
    return obj


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

def resolve_fields(firm, disclosures, aum_history: list) -> dict[str, Any]:
    """
    Resolve all known field paths to raw values for a given firm.
    Returns {field_path: raw_value} — use resolve_answer() to format for display.
    """
    resolved: dict[str, Any] = {}

    # Firm fields
    for attr in (
        "legal_name", "business_name", "crd_number", "sec_number",
        "registration_status", "org_type", "fiscal_year_end",
        "phone", "website", "last_filing_date", "num_employees",
        "main_street1", "main_street2", "main_city", "main_state",
        "main_zip", "main_country",
        "aum_total", "aum_discretionary", "aum_non_discretionary",
        "num_accounts", "aum_2023", "aum_2024",
    ):
        resolved[f"firm.{attr}"] = getattr(firm, attr, None)

    # Computed firm fields
    parts = [
        firm.main_street1 or "",
        firm.main_city or "",
        (f"{firm.main_state or ''} {firm.main_zip or ''}").strip(),
    ]
    resolved["firm.address_full"] = ", ".join(p for p in parts if p).strip(", ")

    # Disclosures
    if disclosures:
        resolved["disclosures.criminal_count"]   = disclosures.criminal_count or 0
        resolved["disclosures.regulatory_count"] = disclosures.regulatory_count or 0
        resolved["disclosures.civil_count"]      = disclosures.civil_count or 0
        resolved["disclosures.customer_count"]   = disclosures.customer_count or 0
        total = (
            (disclosures.criminal_count    or 0)
            + (disclosures.regulatory_count or 0)
            + (disclosures.civil_count      or 0)
            + (disclosures.customer_count   or 0)
        )
        resolved["disclosures.total_count"]    = total
        resolved["disclosures.has_disclosures"] = (
            f"Yes – {total} total disclosure(s)" if total > 0 else "No disclosures on record"
        )
    else:
        for key in (
            "disclosures.criminal_count", "disclosures.regulatory_count",
            "disclosures.civil_count", "disclosures.customer_count",
            "disclosures.total_count",
        ):
            resolved[key] = 0
        resolved["disclosures.has_disclosures"] = "No disclosures on record"

    # AUM history
    sorted_history = sorted(
        aum_history,
        key=lambda r: (getattr(r, "filing_date", None) or date.min),
        reverse=True,
    )
    if sorted_history:
        latest = sorted_history[0]
        resolved["aum_history.latest_total"]       = getattr(latest, "aum_total", None)
        resolved["aum_history.latest_filing_date"] = getattr(latest, "filing_date", None)
    else:
        resolved["aum_history.latest_total"]       = None
        resolved["aum_history.latest_filing_date"] = None
    resolved["aum_history.count"] = len(aum_history)

    return resolved


def resolve_answer(field_path: str, resolved: dict[str, Any], firm=None) -> str:
    """
    Format a single resolved value as a human-readable string for display/Excel.
    Handles raw_adv.* dot-paths specially.
    """
    if field_path.startswith("raw_adv."):
        adv_path = field_path[len("raw_adv."):]
        raw_adv = getattr(firm, "raw_adv", None) if firm else None
        value = _deep_get(raw_adv, adv_path)
        return str(value) if value is not None else "N/A"

    value = resolved.get(field_path)
    defn = FIELD_REGISTRY.get(field_path)

    if value is None:
        return "N/A"

    if defn and defn.field_type == "money":
        return _fmt_money(value)
    if defn and defn.field_type == "date":
        return _fmt_date(value)
    if defn and defn.field_type == "number":
        return str(value)

    return str(value)
