import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from db import get_db
from schemas.firm import (
    AumAnnualSummary,
    AumHistoryPoint,
    AumHistoryResponse,
    BrochureMeta,
    BusinessProfileOut,
    ChangeRecord,
    DisclosuresSummaryOut,
    FirmDetail,
    FirmHistoryResponse,
    FirmSummary,
    PaginatedFirms,
)
from services import firm_service
from services.firm_service import FirmFilters

log = logging.getLogger(__name__)
router = APIRouter(prefix="/firms", tags=["firms"])


# ---------------------------------------------------------------------------
# Helper: build FirmSummary from ORM Firm + platform name list
# ---------------------------------------------------------------------------

def _to_summary(firm, platform_names: list[str]) -> FirmSummary:
    return FirmSummary(
        crd_number=firm.crd_number,
        legal_name=firm.legal_name,
        business_name=firm.business_name,
        main_city=firm.main_city,
        main_state=firm.main_state,
        aum_total=firm.aum_total,
        registration_status=firm.registration_status,
        last_filing_date=firm.last_filing_date,
        platforms=platform_names,
    )


# ---------------------------------------------------------------------------
# GET /api/firms
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=PaginatedFirms,
    summary="List and filter firms",
    description="Returns a paginated list of firms. Supports filtering by state, AUM range, registration status, platform, and full-text search.",
)
def list_firms(
    state: str | None = Query(None, description="2-letter state code"),
    aum_min: int | None = Query(None, ge=0),
    aum_max: int | None = Query(None, ge=0),
    registration_status: str | None = Query(None),
    platform_ids: list[int] = Query(default=[]),
    q: str | None = Query(None, description="Search query (applied via GIN index)"),
    sort_by: str | None = Query(None, description="Column to sort by"),
    sort_dir: str | None = Query(None, description="Sort direction: asc or desc"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedFirms:
    try:
        filters = FirmFilters(
            state=state,
            aum_min=aum_min,
            aum_max=aum_max,
            registration_status=registration_status,
            platform_ids=platform_ids,
            search_query=q,
        )
        total, firms = firm_service.list_firms(filters, page, page_size, db, sort_by=sort_by, sort_dir=sort_dir)
        crds = [f.crd_number for f in firms]
        platform_map = firm_service._platform_names_for_crds(db, crds)
        results = [_to_summary(f, platform_map.get(f.crd_number, [])) for f in firms]
        return PaginatedFirms(total=total, page=page, page_size=page_size, results=results)
    except HTTPException:
        raise
    except Exception:
        log.error("list_firms error\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# GET /api/firms/search
# ---------------------------------------------------------------------------

@router.get(
    "/search",
    response_model=PaginatedFirms,
    summary="Full-text firm search",
    description="Elasticsearch-backed full-text search over firm names. Falls back to PostgreSQL GIN index if ES is unavailable.",
)
def search_firms(
    q: str = Query(..., min_length=1, description="Full-text search query"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedFirms:
    try:
        total, firms = firm_service.search_firms(q, page, page_size, db)
        crds = [f.crd_number for f in firms]
        platform_map = firm_service._platform_names_for_crds(db, crds)
        results = [_to_summary(f, platform_map.get(f.crd_number, [])) for f in firms]
        return PaginatedFirms(total=total, page=page, page_size=page_size, results=results)
    except HTTPException:
        raise
    except Exception:
        log.error("search_firms error\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# GET /api/firms/{crd}
# ---------------------------------------------------------------------------

@router.get(
    "/{crd}",
    response_model=FirmDetail,
    summary="Get firm detail",
    description="Returns full detail for a single firm by CRD number. Returns 404 if the CRD is not in the database.",
)
def get_firm(
    crd: int,
    include_raw_adv: bool = Query(False, description="Include full raw ADV JSON"),
    db: Session = Depends(get_db),
) -> FirmDetail:
    try:
        firm = firm_service.get_firm(crd, db)
        platform_map = firm_service._platform_names_for_crds(db, [crd])
        brochure = firm_service.get_latest_brochure(crd, db)

        return FirmDetail(
            crd_number=firm.crd_number,
            sec_number=firm.sec_number,
            legal_name=firm.legal_name,
            business_name=firm.business_name,
            firm_type=firm.firm_type,
            registration_status=firm.registration_status,
            aum_total=firm.aum_total,
            aum_discretionary=firm.aum_discretionary,
            aum_non_discretionary=firm.aum_non_discretionary,
            num_accounts=firm.num_accounts,
            num_employees=firm.num_employees,
            main_street1=firm.main_street1,
            main_street2=firm.main_street2,
            main_city=firm.main_city,
            main_state=firm.main_state,
            main_zip=firm.main_zip,
            main_country=firm.main_country,
            phone=firm.phone,
            website=firm.website,
            org_type=firm.org_type,
            fiscal_year_end=firm.fiscal_year_end,
            last_filing_date=firm.last_filing_date,
            aum_2023=firm.aum_2023,
            aum_2024=firm.aum_2024,
            created_at=firm.created_at,
            updated_at=firm.updated_at,
            last_iapd_refresh_at=firm.last_iapd_refresh_at,
            platforms=platform_map.get(crd, []),
            raw_adv=firm.raw_adv if include_raw_adv else None,
            latest_brochure=BrochureMeta.model_validate(brochure) if brochure else None,
        )
    except HTTPException:
        raise
    except Exception:
        log.error("get_firm(%s) error\n%s", crd, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# GET /api/firms/{crd}/history
# ---------------------------------------------------------------------------

@router.get(
    "/{crd}/history",
    response_model=FirmHistoryResponse,
    summary="Firm change history",
    description="Returns all field-level changes detected for this firm across refresh cycles, ordered by detection time descending.",
)
def get_firm_history(
    crd: int,
    db: Session = Depends(get_db),
) -> FirmHistoryResponse:
    try:
        changes = firm_service.get_firm_history(crd, db)
        return FirmHistoryResponse(
            crd_number=crd,
            changes=[ChangeRecord.model_validate(c) for c in changes],
        )
    except HTTPException:
        raise
    except Exception:
        log.error("get_firm_history(%s) error\n%s", crd, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# GET /api/firms/{crd}/aum-history
# ---------------------------------------------------------------------------

@router.get(
    "/{crd}/aum-history",
    response_model=AumHistoryResponse,
    summary="AUM history for a firm",
    description="Returns per-filing AUM data points and an annual summary (peak, trough, latest) derived from the firm_aum_annual view.",
)
def get_aum_history(
    crd: int,
    db: Session = Depends(get_db),
) -> AumHistoryResponse:
    try:
        filings = firm_service.get_aum_history(crd, db)
        annual_rows = firm_service.get_aum_annual(crd, db)
        return AumHistoryResponse(
            crd_number=crd,
            annual=[AumAnnualSummary(**r) for r in annual_rows],
            filings=[AumHistoryPoint.model_validate(r) for r in filings],
        )
    except HTTPException:
        raise
    except Exception:
        log.error("get_aum_history(%s) error\n%s", crd, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/{crd}/brochures",
    response_model=list[BrochureMeta],
    summary="Brochure list for a firm",
)
def get_firm_brochures(crd: int, db: Session = Depends(get_db)):
    from models.brochure import AdvBrochure
    from sqlalchemy import desc, select

    # Raises 404 if firm not found (same as get_firm)
    firm_service.get_firm(crd, db)

    brochures = list(db.scalars(
        select(AdvBrochure)
        .where(AdvBrochure.crd_number == crd)
        .order_by(desc(AdvBrochure.date_submitted))
    ).all())
    return [BrochureMeta.model_validate(b) for b in brochures]


# ---------------------------------------------------------------------------
# GET /api/firms/{crd}/brochures/{version_id}/download
# ---------------------------------------------------------------------------

@router.get("/{crd}/brochures/{version_id}/download", summary="Stream a stored brochure PDF")
def download_brochure(crd: int, version_id: int, db: Session = Depends(get_db)):
    from models.brochure import AdvBrochure
    from services.storage_backends import get_active_backend, key_from_uri
    from sqlalchemy import select

    row = db.scalars(
        select(AdvBrochure).where(
            AdvBrochure.crd_number == crd,
            AdvBrochure.brochure_version_id == version_id,
        ).limit(1)
    ).first()

    if row is None:
        raise HTTPException(status_code=404, detail="Brochure not found")

    backend = get_active_backend(db)
    scheme, key = key_from_uri(row.file_path)

    try:
        stream = backend.stream(key)
    except Exception as exc:
        log.error("download_brochure: failed to stream version_id=%d: %s", version_id, exc)
        raise HTTPException(status_code=500, detail="Brochure file unavailable")

    safe_name = (row.brochure_name or f"brochure_{version_id}").replace('"', "")
    filename = f"{safe_name}.pdf"

    return StreamingResponse(
        stream,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# POST /api/firms/{crd}/brochures/{version_id}/parse
# GET  /api/firms/{crd}/brochures/{version_id}/parsed
# ---------------------------------------------------------------------------

@router.post(
    "/{crd}/brochures/{version_id}/parse",
    summary="Parse a brochure PDF via Reducto and store the result",
)
def parse_brochure_endpoint(crd: int, version_id: int, db: Session = Depends(get_db)):
    from services import reducto_service

    firm_service.get_firm(crd, db)  # raises 404 if firm not found

    try:
        result = reducto_service.parse_brochure(crd, version_id, db)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except RuntimeError as exc:
        # Configuration errors (no api key, integration disabled)
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        log.error("parse_brochure(%s, %s) error\n%s", crd, version_id, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Parse failed")

    if result.parse_status == "failed":
        # Persisted as failed but surface a 502 so the UI shows an error
        raise HTTPException(
            status_code=502,
            detail=result.parse_error or "Reducto returned an error",
        )
    return result


@router.get(
    "/{crd}/brochures/{version_id}/parsed",
    summary="Stored Reducto parse result for a brochure",
)
def get_brochure_parsed(crd: int, version_id: int, db: Session = Depends(get_db)):
    from models.brochure import AdvBrochure
    from schemas.reducto_settings import BrochureParsedContent
    from sqlalchemy import select

    row = db.scalars(
        select(AdvBrochure).where(
            AdvBrochure.crd_number == crd,
            AdvBrochure.brochure_version_id == version_id,
        ).limit(1)
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Brochure not found")

    return BrochureParsedContent(
        brochure_version_id=row.brochure_version_id,
        parse_status=row.parse_status,
        parsed_at=row.parsed_at,
        reducto_job_id=row.reducto_job_id,
        parse_error=row.parse_error,
        parsed_markdown=row.parsed_markdown,
        parsed_chunks=row.parsed_chunks,
    )


# ---------------------------------------------------------------------------
# GET /api/firms/{crd}/disclosures
# ---------------------------------------------------------------------------

@router.get(
    "/{crd}/disclosures",
    response_model=DisclosuresSummaryOut,
    summary="Disclosure counts for a firm",
)
def get_firm_disclosures(crd: int, db: Session = Depends(get_db)):
    from models.disclosures import FirmDisclosuresSummary

    firm_service.get_firm(crd, db)  # raises 404 if not found

    row = db.get(FirmDisclosuresSummary, crd)
    if row is None:
        return DisclosuresSummaryOut(
            crd_number=crd,
            criminal_count=0,
            regulatory_count=0,
            civil_count=0,
            customer_count=0,
            total_count=0,
            updated_at=None,
        )
    return DisclosuresSummaryOut(
        crd_number=row.crd_number,
        criminal_count=row.criminal_count,
        regulatory_count=row.regulatory_count,
        civil_count=row.civil_count,
        customer_count=row.customer_count,
        total_count=row.criminal_count + row.regulatory_count + row.civil_count + row.customer_count,
        updated_at=row.updated_at,
    )


# ---------------------------------------------------------------------------
# GET /api/firms/{crd}/business-profile
# ---------------------------------------------------------------------------

_BOOL_TRUE = {"Y", "Yes", "true", "1", True, 1}


def _extract_checked_labels(mapping: dict | None, keys: list[tuple[str, str]]) -> list[str]:
    """Return labels where the mapped key value is truthy/Y."""
    if not mapping:
        return []
    return [label for key, label in keys if mapping.get(key) in _BOOL_TRUE]


@router.get(
    "/{crd}/business-profile",
    response_model=BusinessProfileOut,
    summary="Business profile parsed from raw ADV data",
)
def get_firm_business_profile(crd: int, db: Session = Depends(get_db)):
    firm = firm_service.get_firm(crd, db)

    raw = firm.raw_adv
    if not raw:
        return BusinessProfileOut(
            available=False,
            client_types=[],
            compensation_types=[],
            investment_strategies=[],
            affiliations=[],
        )

    # Business profile requires FormInfo.Part1A from the old IAPD XML format.
    # The current public IAPD API (api.adviserinfo.sec.gov) returns summary data only
    # and does not include FormInfo.Part1A. If raw_adv lacks FormInfo entirely,
    # the data is unavailable regardless of what other keys are present.
    if "FormInfo" not in raw:
        return BusinessProfileOut(
            available=False,
            client_types=[],
            compensation_types=[],
            investment_strategies=[],
            affiliations=[],
        )

    def _get(*keys):
        d = raw
        for k in keys:
            if not isinstance(d, dict):
                return None
            d = d.get(k)
        return d

    # Item 5D — client types (Q5D1 is a dict of checkbox keys)
    client_map = _get("FormInfo", "Part1A", "Item5D", "Q5D1") or {}
    client_keys = [
        ("Individuals", "Individuals (other than high net worth individuals)"),
        ("HighNetWorth", "High net worth individuals"),
        ("BankingInstitutions", "Banking or thrift institutions"),
        ("InvestmentCompanies", "Investment companies"),
        ("BusinessDevelopmentCompanies", "Business development companies"),
        ("PooledInvestmentVehicles", "Pooled investment vehicles"),
        ("PensionProfitSharing", "Pension and profit-sharing plans"),
        ("CharitableOrganizations", "Charitable organizations"),
        ("StateOrMunicipalGovernment", "State or municipal government entities"),
        ("OtherInstitutionalClients", "Other institutional clients"),
        ("OtherClients", "Other"),
    ]
    client_types = _extract_checked_labels(client_map, client_keys)

    # Item 5E — compensation types (Q5E is a dict of checkbox keys)
    comp_map = _get("FormInfo", "Part1A", "Item5E", "Q5E") or {}
    comp_keys = [
        ("APercentageOfAssetsUnderManagement", "A percentage of assets under management"),
        ("HourlyCharges", "Hourly charges"),
        ("SubscriptionFees", "Subscription fees"),
        ("FixedFees", "Fixed fees"),
        ("Commissions", "Commissions"),
        ("PerformanceBasedFees", "Performance-based fees"),
        ("OtherFees", "Other"),
    ]
    compensation_types = _extract_checked_labels(comp_map, comp_keys)

    # Item 6 — investment strategy types (Q6A is a dict of checkbox keys)
    strategy_map = _get("FormInfo", "Part1A", "Item6", "Q6A") or {}
    strategy_keys = [
        ("LongTermPurchases", "Long-term purchases"),
        ("ShortTermPurchases", "Short-term purchases"),
        ("TradingShortSales", "Short sales"),
        ("MarginTransactions", "Margin transactions"),
        ("OptionWritingPurchasing", "Options"),
        ("FuturesContracts", "Futures contracts"),
        ("OtherMethods", "Other"),
    ]
    investment_strategies = _extract_checked_labels(strategy_map, strategy_keys)

    # Item 7 — financial industry affiliations (list of affiliation objects)
    item7 = _get("FormInfo", "Part1A", "Item7") or {}
    affiliations_raw = item7.get("Q7A") or []
    if isinstance(affiliations_raw, dict):
        affiliations_raw = [affiliations_raw]
    affiliations = [
        {"type": a.get("AffiliationType", ""), "name": a.get("AffiliationName", "")}
        for a in affiliations_raw
        if isinstance(a, dict) and (a.get("AffiliationType") or a.get("AffiliationName"))
    ]

    return BusinessProfileOut(
        client_types=client_types,
        compensation_types=compensation_types,
        investment_strategies=investment_strategies,
        affiliations=affiliations,
    )


# ---------------------------------------------------------------------------
# POST /api/firms/{crd}/refresh
# ---------------------------------------------------------------------------

@router.post("/{crd}/refresh", summary="Refresh firm data from live IAPD API")
def refresh_firm(crd: int, db: Session = Depends(get_db)):
    """
    Pull fresh data for a single firm from the live IAPD API, update raw_adv,
    detect field changes, and re-index to Elasticsearch.
    Runs synchronously — typically completes in 1–3 seconds.
    """
    from models.firm import Firm
    from services.firm_refresh_service import refresh_firm as _refresh

    firm_service.get_firm(crd, db)  # raises 404 if not found

    try:
        diffs = _refresh(crd, db)
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=f"IAPD lookup failed: {exc}")

    firm = db.get(Firm, crd)
    return {
        "crd_number": crd,
        "changed": len(diffs) > 0,
        "num_changes": len(diffs),
        "fields_changed": [d["field_path"] for d in diffs],
        "last_iapd_refresh_at": firm.last_iapd_refresh_at.isoformat() if firm and firm.last_iapd_refresh_at else None,
    }
