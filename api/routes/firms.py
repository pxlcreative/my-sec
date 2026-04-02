import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from db import get_db
from schemas.firm import (
    AumAnnualSummary,
    AumHistoryPoint,
    AumHistoryResponse,
    BrochureMeta,
    ChangeRecord,
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
        total, firms = firm_service.list_firms(filters, page, page_size, db)
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
