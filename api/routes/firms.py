import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from db import get_db
from schemas.firm import (
    AumHistoryPoint,
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

@router.get("", response_model=PaginatedFirms)
def list_firms(
    state: str | None = Query(None, description="2-letter state code"),
    aum_min: int | None = Query(None, ge=0),
    aum_max: int | None = Query(None, ge=0),
    registration_status: str | None = Query(None),
    platform_id: int | None = Query(None),
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
            platform_id=platform_id,
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

@router.get("/search", response_model=PaginatedFirms)
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

@router.get("/{crd}", response_model=FirmDetail)
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

@router.get("/{crd}/history", response_model=FirmHistoryResponse)
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

@router.get("/{crd}/aum-history", response_model=list[AumHistoryPoint])
def get_aum_history(
    crd: int,
    db: Session = Depends(get_db),
) -> list[AumHistoryPoint]:
    try:
        records = firm_service.get_aum_history(crd, db)
        return [AumHistoryPoint.model_validate(r) for r in records]
    except HTTPException:
        raise
    except Exception:
        log.error("get_aum_history(%s) error\n%s", crd, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
