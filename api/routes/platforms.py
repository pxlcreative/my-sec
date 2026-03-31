import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from db import get_db
from schemas.firm import FirmSummary, PaginatedFirms
from schemas.platform import (
    BulkTagRequest,
    BulkTagResponse,
    FirmPlatformTag,
    PlatformCreate,
    PlatformOut,
    SetFirmPlatformsRequest,
)
from services import firm_service, platform_service

log = logging.getLogger(__name__)

# Two separate routers — mounted at different prefixes in main.py
platforms_router = APIRouter(prefix="/platforms", tags=["platforms"])
firm_platforms_router = APIRouter(prefix="/firms", tags=["platforms"])
match_router = APIRouter(prefix="/match", tags=["platforms"])


# ---------------------------------------------------------------------------
# GET /api/platforms
# ---------------------------------------------------------------------------

@platforms_router.get("", response_model=list[PlatformOut], summary="List all platforms")
def list_platforms(db: Session = Depends(get_db)) -> list[PlatformOut]:
    try:
        rows = platform_service.list_platforms(db)
        return [PlatformOut.model_validate(r) for r in rows]
    except HTTPException:
        raise
    except Exception:
        log.error("list_platforms error\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# POST /api/platforms
# ---------------------------------------------------------------------------

@platforms_router.post("", response_model=PlatformOut, status_code=201, summary="Create a platform")
def create_platform(
    body: PlatformCreate, db: Session = Depends(get_db)
) -> PlatformOut:
    try:
        p = platform_service.create_platform(body.name, body.description, db)
        return PlatformOut.model_validate(p)
    except HTTPException:
        raise
    except Exception:
        log.error("create_platform error\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# GET /api/platforms/{id}/firms
# ---------------------------------------------------------------------------

@platforms_router.get("/{platform_id}/firms", response_model=PaginatedFirms)
def get_firms_for_platform(
    platform_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> PaginatedFirms:
    try:
        total, firms = platform_service.get_firms_for_platform(
            platform_id, page, page_size, db
        )
        crds = [f.crd_number for f in firms]
        platform_map = firm_service._platform_names_for_crds(db, crds)
        results = [
            FirmSummary(
                crd_number=f.crd_number,
                legal_name=f.legal_name,
                business_name=f.business_name,
                main_city=f.main_city,
                main_state=f.main_state,
                aum_total=f.aum_total,
                registration_status=f.registration_status,
                last_filing_date=f.last_filing_date,
                platforms=platform_map.get(f.crd_number, []),
            )
            for f in firms
        ]
        return PaginatedFirms(total=total, page=page, page_size=page_size, results=results)
    except HTTPException:
        raise
    except Exception:
        log.error("get_firms_for_platform error\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# GET /api/firms/{crd}/platforms
# ---------------------------------------------------------------------------

@firm_platforms_router.get("/{crd}/platforms", response_model=list[FirmPlatformTag])
def get_firm_platforms(crd: int, db: Session = Depends(get_db)) -> list[FirmPlatformTag]:
    try:
        rows = platform_service.get_firm_platforms(crd, db)
        return [FirmPlatformTag(**r) for r in rows]
    except HTTPException:
        raise
    except Exception:
        log.error("get_firm_platforms(%s) error\n%s", crd, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# PUT /api/firms/{crd}/platforms
# ---------------------------------------------------------------------------

@firm_platforms_router.put("/{crd}/platforms", response_model=list[FirmPlatformTag])
def set_firm_platforms(
    crd: int,
    body: SetFirmPlatformsRequest,
    db: Session = Depends(get_db),
) -> list[FirmPlatformTag]:
    try:
        rows = platform_service.set_firm_platforms(
            crd, body.platform_ids, body.tagged_by, body.notes, db
        )
        return [FirmPlatformTag(**r) for r in rows]
    except HTTPException:
        raise
    except Exception:
        log.error("set_firm_platforms(%s) error\n%s", crd, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# DELETE /api/firms/{crd}/platforms/{platform_id}
# ---------------------------------------------------------------------------

@firm_platforms_router.delete(
    "/{crd}/platforms/{platform_id}", status_code=204
)
def remove_firm_platform(
    crd: int, platform_id: int, db: Session = Depends(get_db)
) -> None:
    try:
        platform_service.remove_firm_platform(crd, platform_id, db)
    except HTTPException:
        raise
    except Exception:
        log.error(
            "remove_firm_platform(%s, %s) error\n%s", crd, platform_id,
            traceback.format_exc()
        )
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# POST /api/match/bulk-tag
# ---------------------------------------------------------------------------

@match_router.post("/bulk-tag", response_model=BulkTagResponse, status_code=200)
def bulk_tag(
    body: BulkTagRequest, db: Session = Depends(get_db)
) -> BulkTagResponse:
    try:
        records = [r.model_dump() for r in body.records]
        inserted, skipped = platform_service.bulk_tag_firms(
            records, body.tagged_by, body.notes, db
        )
        return BulkTagResponse(inserted=inserted, skipped=skipped)
    except HTTPException:
        raise
    except Exception:
        log.error("bulk_tag error\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
