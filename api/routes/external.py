"""
External-facing API (Module F) — requires Bearer API key authentication.

All routes share the get_api_key dependency which validates the key and
enforces Redis-backed rate limiting (100 req/min per key).

Endpoints:
  GET  /api/external/firms/{crd}/brochure         Latest PDF (local-first + SEC fallback)
  GET  /api/external/firms/{crd}/brochures        Brochure list for a firm
  GET  /api/external/brochures/{version_id}       Specific brochure PDF
  GET  /api/external/platforms/{id}/firms         Firms tagged to a platform
  GET  /api/external/platforms/{id}/brochures     Latest brochure per firm on a platform
  POST /api/external/match/bulk                   Bulk CRD matching (same logic as /api/match/bulk)
"""
import logging
from pathlib import Path

import requests as _requests
from fastapi import APIRouter, Depends, HTTPException
from fastapi.requests import Request
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from db import get_db
from schemas.match import BulkMatchRequest
from services.auth_service import make_api_key_dep

log = logging.getLogger(__name__)

# Single dependency instance used by every route in this router
_require_key = make_api_key_dep()

router = APIRouter(
    prefix="/external",
    tags=["external"],
    dependencies=[Depends(_require_key)],
)

_SEC_BROCHURE_URL = (
    "https://files.adviserinfo.sec.gov/IAPD/Content/Common/"
    "crd_iapd_Brochure.aspx?BRCHR_VRSN_ID={version_id}"
)
_PDF_MIME = "application/pdf"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_firm_or_404(crd: int, db: Session):
    from models.firm import Firm
    firm = db.get(Firm, crd)
    if firm is None:
        raise HTTPException(status_code=404, detail=f"Firm CRD {crd} not found")
    return firm


def _get_platform_or_404(platform_id: int, db: Session):
    from models.platform import PlatformDefinition
    plat = db.get(PlatformDefinition, platform_id)
    if plat is None:
        raise HTTPException(status_code=404, detail=f"Platform {platform_id} not found")
    return plat


def _latest_brochure(crd: int, db: Session):
    from models.brochure import AdvBrochure
    return db.scalars(
        select(AdvBrochure)
        .where(AdvBrochure.crd_number == crd)
        .order_by(desc(AdvBrochure.date_submitted))
        .limit(1)
    ).first()


def _stream_pdf_local(file_path: str, filename: str) -> StreamingResponse:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(file_path)

    def _iter():
        with open(path, "rb") as fh:
            while chunk := fh.read(65536):
                yield chunk

    return StreamingResponse(
        _iter(),
        media_type=_PDF_MIME,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _stream_pdf_sec_fallback(version_id: int, filename: str) -> Response:
    """Proxy-download the PDF from SEC's IAPD system and return it inline."""
    url = _SEC_BROCHURE_URL.format(version_id=version_id)
    log.info("PDF fallback: fetching %s", url)
    try:
        resp = _requests.get(url, timeout=30, stream=True)
        resp.raise_for_status()
        data = resp.content
    except Exception as exc:
        log.error("PDF fallback failed for version_id=%d: %s", version_id, exc)
        raise HTTPException(status_code=502, detail="Could not retrieve PDF from SEC.")
    return Response(
        content=data,
        media_type=_PDF_MIME,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _stream_pdf_by_uri(uri: str, filename: str, db: Session) -> StreamingResponse:
    """Dispatch to the correct backend based on the URI scheme."""
    from services.storage_backends import key_from_uri, get_active_backend

    scheme, key = key_from_uri(uri)
    if scheme == "local":
        # Absolute path — use existing local streamer unchanged
        return _stream_pdf_local(uri, filename)

    backend = get_active_backend(db)

    def _iter():
        yield from backend.stream(key)

    return StreamingResponse(
        _iter(),
        media_type=_PDF_MIME,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _serve_brochure(
    brochure, firm_name: str = "", db: Session | None = None
) -> StreamingResponse | Response:
    safe_name = (firm_name or "firm").replace(" ", "_")[:30]
    filename = f"{safe_name}_{brochure.brochure_version_id}.pdf"

    if brochure.file_path:
        try:
            return _stream_pdf_by_uri(brochure.file_path, filename, db)
        except Exception as exc:
            log.warning(
                "Storage read failed for version_id=%d (%s): %s — falling back to SEC",
                brochure.brochure_version_id, brochure.file_path, exc,
            )

    return _stream_pdf_sec_fallback(brochure.brochure_version_id, filename)


# ---------------------------------------------------------------------------
# GET /api/external/firms/{crd}/brochure  — latest brochure
# ---------------------------------------------------------------------------

@router.get("/firms/{crd}/brochure", summary="Latest ADV Part 2 brochure for a firm")
def get_latest_brochure(crd: int, db: Session = Depends(get_db)):
    firm = _get_firm_or_404(crd, db)
    brochure = _latest_brochure(crd, db)
    if brochure is None:
        raise HTTPException(status_code=404, detail=f"No brochures stored for CRD {crd}")
    return _serve_brochure(brochure, firm.legal_name, db)


# ---------------------------------------------------------------------------
# GET /api/external/firms/{crd}/brochures  — brochure list
# ---------------------------------------------------------------------------

@router.get("/firms/{crd}/brochures", summary="List all brochures for a firm")
def list_brochures(crd: int, request: Request, db: Session = Depends(get_db)):
    from models.brochure import AdvBrochure

    _get_firm_or_404(crd, db)

    brochures = list(db.scalars(
        select(AdvBrochure)
        .where(AdvBrochure.crd_number == crd)
        .order_by(desc(AdvBrochure.date_submitted))
    ).all())

    base = str(request.base_url).rstrip("/")
    return [
        {
            "version_id":    b.brochure_version_id,
            "brochure_name": b.brochure_name,
            "date_submitted": str(b.date_submitted) if b.date_submitted else None,
            "source_month":  b.source_month,
            "file_size_bytes": b.file_size_bytes,
            "download_url":  f"{base}/api/external/brochures/{b.brochure_version_id}",
        }
        for b in brochures
    ]


# ---------------------------------------------------------------------------
# GET /api/external/brochures/{version_id}  — specific brochure by version
# ---------------------------------------------------------------------------

@router.get("/brochures/{version_id}", summary="Download a specific brochure by version_id")
def get_brochure_by_version(version_id: int, db: Session = Depends(get_db)):
    from models.brochure import AdvBrochure

    brochure = db.scalars(
        select(AdvBrochure).where(AdvBrochure.brochure_version_id == version_id)
    ).first()

    if brochure is None:
        # Not in DB — attempt direct SEC fallback
        log.info("version_id=%d not in DB, attempting SEC fallback", version_id)
        return _stream_pdf_sec_fallback(version_id, f"brochure_{version_id}.pdf")

    return _serve_brochure(brochure, db=db)


# ---------------------------------------------------------------------------
# GET /api/external/platforms/{platform_id}/firms
# ---------------------------------------------------------------------------

@router.get("/platforms/{platform_id}/firms", summary="List firms tagged to a platform")
def platform_firms(platform_id: int, db: Session = Depends(get_db)):
    from models.firm import Firm
    from models.platform import FirmPlatform

    _get_platform_or_404(platform_id, db)

    firms = list(db.scalars(
        select(Firm)
        .join(FirmPlatform, FirmPlatform.crd_number == Firm.crd_number)
        .where(FirmPlatform.platform_id == platform_id)
        .order_by(Firm.legal_name)
    ).all())

    return [
        {
            "crd_number":          f.crd_number,
            "legal_name":          f.legal_name,
            "business_name":       f.business_name,
            "registration_status": f.registration_status,
            "aum_total":           f.aum_total,
            "main_city":           f.main_city,
            "main_state":          f.main_state,
            "last_filing_date":    str(f.last_filing_date) if f.last_filing_date else None,
        }
        for f in firms
    ]


# ---------------------------------------------------------------------------
# GET /api/external/platforms/{platform_id}/brochures
# ---------------------------------------------------------------------------

@router.get("/platforms/{platform_id}/brochures",
            summary="Latest brochure per firm on a platform")
def platform_brochures(platform_id: int, request: Request, db: Session = Depends(get_db)):
    from models.brochure import AdvBrochure
    from models.firm import Firm
    from models.platform import FirmPlatform
    from sqlalchemy import func

    _get_platform_or_404(platform_id, db)

    # Subquery: most recent date_submitted per CRD
    latest_sq = (
        select(
            AdvBrochure.crd_number,
            func.max(AdvBrochure.date_submitted).label("max_date"),
        )
        .join(FirmPlatform, FirmPlatform.crd_number == AdvBrochure.crd_number)
        .where(FirmPlatform.platform_id == platform_id)
        .group_by(AdvBrochure.crd_number)
        .subquery()
    )

    rows = db.execute(
        select(
            AdvBrochure.crd_number,
            Firm.legal_name,
            AdvBrochure.brochure_version_id,
            AdvBrochure.date_submitted,
        )
        .join(latest_sq, (latest_sq.c.crd_number == AdvBrochure.crd_number)
              & (latest_sq.c.max_date == AdvBrochure.date_submitted))
        .join(Firm, Firm.crd_number == AdvBrochure.crd_number)
        .order_by(Firm.legal_name)
    ).all()

    base = str(request.base_url).rstrip("/")
    return [
        {
            "crd_number":    r.crd_number,
            "firm_name":     r.legal_name,
            "version_id":    r.brochure_version_id,
            "date_submitted": str(r.date_submitted) if r.date_submitted else None,
            "download_url":  f"{base}/api/external/brochures/{r.brochure_version_id}",
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# POST /api/external/match/bulk
# ---------------------------------------------------------------------------

@router.post("/match/bulk", summary="Bulk name+address → CRD matching (external auth)")
def external_bulk_match(body: BulkMatchRequest, db: Session = Depends(get_db)):
    """
    Identical logic to POST /api/match/bulk.
    Auth and rate limiting enforced by the router-level dependency.
    """
    from routes.match import bulk_match
    return bulk_match(body, db)
