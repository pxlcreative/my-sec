"""
GET /api/firms/{crd}/due-diligence-excel
Returns a pre-populated Due Diligence Excel workbook for the requested firm.
"""
import io
import logging
import re
from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from db import get_db
from services.excel_generator import build_dd_workbook

log = logging.getLogger(__name__)
router = APIRouter(prefix="/firms", tags=["firms"])

_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _safe_name(s: str, max_len: int = 30) -> str:
    """Strip non-alphanumeric chars, truncate, replace spaces with underscores."""
    clean = re.sub(r"[^\w\s-]", "", s).strip()
    return clean[:max_len].replace(" ", "_")


@router.get("/{crd}/due-diligence-excel", summary="Download Due Diligence Excel workbook")
def due_diligence_excel(crd: int, db: Session = Depends(get_db)):
    from models.aum import FirmAumHistory
    from models.disclosures import FirmDisclosuresSummary
    from models.firm import Firm
    from services.firm_service import get_firm

    firm = get_firm(crd, db)   # raises 404 if not found

    # Load AUM history ordered chronologically
    aum_history = list(db.scalars(
        select(FirmAumHistory)
        .where(FirmAumHistory.crd_number == crd)
        .order_by(FirmAumHistory.filing_date)
    ).all())

    # Load disclosures (may be None)
    disclosures = db.get(FirmDisclosuresSummary, crd)

    try:
        wb = build_dd_workbook(firm, aum_history, disclosures)
    except Exception as exc:
        log.exception("due_diligence_excel(%d): workbook build failed", crd)
        raise HTTPException(status_code=500, detail="Failed to generate workbook")

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    today_str = date.today().strftime("%Y%m%d")
    name_part = _safe_name(firm.legal_name or "")
    filename  = f"DDQ_{crd}_{name_part}_{today_str}.xlsx"

    return StreamingResponse(
        buf,
        media_type=_MIME,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
