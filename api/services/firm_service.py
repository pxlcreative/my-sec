"""
Business logic for firm queries.  All functions accept a SQLAlchemy Session
and return ORM objects or plain dicts — no FastAPI coupling here.
"""
import logging
from dataclasses import dataclass, field

from sqlalchemy import desc, func, or_, select, text
from sqlalchemy.orm import Session, selectinload

from models.aum import FirmAumHistory
from models.brochure import AdvBrochure
from models.firm import Firm, FirmChange
from models.platform import FirmPlatform, PlatformDefinition

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _firm_not_found(crd: int) -> Exception:
    from fastapi import HTTPException
    raise HTTPException(status_code=404, detail=f"Firm CRD {crd} not found")


def _platform_names_for_crds(
    session: Session, crds: list[int]
) -> dict[int, list[str]]:
    """Return {crd_number: [platform_name, ...]} for a batch of CRDs."""
    if not crds:
        return {}
    rows = (
        session.execute(
            select(FirmPlatform.crd_number, PlatformDefinition.name)
            .join(PlatformDefinition, FirmPlatform.platform_id == PlatformDefinition.id)
            .where(FirmPlatform.crd_number.in_(crds))
        )
        .all()
    )
    result: dict[int, list[str]] = {c: [] for c in crds}
    for crd, name in rows:
        result[crd].append(name)
    return result


# ---------------------------------------------------------------------------
# Filters dataclass
# ---------------------------------------------------------------------------

@dataclass
class FirmFilters:
    state: str | None = None
    aum_min: int | None = None
    aum_max: int | None = None
    registration_status: str | None = None
    platform_id: int | None = None
    search_query: str | None = None


# ---------------------------------------------------------------------------
# get_firm
# ---------------------------------------------------------------------------

def get_firm(crd: int, session: Session) -> Firm:
    firm = session.get(Firm, crd)
    if firm is None:
        _firm_not_found(crd)
    return firm  # type: ignore[return-value]  # _firm_not_found always raises


# ---------------------------------------------------------------------------
# list_firms
# ---------------------------------------------------------------------------

def list_firms(
    filters: FirmFilters,
    page: int,
    page_size: int,
    session: Session,
) -> tuple[int, list[Firm]]:
    """
    Returns (total_count, firms_for_page).
    Applies filters in-DB; joins platform when platform_id is set.
    """
    stmt = select(Firm)

    if filters.state:
        stmt = stmt.where(Firm.main_state == filters.state.upper())
    if filters.registration_status:
        stmt = stmt.where(Firm.registration_status == filters.registration_status)
    if filters.aum_min is not None:
        stmt = stmt.where(Firm.aum_total >= filters.aum_min)
    if filters.aum_max is not None:
        stmt = stmt.where(Firm.aum_total <= filters.aum_max)
    if filters.platform_id is not None:
        stmt = stmt.join(
            FirmPlatform,
            (FirmPlatform.crd_number == Firm.crd_number)
            & (FirmPlatform.platform_id == filters.platform_id),
        )
    if filters.search_query:
        q = filters.search_query.strip()
        tsquery = func.plainto_tsquery("english", q)
        stmt = stmt.where(
            or_(
                func.to_tsvector("english", Firm.legal_name).op("@@")(tsquery),
                func.to_tsvector(
                    "english", func.coalesce(Firm.business_name, "")
                ).op("@@")(tsquery),
            )
        )

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total: int = session.scalar(count_stmt) or 0

    offset = (page - 1) * page_size
    firms = session.scalars(
        stmt.order_by(desc(Firm.aum_total)).offset(offset).limit(page_size)
    ).all()

    return total, list(firms)


# ---------------------------------------------------------------------------
# search_firms  (GIN full-text)
# ---------------------------------------------------------------------------

def search_firms(
    q: str,
    page: int,
    page_size: int,
    session: Session,
) -> tuple[int, list[Firm]]:
    """
    Full-text search over legal_name and business_name using the GIN index.
    Falls back to ILIKE if the query produces no tsquery tokens.
    """
    q = q.strip()
    tsquery = func.plainto_tsquery("english", q)
    ts_filter = or_(
        func.to_tsvector("english", Firm.legal_name).op("@@")(tsquery),
        func.to_tsvector(
            "english", func.coalesce(Firm.business_name, "")
        ).op("@@")(tsquery),
    )
    # ILIKE fallback for short/stop-word queries
    like_filter = or_(
        Firm.legal_name.ilike(f"%{q}%"),
        Firm.business_name.ilike(f"%{q}%"),
    )
    combined = or_(ts_filter, like_filter)

    stmt = select(Firm).where(combined)
    total: int = session.scalar(select(func.count()).select_from(stmt.subquery())) or 0

    offset = (page - 1) * page_size
    firms = session.scalars(
        stmt.order_by(
            func.ts_rank(
                func.to_tsvector("english", Firm.legal_name), tsquery
            ).desc()
        )
        .offset(offset)
        .limit(page_size)
    ).all()

    return total, list(firms)


# ---------------------------------------------------------------------------
# get_firm_history
# ---------------------------------------------------------------------------

def get_firm_history(crd: int, session: Session) -> list[FirmChange]:
    get_firm(crd, session)  # raises 404 if missing
    return list(
        session.scalars(
            select(FirmChange)
            .where(FirmChange.crd_number == crd)
            .order_by(desc(FirmChange.detected_at))
            .limit(500)
        ).all()
    )


# ---------------------------------------------------------------------------
# get_aum_history
# ---------------------------------------------------------------------------

def get_aum_history(crd: int, session: Session) -> list[FirmAumHistory]:
    get_firm(crd, session)  # raises 404 if missing
    return list(
        session.scalars(
            select(FirmAumHistory)
            .where(FirmAumHistory.crd_number == crd)
            .order_by(FirmAumHistory.filing_date)
        ).all()
    )


# ---------------------------------------------------------------------------
# get_latest_brochure
# ---------------------------------------------------------------------------

def get_latest_brochure(crd: int, session: Session) -> AdvBrochure | None:
    return session.scalars(
        select(AdvBrochure)
        .where(AdvBrochure.crd_number == crd)
        .order_by(desc(AdvBrochure.date_submitted))
        .limit(1)
    ).first()
