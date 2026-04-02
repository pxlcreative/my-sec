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
    platform_ids: list[int] = field(default_factory=list)
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
    if filters.platform_ids:
        stmt = stmt.join(
            FirmPlatform,
            (FirmPlatform.crd_number == Firm.crd_number)
            & (FirmPlatform.platform_id.in_(filters.platform_ids)),
        ).distinct()
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
# search_firms  (Elasticsearch primary, Postgres GIN fallback)
# ---------------------------------------------------------------------------

def _search_firms_postgres(
    q: str,
    page: int,
    page_size: int,
    session: Session,
) -> tuple[int, list[Firm]]:
    """Postgres GIN full-text search (used when ES is unavailable)."""
    q = q.strip()
    tsquery = func.plainto_tsquery("english", q)
    ts_filter = or_(
        func.to_tsvector("english", Firm.legal_name).op("@@")(tsquery),
        func.to_tsvector(
            "english", func.coalesce(Firm.business_name, "")
        ).op("@@")(tsquery),
    )
    like_filter = or_(
        Firm.legal_name.ilike(f"%{q}%"),
        Firm.business_name.ilike(f"%{q}%"),
    )
    stmt = select(Firm).where(or_(ts_filter, like_filter))
    total: int = session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    offset = (page - 1) * page_size
    firms = session.scalars(
        stmt.order_by(
            func.ts_rank(func.to_tsvector("english", Firm.legal_name), tsquery).desc()
        )
        .offset(offset)
        .limit(page_size)
    ).all()
    return total, list(firms)


def search_firms(
    q: str,
    page: int,
    page_size: int,
    session: Session,
    city: str | None = None,
    state: str | None = None,
) -> tuple[int, list[Firm]]:
    """
    Primary: Elasticsearch fuzzy multi-match.
    Fallback: Postgres GIN full-text + ILIKE when ES is unreachable.

    Returns (total_count, Firm ORM objects in ES score order).
    """
    try:
        from services.es_client import search_firms as es_search
        # ES doesn't give a count, so we request enough results and paginate in Python.
        es_size = page * page_size
        hits = es_search(q, city=city, state=state, size=min(es_size, 500))
        if not hits:
            return 0, []

        # Slice for the requested page
        offset = (page - 1) * page_size
        page_hits = hits[offset: offset + page_size]
        crds = [h["crd_number"] for h in page_hits]

        # Fetch full ORM objects, preserving ES score order
        crd_to_firm: dict[int, Firm] = {
            f.crd_number: f
            for f in session.scalars(select(Firm).where(Firm.crd_number.in_(crds))).all()
        }
        firms = [crd_to_firm[c] for c in crds if c in crd_to_firm]
        return len(hits), firms

    except Exception as exc:
        log.warning("Elasticsearch unavailable (%s), falling back to Postgres", exc)
        return _search_firms_postgres(q, page, page_size, session)


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
# get_aum_annual
# ---------------------------------------------------------------------------

def get_aum_annual(crd: int, session: Session) -> list[dict]:
    """Return rows from firm_aum_annual for *crd*, ordered by year ASC."""
    get_firm(crd, session)  # raises 404 if missing
    rows = session.execute(
        text(
            "SELECT year, peak_aum, trough_aum, latest_aum_for_year, filing_count "
            "FROM firm_aum_annual WHERE crd_number = :crd ORDER BY year"
        ),
        {"crd": crd},
    ).mappings().all()
    return [dict(r) for r in rows]


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
