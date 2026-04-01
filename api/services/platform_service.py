"""
Platform CRUD and firm-tagging service.
All functions accept a SQLAlchemy Session; no FastAPI imports here.
"""
import logging

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from models.firm import Firm
from models.platform import FirmPlatform, PlatformDefinition

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_platform(platform_id: int, session: Session) -> PlatformDefinition:
    p = session.get(PlatformDefinition, platform_id)
    if p is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Platform {platform_id} not found")
    return p


def _require_firm(crd: int, session: Session) -> Firm:
    f = session.get(Firm, crd)
    if f is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Firm CRD {crd} not found")
    return f


def _es_update_firm_platforms(crd: int, session: Session) -> None:
    """Push current platform list for crd into its ES document (best-effort)."""
    try:
        from services.es_client import FIRMS_INDEX, get_client
        names = [
            row[0]
            for row in session.execute(
                select(PlatformDefinition.name)
                .join(FirmPlatform, FirmPlatform.platform_id == PlatformDefinition.id)
                .where(FirmPlatform.crd_number == crd)
            ).all()
        ]
        get_client().update(
            index=FIRMS_INDEX,
            id=str(crd),
            doc={"platforms": names},
            retry_on_conflict=3,
        )
    except Exception as exc:
        log.warning("ES platform update failed for CRD %s: %s", crd, exc)


# ---------------------------------------------------------------------------
# Platform definitions
# ---------------------------------------------------------------------------

def list_platforms(session: Session) -> list[PlatformDefinition]:
    return list(session.scalars(
        select(PlatformDefinition).order_by(PlatformDefinition.name)
    ).all())


def delete_platform(platform_id: int, session: Session) -> None:
    platform = _require_platform(platform_id, session)
    # Remove all firm-platform tags first to satisfy FK constraint
    session.execute(delete(FirmPlatform).where(FirmPlatform.platform_id == platform_id))
    session.delete(platform)
    session.commit()


def create_platform(
    name: str, description: str | None, session: Session
) -> PlatformDefinition:
    existing = session.scalars(
        select(PlatformDefinition).where(PlatformDefinition.name == name)
    ).first()
    if existing:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=409, detail=f"Platform '{name}' already exists"
        )
    p = PlatformDefinition(name=name, description=description)
    session.add(p)
    session.commit()
    session.refresh(p)
    return p


# ---------------------------------------------------------------------------
# Firm ↔ platform queries
# ---------------------------------------------------------------------------

def get_firms_for_platform(
    platform_id: int,
    page: int,
    page_size: int,
    session: Session,
) -> tuple[int, list[Firm]]:
    _require_platform(platform_id, session)
    stmt = (
        select(Firm)
        .join(FirmPlatform, FirmPlatform.crd_number == Firm.crd_number)
        .where(FirmPlatform.platform_id == platform_id)
    )
    from sqlalchemy import func
    total: int = session.scalar(
        select(func.count()).select_from(stmt.subquery())
    ) or 0
    offset = (page - 1) * page_size
    firms = list(session.scalars(
        stmt.order_by(Firm.legal_name).offset(offset).limit(page_size)
    ).all())
    return total, firms


def get_firm_platforms(crd: int, session: Session) -> list[dict]:
    """Return list of {id, platform_id, platform_name, tagged_at, tagged_by, notes}."""
    _require_firm(crd, session)
    rows = session.execute(
        select(
            FirmPlatform.id,
            FirmPlatform.platform_id,
            PlatformDefinition.name.label("platform_name"),
            FirmPlatform.tagged_at,
            FirmPlatform.tagged_by,
            FirmPlatform.notes,
        )
        .join(PlatformDefinition, FirmPlatform.platform_id == PlatformDefinition.id)
        .where(FirmPlatform.crd_number == crd)
        .order_by(PlatformDefinition.name)
    ).all()
    return [r._asdict() for r in rows]


# ---------------------------------------------------------------------------
# Firm platform mutations
# ---------------------------------------------------------------------------

def set_firm_platforms(
    crd: int,
    platform_ids: list[int],
    tagged_by: str | None,
    notes: str | None,
    session: Session,
) -> list[dict]:
    """
    Replace all platform tags for a firm with the supplied list.
    Returns the new tag list.
    """
    _require_firm(crd, session)
    # Validate all IDs exist upfront
    for pid in platform_ids:
        _require_platform(pid, session)

    session.execute(
        delete(FirmPlatform).where(FirmPlatform.crd_number == crd)
    )
    for pid in platform_ids:
        session.add(FirmPlatform(
            crd_number=crd,
            platform_id=pid,
            tagged_by=tagged_by,
            notes=notes,
        ))
    session.commit()
    _es_update_firm_platforms(crd, session)
    return get_firm_platforms(crd, session)


def remove_firm_platform(crd: int, platform_id: int, session: Session) -> None:
    """Delete one FirmPlatform row. Raises 404 if it doesn't exist."""
    row = session.scalars(
        select(FirmPlatform)
        .where(
            FirmPlatform.crd_number == crd,
            FirmPlatform.platform_id == platform_id,
        )
    ).first()
    if row is None:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=404,
            detail=f"Firm {crd} is not tagged with platform {platform_id}",
        )
    session.delete(row)
    session.commit()
    _es_update_firm_platforms(crd, session)


def bulk_tag_firms(
    records: list[dict],   # [{crd_number, platform_id}]
    tagged_by: str | None,
    notes: str | None,
    session: Session,
) -> tuple[int, int]:
    """
    Insert FirmPlatform rows; skip on duplicate (ON CONFLICT DO NOTHING).
    Returns (inserted, skipped).
    """
    inserted = 0
    skipped = 0
    # Collect unique CRDs for ES update after commit
    touched_crds: set[int] = set()

    for rec in records:
        crd = rec["crd_number"]
        pid = rec["platform_id"]
        # Use a nested savepoint so one dupe doesn't poison the whole session
        try:
            with session.begin_nested():
                session.add(FirmPlatform(
                    crd_number=crd,
                    platform_id=pid,
                    tagged_by=tagged_by,
                    notes=notes,
                ))
            inserted += 1
            touched_crds.add(crd)
        except IntegrityError:
            skipped += 1

    session.commit()

    for crd in touched_crds:
        _es_update_firm_platforms(crd, session)

    return inserted, skipped
