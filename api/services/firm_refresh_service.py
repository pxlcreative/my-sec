"""
Firm refresh service: pull live IAPD data, diff against last snapshot,
persist changes, and re-index to Elasticsearch.
"""
import logging
from datetime import date, datetime, timezone

from sqlalchemy.orm import Session

log = logging.getLogger(__name__)


def refresh_firm(crd: int, db: Session) -> list[dict]:
    """
    Full refresh pipeline for a single firm:

    1. Fetch raw IAPD JSON
    2. Extract canonical fields
    3. Compute SHA-256 hash of new state
    4. If hash changed vs last snapshot → save snapshot + change rows + update Firm row
    5. Insert FirmAumHistory row (source='iapd_live') using today as filing_date
    6. Re-index the firm to Elasticsearch (best-effort; errors are logged, not raised)
    7. Return list of diff dicts (empty if no change)
    """
    from models.aum import FirmAumHistory
    from models.firm import Firm
    from services.change_detector import (
        canonical_json,
        compute_diffs,
        get_current_snapshot,
        save_snapshot_and_changes,
        sha256_hash,
    )
    from services.iapd_client import extract_firm_fields, fetch_firm

    # 1. Fetch
    raw = fetch_firm(crd)

    # 2. Extract
    new_fields = extract_firm_fields(raw)

    # 3. Hash
    new_hash = sha256_hash(canonical_json(new_fields))

    # 4. Compare with last snapshot
    prev_snapshot = get_current_snapshot(crd, db)
    prev_hash = prev_snapshot.snapshot_hash if prev_snapshot else None

    diffs: list[dict] = []
    if new_hash != prev_hash:
        old_raw = prev_snapshot.raw_json if prev_snapshot else None
        diffs = compute_diffs(crd, old_raw, new_fields)

        save_snapshot_and_changes(
            crd=crd,
            new_hash=new_hash,
            raw_json=new_fields,
            diffs=diffs,
            db=db,
            prev_snapshot_id=prev_snapshot.id if prev_snapshot else None,
        )

        # Update the Firm row with fresh values (skip crd_number — it's the PK)
        firm: Firm | None = db.get(Firm, crd)
        if firm:
            updateable = {
                k: v for k, v in new_fields.items()
                if k != "crd_number" and hasattr(firm, k)
            }
            for attr, val in updateable.items():
                setattr(firm, attr, val)
            firm.raw_adv = raw

        db.commit()

        # Re-index to ES (best-effort)
        _reindex_firm(crd, new_fields, db)

        # Evaluate alert rules (best-effort)
        if diffs:
            try:
                from services.alert_service import evaluate_alerts_for_firm
                evaluate_alerts_for_firm(crd, diffs, db)
            except Exception as exc:
                log.warning("refresh_firm(%d): alert evaluation failed (non-fatal): %s", crd, exc)
    else:
        log.debug("refresh_firm(%d): no change (hash=%s)", crd, new_hash[:12])
        db.commit()  # commit the AUM history row added below

    # 5. AUM history — always insert a record for the current observation
    today = date.today()
    _upsert_aum_history(
        crd=crd,
        fields=new_fields,
        filing_date=today,
        db=db,
    )

    return diffs


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _upsert_aum_history(crd: int, fields: dict, filing_date: date, db: Session) -> None:
    """
    Insert a FirmAumHistory row for today's IAPD observation.
    Skips if a row for (crd, filing_date, 'iapd_live') already exists.
    """
    from models.aum import FirmAumHistory
    from sqlalchemy import select

    existing = db.scalars(
        select(FirmAumHistory)
        .where(
            FirmAumHistory.crd_number == crd,
            FirmAumHistory.filing_date == filing_date,
            FirmAumHistory.source == "iapd_live",
        )
        .limit(1)
    ).first()

    if existing:
        return

    db.add(FirmAumHistory(
        crd_number=crd,
        filing_date=filing_date,
        aum_total=fields.get("aum_total"),
        aum_discretionary=fields.get("aum_discretionary"),
        aum_non_discretionary=fields.get("aum_non_discretionary"),
        num_accounts=fields.get("num_accounts"),
        source="iapd_live",
    ))
    db.commit()


def _reindex_firm(crd: int, fields: dict, db: Session) -> None:
    """Push an updated firm document to Elasticsearch. Logs errors, never raises."""
    try:
        from services.es_client import bulk_index_firms
        from models.platform import FirmPlatform, PlatformDefinition
        from sqlalchemy import select

        plat_names = [
            row[0] for row in db.execute(
                select(PlatformDefinition.name)
                .join(FirmPlatform, FirmPlatform.platform_id == PlatformDefinition.id)
                .where(FirmPlatform.crd_number == crd)
            ).all()
        ]
        doc = {
            "crd_number":          crd,
            "legal_name":          fields.get("legal_name", ""),
            "business_name":       fields.get("business_name"),
            "main_street1":        fields.get("main_street1"),
            "main_city":           fields.get("main_city"),
            "main_state":          fields.get("main_state"),
            "main_zip":            fields.get("main_zip"),
            "registration_status": fields.get("registration_status"),
            "platforms":           plat_names,
        }
        bulk_index_firms([doc])
    except Exception as exc:
        log.warning("refresh_firm(%d): ES re-index failed (non-fatal): %s", crd, exc)
