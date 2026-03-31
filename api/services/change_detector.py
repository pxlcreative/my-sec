"""
Snapshot diffing and change persistence for Firm records.

Workflow:
  1. canonical_json(new_fields) → deterministic string
  2. sha256_hash(s) → hex digest
  3. Compare against get_current_snapshot(crd, db).snapshot_hash
  4. compute_diffs(crd, old_snapshot_json, new_fields) → list of changes
  5. save_snapshot_and_changes(...)
"""
import hashlib
import json
import logging
from datetime import datetime, timezone

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

# Fields diffed on every refresh (match spec + all materialized in Firm)
DIFF_FIELDS = [
    "registration_status",
    "aum_total",
    "aum_discretionary",
    "aum_non_discretionary",
    "legal_name",
    "business_name",
    "main_city",
    "main_state",
    "main_zip",
    "num_accounts",
    "num_employees",
]


# ---------------------------------------------------------------------------
# a. Deterministic JSON serialization
# ---------------------------------------------------------------------------

def canonical_json(d: dict) -> str:
    """Serialize *d* to a deterministic JSON string (sorted keys, no whitespace)."""
    return json.dumps(d, sort_keys=True, separators=(",", ":"), default=str)


# ---------------------------------------------------------------------------
# b. SHA-256
# ---------------------------------------------------------------------------

def sha256_hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# c. Latest snapshot lookup
# ---------------------------------------------------------------------------

def get_current_snapshot(crd: int, db: Session):
    """Return the most recent FirmSnapshot for *crd*, or None."""
    from models.firm import FirmSnapshot
    return db.scalars(
        select(FirmSnapshot)
        .where(FirmSnapshot.crd_number == crd)
        .order_by(desc(FirmSnapshot.synced_at))
        .limit(1)
    ).first()


# ---------------------------------------------------------------------------
# d. Field-by-field diff
# ---------------------------------------------------------------------------

def compute_diffs(
    crd: int,
    old_snapshot_json: dict | None,
    new_fields: dict,
) -> list[dict]:
    """
    Compare DIFF_FIELDS between *old_snapshot_json* (the raw_json stored in the
    previous FirmSnapshot) and *new_fields* (output of extract_firm_fields).

    Returns a list of dicts:  [{field_path, old_value, new_value}, ...]
    Only fields that actually changed are included.
    """
    diffs = []
    old = old_snapshot_json or {}

    for field in DIFF_FIELDS:
        old_val = old.get(field)
        new_val = new_fields.get(field)

        # Normalise to string for comparison (None stays None)
        old_str = str(old_val) if old_val is not None else None
        new_str = str(new_val) if new_val is not None else None

        if old_str != new_str:
            diffs.append({
                "field_path": field,
                "old_value":  old_str,
                "new_value":  new_str,
            })

    if diffs:
        log.info("compute_diffs: CRD %d — %d field(s) changed: %s",
                 crd, len(diffs), [d["field_path"] for d in diffs])
    return diffs


# ---------------------------------------------------------------------------
# e. Persist snapshot + changes
# ---------------------------------------------------------------------------

def save_snapshot_and_changes(
    crd: int,
    new_hash: str,
    raw_json: dict,
    diffs: list[dict],
    db: Session,
    prev_snapshot_id: int | None = None,
) -> int:
    """
    Insert a new FirmSnapshot and FirmChange rows for each diff.
    Returns the new snapshot id.
    """
    from models.firm import FirmChange, FirmSnapshot

    snapshot = FirmSnapshot(
        crd_number=crd,
        snapshot_hash=new_hash,
        raw_json=raw_json,
        synced_at=datetime.now(timezone.utc),
    )
    db.add(snapshot)
    db.flush()  # assign snapshot.id before referencing it in FirmChange rows

    now = datetime.now(timezone.utc)
    for diff in diffs:
        db.add(FirmChange(
            crd_number=crd,
            field_path=diff["field_path"],
            old_value=diff["old_value"],
            new_value=diff["new_value"],
            detected_at=now,
            snapshot_from=prev_snapshot_id,
            snapshot_to=snapshot.id,
        ))

    return snapshot.id
