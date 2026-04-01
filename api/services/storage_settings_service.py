from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from models.storage_settings import StorageSettings
from schemas.storage_settings import StorageSettingsPatch, StorageTestResult

_MASKED_SENTINEL = "***"
_SECRET_FIELDS = {"s3_secret_access_key", "azure_connection_string"}


def get_settings(db: Session) -> StorageSettings:
    row: StorageSettings | None = db.get(StorageSettings, 1)
    if row is None:
        row = StorageSettings(id=1, backend="local")
        db.add(row)
        db.commit()
        db.refresh(row)
    return row


def update_settings(db: Session, patch: StorageSettingsPatch) -> StorageSettings:
    row = get_settings(db)
    for field, value in patch.model_dump(exclude_none=True).items():
        # Don't overwrite real secrets with the masked placeholder
        if field in _SECRET_FIELDS and value == _MASKED_SENTINEL:
            continue
        setattr(row, field, value)
    db.commit()
    db.refresh(row)
    return row


def test_connection(db: Session) -> StorageTestResult:
    from services.storage_backends import get_active_backend

    row = get_settings(db)
    backend = get_active_backend(db)
    test_key = f"_test/{uuid.uuid4().hex}.txt"
    test_data = b"mysec-storage-test"

    try:
        backend.put(test_key, test_data)
        retrieved = backend.get(test_key)
        assert retrieved == test_data, "Round-trip data mismatch"
        backend.delete(test_key)
        return StorageTestResult(
            success=True,
            backend=row.backend,
            message=f"Connection to '{row.backend}' backend verified successfully.",
        )
    except Exception as exc:
        return StorageTestResult(
            success=False,
            backend=row.backend,
            message=str(exc),
        )
