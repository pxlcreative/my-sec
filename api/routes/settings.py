"""
Settings routes.

Storage:
  GET  /api/settings/storage       → current storage backend config (secrets masked)
  PATCH /api/settings/storage      → update storage backend config
  POST /api/settings/storage/test  → test connection round-trip

Reducto (PDF parsing):
  GET  /api/settings/reducto       → current Reducto config (api_key masked)
  PATCH /api/settings/reducto      → update Reducto config
  POST /api/settings/reducto/test  → verify Reducto API key
"""
from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from db import get_db
from schemas.reducto_settings import (
    ReductoSettingsOut,
    ReductoSettingsPatch,
    ReductoTestResult,
)
from schemas.storage_settings import StorageSettingsOut, StorageSettingsPatch, StorageTestResult
from services import reducto_service, storage_settings_service

log = logging.getLogger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])

DbDep = Annotated[Session, Depends(get_db)]

_SECRET_FIELDS = ("s3_secret_access_key", "azure_connection_string")


def _mask_secrets(out: StorageSettingsOut) -> StorageSettingsOut:
    """Replace non-None secret values with '***' before returning to client."""
    data = out.model_dump()
    for field in _SECRET_FIELDS:
        if data.get(field) is not None:
            data[field] = "***"
    return StorageSettingsOut(**data)


@router.get("/storage", response_model=StorageSettingsOut)
def get_storage_settings(db: DbDep = None):
    row = storage_settings_service.get_settings(db)
    return _mask_secrets(StorageSettingsOut.model_validate(row))


@router.patch("/storage", response_model=StorageSettingsOut)
def update_storage_settings(body: StorageSettingsPatch, db: DbDep = None):
    row = storage_settings_service.update_settings(db, body)
    return _mask_secrets(StorageSettingsOut.model_validate(row))


@router.post("/storage/test", response_model=StorageTestResult)
def test_storage_connection(db: DbDep = None):
    return storage_settings_service.test_connection(db)


# ---------------------------------------------------------------------------
# Reducto
# ---------------------------------------------------------------------------

def _mask_reducto(out: ReductoSettingsOut) -> ReductoSettingsOut:
    data = out.model_dump()
    if data.get("api_key"):
        data["api_key"] = "***"
    return ReductoSettingsOut(**data)


@router.get("/reducto", response_model=ReductoSettingsOut)
def get_reducto_settings(db: DbDep = None):
    row = reducto_service.get_settings(db)
    return _mask_reducto(ReductoSettingsOut.model_validate(row))


@router.patch("/reducto", response_model=ReductoSettingsOut)
def update_reducto_settings(body: ReductoSettingsPatch, db: DbDep = None):
    row = reducto_service.update_settings(db, body)
    return _mask_reducto(ReductoSettingsOut.model_validate(row))


@router.post("/reducto/test", response_model=ReductoTestResult)
def test_reducto_connection(db: DbDep = None):
    return reducto_service.test_connection(db)
