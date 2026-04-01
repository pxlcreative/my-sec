from __future__ import annotations

import datetime

from pydantic import BaseModel, ConfigDict, Field


class StorageSettingsOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    backend: str
    s3_bucket: str | None
    s3_region: str | None
    s3_access_key_id: str | None
    s3_secret_access_key: str | None
    s3_endpoint_url: str | None
    azure_container: str | None
    azure_connection_string: str | None
    updated_at: datetime.datetime


class StorageSettingsPatch(BaseModel):
    backend: str | None = Field(None, pattern="^(local|s3|azure)$")
    s3_bucket: str | None = None
    s3_region: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_endpoint_url: str | None = None
    azure_container: str | None = None
    azure_connection_string: str | None = None


class StorageTestResult(BaseModel):
    success: bool
    backend: str
    message: str
