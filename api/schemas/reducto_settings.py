from __future__ import annotations

import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class ReductoSettingsOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    api_key: str | None
    base_url: str
    enabled: bool
    updated_at: datetime.datetime


class ReductoSettingsPatch(BaseModel):
    api_key: str | None = None
    base_url: str | None = None
    enabled: bool | None = None


class ReductoTestResult(BaseModel):
    success: bool
    message: str


class BrochureParseResult(BaseModel):
    brochure_version_id: int
    parse_status: str
    parsed_at: datetime.datetime | None = None
    reducto_job_id: str | None = None
    parse_error: str | None = None
    page_count: int | None = None
    chunk_count: int | None = None


class BrochureParsedContent(BaseModel):
    brochure_version_id: int
    parse_status: str | None
    parsed_at: datetime.datetime | None
    reducto_job_id: str | None
    parse_error: str | None
    parsed_markdown: str | None
    parsed_chunks: list[Any] | None
