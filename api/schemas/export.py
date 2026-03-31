import datetime
import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

ExportFormat = Literal["csv", "json", "xlsx"]
ExportStatus = Literal["pending", "running", "complete", "failed", "expired"]


class ExportFilterCriteria(BaseModel):
    platform_ids: list[int] | None = None
    registration_status: str | None = None
    aum_min: int | None = None
    aum_max: int | None = None
    states: list[str] | None = None


class ExportRequest(BaseModel):
    filter: ExportFilterCriteria = Field(default_factory=ExportFilterCriteria)
    crd_list: list[int] | None = Field(default=None, max_length=50_000)
    field_selection: list[str] | None = None   # extra columns beyond default set
    format: ExportFormat = "csv"


class ExportJobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    format: str
    status: str
    row_count: int | None
    file_path: str | None
    error_message: str | None
    created_at: datetime.datetime | None
    completed_at: datetime.datetime | None
    expires_at: datetime.datetime | None


class AsyncExportResponse(BaseModel):
    job_id: uuid.UUID
    status: str
    message: str


# ---------------------------------------------------------------------------
# Export templates
# ---------------------------------------------------------------------------

class ExportTemplateCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None
    format: ExportFormat = "csv"
    filter_criteria: dict | None = None
    field_selection: list[str] | None = None


class ExportTemplateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: str | None
    format: str
    filter_criteria: dict | None
    field_selection: dict | None
    created_at: datetime.datetime | None
    updated_at: datetime.datetime | None
