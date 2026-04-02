import datetime

from pydantic import BaseModel, ConfigDict, Field


class PlatformOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: str | None
    save_brochures: bool
    created_at: datetime.datetime | None


class PlatformCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None
    save_brochures: bool = False


class SetFirmPlatformsRequest(BaseModel):
    platform_ids: list[int] = Field(..., min_length=0)
    tagged_by: str | None = None
    notes: str | None = None


class BulkTagRecord(BaseModel):
    crd_number: int
    platform_id: int


class BulkTagRequest(BaseModel):
    records: list[BulkTagRecord] = Field(..., min_length=1, max_length=50_000)
    tagged_by: str | None = None
    notes: str | None = None


class BulkTagResponse(BaseModel):
    inserted: int
    skipped: int


class FirmPlatformTag(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    platform_id: int
    platform_name: str
    tagged_at: datetime.datetime | None
    tagged_by: str | None
    notes: str | None
