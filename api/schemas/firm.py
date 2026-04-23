import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class PlatformTag(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str


class FirmSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    crd_number: int
    legal_name: str
    business_name: str | None
    main_city: str | None
    main_state: str | None
    aum_total: int | None
    registration_status: str | None
    last_filing_date: datetime.date | None
    platforms: list[str] = []


class FirmDetail(FirmSummary):
    sec_number: str | None
    firm_type: str | None
    aum_discretionary: int | None
    aum_non_discretionary: int | None
    num_accounts: int | None
    num_employees: int | None
    main_street1: str | None
    main_street2: str | None
    main_zip: str | None
    main_country: str | None
    phone: str | None
    website: str | None
    org_type: str | None
    fiscal_year_end: str | None
    aum_2023: int | None
    aum_2024: int | None
    created_at: datetime.datetime | None
    updated_at: datetime.datetime | None
    last_iapd_refresh_at: datetime.datetime | None = None
    # Only included when ?include_raw_adv=true
    raw_adv: Any | None = None
    # Latest brochure metadata (None if no brochures stored)
    latest_brochure: "BrochureMeta | None" = None


class BrochureMeta(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    brochure_version_id: int
    brochure_name: str | None
    date_submitted: datetime.date | None
    source_month: str | None
    file_size_bytes: int | None
    parse_status: str | None = None
    parsed_at: datetime.datetime | None = None


class AumHistoryPoint(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    filing_date: datetime.date
    aum_total: int | None
    aum_discretionary: int | None
    aum_non_discretionary: int | None
    num_accounts: int | None
    source: str


class AumAnnualSummary(BaseModel):
    year: int
    peak_aum: int | None
    trough_aum: int | None
    latest_aum_for_year: int | None
    filing_count: int


class AumHistoryResponse(BaseModel):
    crd_number: int
    annual: list[AumAnnualSummary]
    filings: list[AumHistoryPoint]


class ChangeRecord(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    field_path: str
    old_value: str | None
    new_value: str | None
    detected_at: datetime.datetime
    snapshot_from: int | None
    snapshot_to: int | None


class FirmHistoryResponse(BaseModel):
    crd_number: int
    changes: list[ChangeRecord]


class PaginatedFirms(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[FirmSummary]


class DisclosuresSummaryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    crd_number: int
    criminal_count: int
    regulatory_count: int
    civil_count: int
    customer_count: int
    total_count: int
    updated_at: datetime.datetime | None = None


class BusinessProfileOut(BaseModel):
    available: bool = True
    client_types: list[str]
    compensation_types: list[str]
    investment_strategies: list[str]
    affiliations: list[dict]


class SyncStatusEntry(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    job_type: str
    status: str
    source_url: str | None
    firms_processed: int
    firms_updated: int
    changes_detected: int
    error_message: str | None
    results: dict | None
    started_at: datetime.datetime | None
    completed_at: datetime.datetime | None
    created_at: datetime.datetime | None
