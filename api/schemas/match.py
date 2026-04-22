from typing import Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class MatchOptions(BaseModel):
    min_score: float = Field(default=50.0, ge=0, le=100)
    max_candidates: int = Field(default=3, ge=1, le=10)


class InputRecord(BaseModel):
    id: str | int | None = None  # caller-supplied identifier, echoed in results
    name: str
    address: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None


class BulkMatchRequest(BaseModel):
    records: list[InputRecord] = Field(default_factory=list)
    options: MatchOptions = Field(default_factory=MatchOptions)


# ---------------------------------------------------------------------------
# Response: one candidate
# ---------------------------------------------------------------------------

MatchStatus = Literal["confirmed", "probable", "possible", "no_match"]


class MatchCandidate(BaseModel):
    crd_number: int
    legal_name: str
    business_name: str | None
    main_city: str | None
    main_state: str | None
    main_zip: str | None
    registration_status: str | None
    score: float
    status: MatchStatus


# ---------------------------------------------------------------------------
# Response: one input row with its candidates
# ---------------------------------------------------------------------------

class MatchResult(BaseModel):
    id: str | int | None
    input_name: str
    input_city: str | None
    input_state: str | None
    input_zip: str | None
    best_score: float
    best_status: MatchStatus
    candidates: list[MatchCandidate]


# ---------------------------------------------------------------------------
# Stats block
# ---------------------------------------------------------------------------

class MatchStats(BaseModel):
    total: int
    confirmed: int
    probable: int
    possible: int
    no_match: int


# ---------------------------------------------------------------------------
# Sync (≤100) response
# ---------------------------------------------------------------------------

class BulkMatchSyncResponse(BaseModel):
    results: list[MatchResult]
    stats: MatchStats


# ---------------------------------------------------------------------------
# Async (>100) response
# ---------------------------------------------------------------------------

class BulkMatchAsyncResponse(BaseModel):
    job_id: int
    status: str
    message: str


# ---------------------------------------------------------------------------
# Job-status poll response
# ---------------------------------------------------------------------------

class MatchJobStatus(BaseModel):
    job_id: int
    status: str
    created_at: str | None
    started_at: str | None
    completed_at: str | None
    error_message: str | None
    results: BulkMatchSyncResponse | None = None
