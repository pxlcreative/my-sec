from __future__ import annotations

import datetime

from pydantic import BaseModel, ConfigDict


class QuestionnaireTemplateIn(BaseModel):
    name: str
    description: str | None = None
    style_type: str = "custom"


class QuestionnaireTemplateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: str | None
    style_type: str
    created_at: datetime.datetime | None
    updated_at: datetime.datetime | None
    question_count: int = 0  # populated by route, not ORM


class QuestionnaireQuestionIn(BaseModel):
    section: str = "General"
    order_index: int | None = None
    question_text: str
    answer_field_path: str | None = None
    answer_hint: str | None = None
    notes_enabled: bool = True


class QuestionnaireQuestionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    template_id: int
    section: str
    order_index: int
    question_text: str
    answer_field_path: str | None
    answer_hint: str | None
    notes_enabled: bool
    created_at: datetime.datetime | None


class QuestionnaireTemplateDetailOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: str | None
    style_type: str
    created_at: datetime.datetime | None
    updated_at: datetime.datetime | None
    questions: list[QuestionnaireQuestionOut]


class QuestionnaireResponseOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    template_id: int
    crd_number: int
    generated_at: datetime.datetime | None
    answers: dict | None
    analyst_notes: dict | None
    ai_suggested: dict | None
    status: str
    template: QuestionnaireTemplateDetailOut | None = None


class UpdateResponseIn(BaseModel):
    answers: dict[str, str] | None = None
    analyst_notes: dict[str, str] | None = None
    status: str | None = None


class ReorderIn(BaseModel):
    ordered_ids: list[int]


class FieldDefOut(BaseModel):
    label: str
    category: str
    field_type: str
    example: str


class FirmQuestionnaireListItem(BaseModel):
    """Summary of a template + whether this firm has a response."""
    template_id: int
    template_name: str
    description: str | None
    style_type: str
    question_count: int
    has_response: bool
    response_generated_at: datetime.datetime | None
    response_status: str | None
