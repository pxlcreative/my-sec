from __future__ import annotations

import datetime

from sqlalchemy import Boolean, ForeignKey, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from models.base import Base


class QuestionnaireTemplate(Base):
    __tablename__ = "questionnaire_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text)
    style_type: Mapped[str] = mapped_column(Text, nullable=False, server_default="custom")
    created_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        server_default=func.now(), onupdate=func.now()
    )

    questions: Mapped[list[QuestionnaireQuestion]] = relationship(
        "QuestionnaireQuestion",
        back_populates="template",
        order_by="QuestionnaireQuestion.order_index",
        cascade="all, delete-orphan",
    )
    responses: Mapped[list[QuestionnaireResponse]] = relationship(
        "QuestionnaireResponse",
        back_populates="template",
        cascade="all, delete-orphan",
    )


class QuestionnaireQuestion(Base):
    __tablename__ = "questionnaire_questions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    template_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("questionnaire_templates.id", ondelete="CASCADE"), nullable=False
    )
    section: Mapped[str] = mapped_column(Text, nullable=False, server_default="General")
    order_index: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    question_text: Mapped[str] = mapped_column(Text, nullable=False)
    answer_field_path: Mapped[str | None] = mapped_column(Text)
    answer_hint: Mapped[str | None] = mapped_column(Text)
    notes_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())

    template: Mapped[QuestionnaireTemplate] = relationship(
        "QuestionnaireTemplate", back_populates="questions"
    )


class QuestionnaireResponse(Base):
    __tablename__ = "questionnaire_responses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    template_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("questionnaire_templates.id", ondelete="CASCADE"), nullable=False
    )
    crd_number: Mapped[int] = mapped_column(
        Integer, ForeignKey("firms.crd_number", ondelete="CASCADE"), nullable=False
    )
    generated_at: Mapped[datetime.datetime | None] = mapped_column(server_default=func.now())
    answers: Mapped[dict | None] = mapped_column(JSONB)        # {str(question_id): answer_str}
    analyst_notes: Mapped[dict | None] = mapped_column(JSONB)  # {str(question_id): note_str}
    ai_suggested: Mapped[dict | None] = mapped_column(JSONB)   # {str(question_id): suggestion_str}
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="draft")

    template: Mapped[QuestionnaireTemplate] = relationship(
        "QuestionnaireTemplate", back_populates="responses"
    )
