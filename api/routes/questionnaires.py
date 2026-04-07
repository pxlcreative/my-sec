"""
Questionnaire routes:
  /api/questionnaires      — template CRUD + field registry
  /api/firms/{crd}/questionnaires  — firm-specific responses + Excel download
"""
from __future__ import annotations

import logging
import re
from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from db import get_db
from models.firm import Firm
from models.questionnaire import QuestionnaireQuestion, QuestionnaireResponse, QuestionnaireTemplate
from schemas.questionnaire import (
    FieldDefOut,
    FirmQuestionnaireListItem,
    QuestionnaireQuestionIn,
    QuestionnaireQuestionOut,
    QuestionnaireResponseOut,
    QuestionnaireTemplateDetailOut,
    QuestionnaireTemplateIn,
    QuestionnaireTemplateOut,
    ReorderIn,
    UpdateResponseIn,
)
from services.questionnaire_resolver import get_field_registry
from services.questionnaire_service import (
    add_question,
    build_questionnaire_xlsx,
    create_template,
    delete_question,
    delete_template,
    get_or_create_response,
    get_template,
    get_templates,
    has_responses,
    regenerate_response,
    reorder_questions,
    update_question,
    update_response,
    update_template,
)

log = logging.getLogger(__name__)

router = APIRouter(tags=["questionnaires"])
_MIME_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _safe_name(s: str, max_len: int = 30) -> str:
    clean = re.sub(r"[^\w\s-]", "", s).strip()
    return clean[:max_len].replace(" ", "_")


# ---------------------------------------------------------------------------
# Template CRUD
# ---------------------------------------------------------------------------

@router.get("/questionnaires", response_model=list[QuestionnaireTemplateOut])
def list_templates(db: Session = Depends(get_db)):
    templates = get_templates(db)
    result = []
    for t in templates:
        data = {
            "id": t.id,
            "name": t.name,
            "description": t.description,
            "style_type": t.style_type,
            "created_at": t.created_at,
            "updated_at": t.updated_at,
            "question_count": len(t.questions),
        }
        result.append(QuestionnaireTemplateOut(**data))
    return result


@router.post("/questionnaires", response_model=QuestionnaireTemplateOut, status_code=201)
def create_questionnaire_template(body: QuestionnaireTemplateIn, db: Session = Depends(get_db)):
    existing = db.scalar(
        select(QuestionnaireTemplate).where(QuestionnaireTemplate.name == body.name)
    )
    if existing:
        raise HTTPException(status_code=409, detail="A template with this name already exists")
    tmpl = create_template(body.model_dump(), db)
    return QuestionnaireTemplateOut(
        id=tmpl.id,
        name=tmpl.name,
        description=tmpl.description,
        style_type=tmpl.style_type,
        created_at=tmpl.created_at,
        updated_at=tmpl.updated_at,
        question_count=0,
    )


@router.get("/questionnaires/fields", response_model=dict[str, FieldDefOut])
def list_fields():
    registry = get_field_registry()
    return {
        path: FieldDefOut(
            label=defn.label,
            category=defn.category,
            field_type=defn.field_type,
            example=defn.example,
        )
        for path, defn in registry.items()
    }


@router.get("/questionnaires/{template_id}", response_model=QuestionnaireTemplateDetailOut)
def get_questionnaire_template(template_id: int, db: Session = Depends(get_db)):
    tmpl = get_template(template_id, db)
    if tmpl is None:
        raise HTTPException(status_code=404, detail="Template not found")
    return QuestionnaireTemplateDetailOut.model_validate(tmpl)


@router.put("/questionnaires/{template_id}", response_model=QuestionnaireTemplateOut)
def update_questionnaire_template(
    template_id: int, body: QuestionnaireTemplateIn, db: Session = Depends(get_db)
):
    # Check name uniqueness (excluding self)
    existing = db.scalar(
        select(QuestionnaireTemplate)
        .where(QuestionnaireTemplate.name == body.name, QuestionnaireTemplate.id != template_id)
    )
    if existing:
        raise HTTPException(status_code=409, detail="A template with this name already exists")
    tmpl = update_template(template_id, body.model_dump(exclude_none=True), db)
    if tmpl is None:
        raise HTTPException(status_code=404, detail="Template not found")
    # Reload with questions to get accurate count
    tmpl_with_qs = get_template(template_id, db)
    return QuestionnaireTemplateOut(
        id=tmpl.id,
        name=tmpl.name,
        description=tmpl.description,
        style_type=tmpl.style_type,
        created_at=tmpl.created_at,
        updated_at=tmpl.updated_at,
        question_count=len(tmpl_with_qs.questions) if tmpl_with_qs else 0,
    )


@router.delete("/questionnaires/{template_id}", status_code=204)
def delete_questionnaire_template(template_id: int, db: Session = Depends(get_db)):
    if has_responses(template_id, db):
        raise HTTPException(
            status_code=409,
            detail="Cannot delete template with existing firm responses. Delete responses first or use force=true.",
        )
    if not delete_template(template_id, db):
        raise HTTPException(status_code=404, detail="Template not found")


# ---------------------------------------------------------------------------
# Question CRUD
# ---------------------------------------------------------------------------

@router.post(
    "/questionnaires/{template_id}/questions",
    response_model=QuestionnaireQuestionOut,
    status_code=201,
)
def create_question(
    template_id: int, body: QuestionnaireQuestionIn, db: Session = Depends(get_db)
):
    q = add_question(template_id, body.model_dump(), db)
    if q is None:
        raise HTTPException(status_code=404, detail="Template not found")
    return QuestionnaireQuestionOut.model_validate(q)


@router.put(
    "/questionnaires/{template_id}/questions/reorder",
    status_code=204,
)
def reorder_template_questions(
    template_id: int, body: ReorderIn, db: Session = Depends(get_db)
):
    tmpl = db.get(QuestionnaireTemplate, template_id)
    if tmpl is None:
        raise HTTPException(status_code=404, detail="Template not found")
    reorder_questions(template_id, body.ordered_ids, db)


@router.put(
    "/questionnaires/{template_id}/questions/{question_id}",
    response_model=QuestionnaireQuestionOut,
)
def update_template_question(
    template_id: int,
    question_id: int,
    body: QuestionnaireQuestionIn,
    db: Session = Depends(get_db),
):
    q = db.get(QuestionnaireQuestion, question_id)
    if q is None or q.template_id != template_id:
        raise HTTPException(status_code=404, detail="Question not found")
    updated = update_question(question_id, body.model_dump(exclude_none=True), db)
    return QuestionnaireQuestionOut.model_validate(updated)


@router.delete("/questionnaires/{template_id}/questions/{question_id}", status_code=204)
def delete_template_question(
    template_id: int, question_id: int, db: Session = Depends(get_db)
):
    q = db.get(QuestionnaireQuestion, question_id)
    if q is None or q.template_id != template_id:
        raise HTTPException(status_code=404, detail="Question not found")
    delete_question(question_id, db)


# ---------------------------------------------------------------------------
# Firm-specific questionnaire endpoints
# ---------------------------------------------------------------------------

@router.get("/firms/{crd}/questionnaires", response_model=list[FirmQuestionnaireListItem])
def list_firm_questionnaires(crd: int, db: Session = Depends(get_db)):
    firm = db.get(Firm, crd)
    if firm is None:
        raise HTTPException(status_code=404, detail="Firm not found")

    templates = get_templates(db)
    responses_by_template = {
        r.template_id: r
        for r in db.scalars(
            select(QuestionnaireResponse).where(QuestionnaireResponse.crd_number == crd)
        ).all()
    }

    result = []
    for t in templates:
        resp = responses_by_template.get(t.id)
        result.append(FirmQuestionnaireListItem(
            template_id=t.id,
            template_name=t.name,
            description=t.description,
            style_type=t.style_type,
            question_count=len(t.questions),
            has_response=resp is not None,
            response_generated_at=resp.generated_at if resp else None,
            response_status=resp.status if resp else None,
        ))
    return result


@router.get(
    "/firms/{crd}/questionnaires/{template_id}",
    response_model=QuestionnaireResponseOut,
)
def get_firm_questionnaire(
    crd: int, template_id: int, db: Session = Depends(get_db)
):
    firm = db.get(Firm, crd)
    if firm is None:
        raise HTTPException(status_code=404, detail="Firm not found")
    response = get_or_create_response(template_id, crd, db)
    if response is None:
        raise HTTPException(status_code=404, detail="Template not found")
    # Eager-load template+questions for the response schema
    template = get_template(template_id, db)
    response.template = template
    return QuestionnaireResponseOut.model_validate(response)


@router.post(
    "/firms/{crd}/questionnaires/{template_id}/regenerate",
    response_model=QuestionnaireResponseOut,
)
def regenerate_firm_questionnaire(
    crd: int, template_id: int, db: Session = Depends(get_db)
):
    firm = db.get(Firm, crd)
    if firm is None:
        raise HTTPException(status_code=404, detail="Firm not found")
    response = regenerate_response(template_id, crd, db)
    if response is None:
        raise HTTPException(status_code=404, detail="Template not found")
    template = get_template(template_id, db)
    response.template = template
    return QuestionnaireResponseOut.model_validate(response)


@router.patch(
    "/firms/{crd}/questionnaires/{template_id}/answers",
    response_model=QuestionnaireResponseOut,
)
def update_firm_questionnaire_answers(
    crd: int,
    template_id: int,
    body: UpdateResponseIn,
    db: Session = Depends(get_db),
):
    firm = db.get(Firm, crd)
    if firm is None:
        raise HTTPException(status_code=404, detail="Firm not found")
    response = update_response(
        template_id, crd,
        answers=body.answers or {},
        analyst_notes=body.analyst_notes or {},
        db=db,
    )
    if response is None:
        raise HTTPException(
            status_code=404,
            detail="Response not found — load the questionnaire first via GET",
        )
    if body.status:
        response.status = body.status
        db.commit()
        db.refresh(response)
    template = get_template(template_id, db)
    response.template = template
    return QuestionnaireResponseOut.model_validate(response)


@router.get("/firms/{crd}/questionnaires/{template_id}/excel")
def download_firm_questionnaire_excel(
    crd: int, template_id: int, db: Session = Depends(get_db)
):
    firm = db.get(Firm, crd)
    if firm is None:
        raise HTTPException(status_code=404, detail="Firm not found")

    response = get_or_create_response(template_id, crd, db)
    if response is None:
        raise HTTPException(status_code=404, detail="Template not found")

    template = get_template(template_id, db)

    try:
        xlsx_bytes = build_questionnaire_xlsx(response, template, template.questions, firm)
    except Exception:
        log.exception("Failed to build questionnaire xlsx for crd=%d template=%d", crd, template_id)
        raise HTTPException(status_code=500, detail="Failed to generate workbook")

    today_str = date.today().strftime("%Y%m%d")
    name_part = _safe_name(firm.legal_name or "")
    tmpl_part = _safe_name(template.name)
    filename  = f"{tmpl_part}_{crd}_{name_part}_{today_str}.xlsx"

    return Response(
        content=xlsx_bytes,
        media_type=_MIME_XLSX,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post(
    "/firms/{crd}/questionnaires/{template_id}/ai-suggest",
    status_code=501,
)
def ai_suggest_answers(crd: int, template_id: int):
    """AI suggestions are not yet implemented."""
    raise HTTPException(
        status_code=501,
        detail="AI suggestions are not yet enabled. This feature will be added in a future release.",
    )
