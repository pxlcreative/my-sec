#!/usr/bin/env python3
"""
Seed default questionnaire templates into the database.
Safe to run multiple times — skips templates that already exist.

Usage:
    docker compose exec api python scripts/seed_questionnaires.py

Templates seeded:
  1. "Initial RFI / Due Diligence" — 40 questions matching the legacy DDQ
  2. "Annual Certification" — 15-question annual review checklist
"""
from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "api"))
load_dotenv(PROJECT_ROOT / ".env")


# ---------------------------------------------------------------------------
# Template definitions
# ---------------------------------------------------------------------------

TEMPLATES = [
    {
        "name": "Initial RFI / Due Diligence",
        "description": "Comprehensive 40-question due diligence questionnaire populated from ADV data.",
        "style_type": "initial_rfi",
        "questions": [
            # Core identification
            {"section": "Core Identification", "question_text": "What is the firm's total AUM?",             "answer_field_path": "firm.aum_total"},
            {"section": "Core Identification", "question_text": "How many client accounts does the firm serve?", "answer_field_path": "firm.num_accounts"},
            {"section": "Core Identification", "question_text": "What is the firm's primary business name?",  "answer_field_path": "firm.business_name"},
            {"section": "Core Identification", "question_text": "Is the firm currently registered with the SEC?", "answer_field_path": "firm.registration_status"},
            {"section": "Core Identification", "question_text": "Does the firm have disciplinary disclosures?", "answer_field_path": "disclosures.has_disclosures"},
            {"section": "Core Identification", "question_text": "What types of clients does the firm serve?",  "answer_hint": "See ADV Item 5D"},
            {"section": "Core Identification", "question_text": "What compensation arrangements does the firm use?", "answer_hint": "See ADV Item 5E"},
            {"section": "Core Identification", "question_text": "Does the firm have custody of client assets?", "answer_hint": "See ADV Item 9"},
            {"section": "Core Identification", "question_text": "What is the year of the most recent ADV filing?", "answer_field_path": "firm.last_filing_date"},
            {"section": "Core Identification", "question_text": "Does the firm have private fund clients?",    "answer_hint": "See ADV Item 7B"},
            # AUM Detail
            {"section": "AUM Detail", "question_text": "What is the discretionary AUM?",                     "answer_field_path": "firm.aum_discretionary"},
            {"section": "AUM Detail", "question_text": "What is the non-discretionary AUM?",                  "answer_field_path": "firm.aum_non_discretionary"},
            {"section": "AUM Detail", "question_text": "What was the firm's AUM for 2023?",                   "answer_field_path": "firm.aum_2023"},
            {"section": "AUM Detail", "question_text": "What was the firm's AUM for 2024?",                   "answer_field_path": "firm.aum_2024"},
            {"section": "AUM Detail", "question_text": "Has AUM grown or declined over the past two years?",  "answer_hint": "See AUM History sheet"},
            # Employees & Organisation
            {"section": "Employees & Organisation", "question_text": "How many employees does the firm have?", "answer_field_path": "firm.num_employees"},
            {"section": "Employees & Organisation", "question_text": "What is the firm's legal organisational type?", "answer_field_path": "firm.org_type"},
            {"section": "Employees & Organisation", "question_text": "What is the firm's fiscal year end?",    "answer_field_path": "firm.fiscal_year_end"},
            {"section": "Employees & Organisation", "question_text": "What is the firm's CRD number?",        "answer_field_path": "firm.crd_number"},
            {"section": "Employees & Organisation", "question_text": "What is the firm's SEC file number?",   "answer_field_path": "firm.sec_number"},
            # Registration & Regulatory
            {"section": "Registration & Regulatory", "question_text": "What states is the firm registered in?", "answer_hint": "See ADV Part 1 Item 2"},
            {"section": "Registration & Regulatory", "question_text": "Is the firm registered as an investment company?", "answer_hint": "See ADV Item 2A"},
            {"section": "Registration & Regulatory", "question_text": "Does the firm rely on any exemptions from registration?", "answer_hint": "See ADV Item 2B"},
            {"section": "Registration & Regulatory", "question_text": "Has the firm ever been subject to a regulatory action?", "answer_field_path": "disclosures.regulatory_count"},
            {"section": "Registration & Regulatory", "question_text": "Has the firm been subject to any criminal proceedings?", "answer_field_path": "disclosures.criminal_count"},
            {"section": "Registration & Regulatory", "question_text": "Has the firm been subject to any civil proceedings?",   "answer_field_path": "disclosures.civil_count"},
            {"section": "Registration & Regulatory", "question_text": "Have there been customer complaints or arbitrations?", "answer_field_path": "disclosures.customer_count"},
            # Investment Management
            {"section": "Investment Management", "question_text": "What investment strategies does the firm employ?", "answer_hint": "See ADV Item 8"},
            {"section": "Investment Management", "question_text": "Does the firm manage wrap fee programs?",   "answer_hint": "See ADV Item 4"},
            {"section": "Investment Management", "question_text": "Does the firm sponsor or advise private funds?", "answer_hint": "See ADV Item 7B"},
            {"section": "Investment Management", "question_text": "Does the firm use sub-advisers?",           "answer_hint": "See ADV Item 8F"},
            {"section": "Investment Management", "question_text": "Does the firm trade on margin or use leverage?", "answer_hint": "See ADV Item 8"},
            {"section": "Investment Management", "question_text": "Does the firm employ derivatives strategies?", "answer_hint": "See ADV Item 8"},
            # Client Relationships
            {"section": "Client Relationships", "question_text": "What is the minimum account size?",          "answer_hint": "See ADV Item 5F"},
            {"section": "Client Relationships", "question_text": "Does the firm provide financial planning services?", "answer_hint": "See ADV Item 5G"},
            {"section": "Client Relationships", "question_text": "Does the firm participate in wrap fee programs as sponsor?", "answer_hint": "See ADV Item 4"},
            # Operations
            {"section": "Operations", "question_text": "What is the firm's primary business address?", "answer_field_path": "firm.address_full"},
            {"section": "Operations", "question_text": "What is the firm's phone number?",             "answer_field_path": "firm.phone"},
            {"section": "Operations", "question_text": "What is the firm's website?",                  "answer_field_path": "firm.website"},
            {"section": "Operations", "question_text": "What is the date of the most recent ADV filing?", "answer_field_path": "firm.last_filing_date"},
        ],
    },
    {
        "name": "Annual Certification",
        "description": "Annual certification checklist covering key compliance and regulatory data points.",
        "style_type": "annual_cert",
        "questions": [
            {"section": "Registration & Status", "question_text": "Confirm the firm's current SEC registration status.", "answer_field_path": "firm.registration_status"},
            {"section": "Registration & Status", "question_text": "Confirm the firm's CRD number.", "answer_field_path": "firm.crd_number"},
            {"section": "Registration & Status", "question_text": "What is the date of the most recent ADV filing?", "answer_field_path": "firm.last_filing_date"},
            {"section": "Registration & Status", "question_text": "Confirm the firm's fiscal year end.", "answer_field_path": "firm.fiscal_year_end"},
            # AUM
            {"section": "Assets Under Management", "question_text": "Confirm the firm's total AUM as of the most recent filing.", "answer_field_path": "firm.aum_total"},
            {"section": "Assets Under Management", "question_text": "Confirm the firm's discretionary AUM.", "answer_field_path": "firm.aum_discretionary"},
            {"section": "Assets Under Management", "question_text": "Confirm the number of client accounts.", "answer_field_path": "firm.num_accounts"},
            {"section": "Assets Under Management", "question_text": "Has AUM changed by more than 20% since the previous certification?", "answer_hint": "Compare to prior year AUM"},
            # Disclosures
            {"section": "Disclosures", "question_text": "Are there any new regulatory disclosures since the last certification?", "answer_field_path": "disclosures.regulatory_count"},
            {"section": "Disclosures", "question_text": "Are there any new criminal disclosures since the last certification?",   "answer_field_path": "disclosures.criminal_count"},
            {"section": "Disclosures", "question_text": "Are there any new civil or customer complaints since the last certification?", "answer_field_path": "disclosures.customer_count"},
            # Operations
            {"section": "Operations & Contact", "question_text": "Confirm the firm's primary business address.", "answer_field_path": "firm.address_full"},
            {"section": "Operations & Contact", "question_text": "Confirm the firm's phone number.", "answer_field_path": "firm.phone"},
            {"section": "Operations & Contact", "question_text": "Confirm the firm's website.", "answer_field_path": "firm.website"},
            # Sign-off
            {"section": "Sign-off", "question_text": "I certify that all information provided above is accurate as of the date of this certification.", "answer_hint": "Analyst signature / name"},
        ],
    },
]


def main() -> None:
    from sqlalchemy import select

    from db import SessionLocal
    from models.questionnaire import QuestionnaireQuestion, QuestionnaireTemplate

    db = SessionLocal()
    try:
        inserted_templates = 0
        skipped_templates = 0

        for tmpl_data in TEMPLATES:
            existing = db.scalars(
                select(QuestionnaireTemplate).where(
                    QuestionnaireTemplate.name == tmpl_data["name"]
                )
            ).first()

            if existing:
                print(f"  skip  '{tmpl_data['name']}' (already exists, id={existing.id})")
                skipped_templates += 1
                continue

            tmpl = QuestionnaireTemplate(
                name=tmpl_data["name"],
                description=tmpl_data["description"],
                style_type=tmpl_data["style_type"],
            )
            db.add(tmpl)
            db.flush()  # get tmpl.id

            for idx, q_data in enumerate(tmpl_data["questions"]):
                q = QuestionnaireQuestion(
                    template_id=tmpl.id,
                    section=q_data.get("section", "General"),
                    order_index=idx,
                    question_text=q_data["question_text"],
                    answer_field_path=q_data.get("answer_field_path"),
                    answer_hint=q_data.get("answer_hint"),
                    notes_enabled=True,
                )
                db.add(q)

            db.commit()
            print(f"  insert '{tmpl_data['name']}' ({len(tmpl_data['questions'])} questions)")
            inserted_templates += 1

        print(f"\nDone. Inserted {inserted_templates}, skipped {skipped_templates}.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
