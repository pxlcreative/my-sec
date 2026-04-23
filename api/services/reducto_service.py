"""
Reducto API integration for parsing ADV Part 2 brochure PDFs.

Two-step flow:
  1. POST {base_url}/upload  (multipart file) → {"file_id": "reducto://..."}
  2. POST {base_url}/parse   (JSON {"input": "reducto://..."}) → sync result

Settings live in the singleton `reducto_settings` table; the masked sentinel
"***" is preserved on PATCH so secrets are never overwritten by the UI echo.
"""
from __future__ import annotations

import datetime
import logging
from typing import Any

import requests
from sqlalchemy.orm import Session

from models.brochure import AdvBrochure
from models.reducto_settings import ReductoSettings
from schemas.reducto_settings import (
    BrochureParseResult,
    ReductoSettingsPatch,
    ReductoTestResult,
)

log = logging.getLogger(__name__)

_MASKED_SENTINEL = "***"
_DEFAULT_BASE_URL = "https://platform.reducto.ai"
# Reducto sync parses for typical 1-5MB brochures finish in seconds, but
# allow headroom for occasional large or queued requests.
_PARSE_TIMEOUT = 180
_UPLOAD_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Settings CRUD
# ---------------------------------------------------------------------------

def get_settings(db: Session) -> ReductoSettings:
    row: ReductoSettings | None = db.get(ReductoSettings, 1)
    if row is None:
        row = ReductoSettings(id=1, base_url=_DEFAULT_BASE_URL, enabled=False)
        db.add(row)
        db.commit()
        db.refresh(row)
    return row


def update_settings(db: Session, patch: ReductoSettingsPatch) -> ReductoSettings:
    row = get_settings(db)
    for field, value in patch.model_dump(exclude_none=True).items():
        if field == "api_key" and value == _MASKED_SENTINEL:
            continue
        setattr(row, field, value)
    db.commit()
    db.refresh(row)
    return row


def test_connection(db: Session) -> ReductoTestResult:
    """
    Verify Reducto credentials by hitting the /upload endpoint with a tiny
    payload. The endpoint returns 200 with a file_id when auth is valid.
    """
    row = get_settings(db)
    if not row.api_key:
        return ReductoTestResult(success=False, message="No API key configured.")

    base_url = (row.base_url or _DEFAULT_BASE_URL).rstrip("/")
    try:
        # A minimal valid PDF is plenty to validate auth round-trip.
        files = {"file": ("ping.pdf", b"%PDF-1.4\n%%EOF\n", "application/pdf")}
        resp = requests.post(
            f"{base_url}/upload",
            headers={"Authorization": f"Bearer {row.api_key}"},
            files=files,
            timeout=30,
        )
    except requests.RequestException as exc:
        return ReductoTestResult(success=False, message=f"Network error: {exc}")

    if resp.status_code == 401:
        return ReductoTestResult(success=False, message="Invalid API key (401).")
    if resp.status_code == 403:
        return ReductoTestResult(success=False, message="API key forbidden (403).")
    if resp.status_code >= 400:
        snippet = resp.text[:200] if resp.text else ""
        return ReductoTestResult(
            success=False,
            message=f"Reducto returned HTTP {resp.status_code}: {snippet}",
        )

    return ReductoTestResult(success=True, message="Reducto API key verified.")


# ---------------------------------------------------------------------------
# Parse one brochure
# ---------------------------------------------------------------------------

def parse_brochure(crd: int, version_id: int, db: Session) -> BrochureParseResult:
    """
    Read the stored PDF for (crd, version_id), upload it to Reducto, parse it,
    and persist the results onto the AdvBrochure row.
    """
    settings_row = get_settings(db)
    if not settings_row.enabled or not settings_row.api_key:
        raise RuntimeError(
            "Reducto integration is not configured. Set the API key and enable it under Settings."
        )

    brochure = db.query(AdvBrochure).filter(
        AdvBrochure.crd_number == crd,
        AdvBrochure.brochure_version_id == version_id,
    ).first()
    if brochure is None:
        raise LookupError(f"Brochure not found for CRD {crd} version {version_id}")

    # Pull bytes from whatever storage backend holds this brochure
    from services.storage_backends import get_active_backend, key_from_uri

    backend = get_active_backend(db)
    _, key = key_from_uri(brochure.file_path)
    pdf_bytes = backend.get(key)

    base_url = (settings_row.base_url or _DEFAULT_BASE_URL).rstrip("/")
    headers = {"Authorization": f"Bearer {settings_row.api_key}"}

    try:
        file_ref = _upload(base_url, headers, pdf_bytes, version_id)
        result = _parse(base_url, headers, file_ref)
    except Exception as exc:
        log.warning(
            "parse_brochure: failed crd=%d version=%d: %s", crd, version_id, exc
        )
        brochure.parse_status = "failed"
        brochure.parse_error = str(exc)[:1000]
        brochure.parsed_at = datetime.datetime.now(datetime.timezone.utc)
        db.commit()
        return BrochureParseResult(
            brochure_version_id=version_id,
            parse_status="failed",
            parsed_at=brochure.parsed_at,
            parse_error=brochure.parse_error,
        )

    chunks = _extract_chunks(result)
    markdown = _flatten_markdown(chunks)

    brochure.parse_status = "success"
    brochure.parse_error = None
    brochure.parsed_at = datetime.datetime.now(datetime.timezone.utc)
    brochure.parsed_markdown = markdown
    brochure.parsed_chunks = chunks
    brochure.reducto_job_id = result.get("job_id")
    db.commit()

    return BrochureParseResult(
        brochure_version_id=version_id,
        parse_status="success",
        parsed_at=brochure.parsed_at,
        reducto_job_id=brochure.reducto_job_id,
        page_count=(result.get("usage") or {}).get("num_pages"),
        chunk_count=len(chunks),
    )


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _upload(base_url: str, headers: dict, pdf_bytes: bytes, version_id: int) -> str:
    files = {"file": (f"brochure_{version_id}.pdf", pdf_bytes, "application/pdf")}
    resp = requests.post(
        f"{base_url}/upload", headers=headers, files=files, timeout=_UPLOAD_TIMEOUT
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"upload failed HTTP {resp.status_code}: {resp.text[:300]}")
    body = resp.json()
    file_ref = body.get("file_id")
    if not file_ref:
        raise RuntimeError(f"upload response missing file_id: {body!r}")
    return file_ref


def _parse(base_url: str, headers: dict, file_ref: str) -> dict[str, Any]:
    resp = requests.post(
        f"{base_url}/parse",
        headers={**headers, "Content-Type": "application/json"},
        json={"input": file_ref},
        timeout=_PARSE_TIMEOUT,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"parse failed HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


# ---------------------------------------------------------------------------
# Result shaping
# ---------------------------------------------------------------------------

def _extract_chunks(result: dict[str, Any]) -> list[dict]:
    """
    Reducto returns either {result: {chunks: [...]}} (FullResult) or
    {result: {url: "..."}} (UrlResult, when the response is too large to inline).
    Only the FullResult shape carries inline content; UrlResult requires a
    follow-up GET which we don't support here yet.
    """
    inner = result.get("result") or {}
    chunks = inner.get("chunks")
    if isinstance(chunks, list):
        return chunks
    return []


def _flatten_markdown(chunks: list[dict]) -> str:
    parts: list[str] = []
    for c in chunks:
        content = c.get("content")
        if content:
            parts.append(content)
    return "\n\n".join(parts)
