"""
API key authentication service.

Keys are stored as SHA-256 hashes — the raw key is shown once at creation and
never persisted in plaintext.

Rate limiting uses a Redis sliding-window counter:
  key: rate:{key_hash}:{minute_bucket}
  TTL: 90 seconds (covers current + previous minute window overlap)
  limit: 100 requests per minute per API key
"""
import hashlib
import logging
import os
import time
from datetime import datetime, timezone

from fastapi import Header, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

_RATE_LIMIT_RPS   = 100           # requests per minute
_RATE_WINDOW_SECS = 60
_RATE_KEY_TTL     = 90            # slightly longer than window


# ---------------------------------------------------------------------------
# Key generation & hashing
# ---------------------------------------------------------------------------

def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def generate_api_key() -> tuple[str, str]:
    """
    Generate a new API key.
    Returns (raw_key, key_hash).
    raw_key should be shown to the user once and never stored.
    """
    raw_key = os.urandom(32).hex()          # 64-char hex string
    return raw_key, _hash_key(raw_key)


# ---------------------------------------------------------------------------
# Key verification
# ---------------------------------------------------------------------------

def verify_api_key(raw_key: str, db: Session):
    """
    Hash raw_key, look up in api_keys, check active.
    Updates last_used_at on success.
    Returns ApiKey ORM object or None.
    """
    from models.api_key import ApiKey

    key_hash = _hash_key(raw_key)
    api_key = db.scalars(
        select(ApiKey)
        .where(ApiKey.key_hash == key_hash, ApiKey.active.is_(True))
    ).first()

    if api_key is None:
        return None

    api_key.last_used_at = datetime.now(timezone.utc)
    db.commit()
    return api_key


# ---------------------------------------------------------------------------
# Rate limiting (Redis sliding window)
# ---------------------------------------------------------------------------

def _get_redis():
    """Return a redis.Redis client. Lazy import so the module loads without Redis."""
    import redis as redis_lib
    from config import settings
    return redis_lib.from_url(settings.redis_url, decode_responses=True)


def check_rate_limit(key_hash: str) -> None:
    """
    Increment the sliding-window counter for key_hash.
    Raises HTTP 429 if the rate limit is exceeded.
    Fails open (logs warning) if Redis is unavailable.
    """
    try:
        r = _get_redis()
        minute_bucket = int(time.time() // _RATE_WINDOW_SECS)
        redis_key = f"rate:{key_hash}:{minute_bucket}"

        count = r.incr(redis_key)
        if count == 1:
            r.expire(redis_key, _RATE_KEY_TTL)

        if count > _RATE_LIMIT_RPS:
            log.warning("Rate limit exceeded for key_hash=%.12s… count=%d", key_hash, count)
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded: max 100 requests/minute.",
                headers={"Retry-After": str(_RATE_WINDOW_SECS)},
            )
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("check_rate_limit: Redis unavailable (%s) — allowing request", exc)


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)


def get_current_api_key(
    credentials: HTTPAuthorizationCredentials | None = Header(default=None, alias="Authorization"),
):
    """
    FastAPI dependency.  Expects: Authorization: Bearer <raw_key>
    Returns the ApiKey ORM object.
    Raises 401 if missing/invalid, 429 if rate limited.

    NOTE: DB session is opened inside the function so the dependency can be
    used without Depends(get_db) coupling.
    """
    # Parse Bearer token
    raw_key: str | None = None
    if isinstance(credentials, str):
        # Called with Header(alias="Authorization") — parse manually
        val = credentials.strip()
        if val.lower().startswith("bearer "):
            raw_key = val[7:].strip()
    elif credentials and hasattr(credentials, "credentials"):
        raw_key = credentials.credentials

    if not raw_key:
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization: Bearer <key> header.",
        )

    from db import SessionLocal
    db = SessionLocal()
    try:
        api_key = verify_api_key(raw_key, db)
    finally:
        db.close()

    if api_key is None:
        raise HTTPException(status_code=401, detail="Invalid or inactive API key.")

    check_rate_limit(api_key.key_hash)
    return api_key


def make_api_key_dep():
    """
    Return a FastAPI Depends-compatible dependency that extracts the Bearer
    token from the Authorization header, validates it, and enforces rate limiting.
    """
    from fastapi import Depends, Request

    async def _dep(request: Request):
        auth_header = request.headers.get("Authorization", "")
        raw_key: str | None = None
        if auth_header.lower().startswith("bearer "):
            raw_key = auth_header[7:].strip()

        if not raw_key:
            raise HTTPException(
                status_code=401,
                detail="Missing or malformed Authorization: Bearer <key> header.",
            )

        from db import SessionLocal
        db = SessionLocal()
        try:
            api_key = verify_api_key(raw_key, db)
        finally:
            db.close()

        if api_key is None:
            raise HTTPException(status_code=401, detail="Invalid or inactive API key.")

        check_rate_limit(api_key.key_hash)
        return api_key

    return _dep
