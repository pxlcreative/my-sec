"""
Tests for services.auth_service.

Covers:
- generate_api_key: distinct raw, correct hash shape
- _hash_key: stable, distinct inputs produce distinct outputs
- verify_api_key: success, failure, inactive key rejected, last_used_at updated
- check_rate_limit: below-limit passes, over-limit raises 429, Redis down fails open
"""
from __future__ import annotations

import datetime
import hashlib
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from services.auth_service import (
    _hash_key,
    check_rate_limit,
    generate_api_key,
    verify_api_key,
)


# ── generate_api_key / _hash_key ────────────────────────────────────────────

class TestGenerateApiKey:
    def test_returns_raw_and_hash(self):
        raw, h = generate_api_key()
        assert isinstance(raw, str) and len(raw) == 64
        assert isinstance(h, str) and len(h) == 64

    def test_hash_matches_raw(self):
        raw, h = generate_api_key()
        assert hashlib.sha256(raw.encode()).hexdigest() == h

    def test_two_calls_yield_different_keys(self):
        a, _ = generate_api_key()
        b, _ = generate_api_key()
        assert a != b


class TestHashKey:
    def test_same_input_same_hash(self):
        assert _hash_key("xyz") == _hash_key("xyz")

    def test_different_input_different_hash(self):
        assert _hash_key("a") != _hash_key("b")


# ── verify_api_key ──────────────────────────────────────────────────────────

class TestVerifyApiKey:
    def test_valid_key_returns_api_key(self, db):
        from models.api_key import ApiKey

        raw, h = generate_api_key()
        db.add(ApiKey(key_hash=h, label="t", active=True))
        db.flush()

        result = verify_api_key(raw, db)
        assert result is not None
        assert result.key_hash == h

    def test_unknown_key_returns_none(self, db):
        assert verify_api_key("not-a-real-key", db) is None

    def test_inactive_key_returns_none(self, db):
        from models.api_key import ApiKey

        raw, h = generate_api_key()
        db.add(ApiKey(key_hash=h, label="t", active=False))
        db.flush()
        assert verify_api_key(raw, db) is None

    def test_updates_last_used_at(self, db):
        from models.api_key import ApiKey

        raw, h = generate_api_key()
        db.add(ApiKey(key_hash=h, label="t", active=True, last_used_at=None))
        db.flush()

        before = datetime.datetime.now(datetime.timezone.utc)
        verify_api_key(raw, db)

        key = db.scalars(
            __import__("sqlalchemy").select(ApiKey).where(ApiKey.key_hash == h)
        ).first()
        assert key.last_used_at is not None
        # last_used_at is TIMESTAMPTZ; allow for TZ-naive vs TZ-aware comparisons.
        last_used = key.last_used_at
        if last_used.tzinfo is None:
            last_used = last_used.replace(tzinfo=datetime.timezone.utc)
        assert last_used >= before


# ── check_rate_limit ────────────────────────────────────────────────────────

class TestCheckRateLimit:
    def test_under_limit_passes(self):
        fake_redis = _FakeRedis({"rate:h:0": 50})
        with patch("services.auth_service._get_redis", return_value=fake_redis), \
             patch("services.auth_service.time.time", return_value=0.0):
            check_rate_limit("h")  # no exception

    def test_over_limit_raises_429(self):
        # Initial value = 100. incr bumps to 101 → over limit.
        fake_redis = _FakeRedis({"rate:h:0": 100})
        with patch("services.auth_service._get_redis", return_value=fake_redis), \
             patch("services.auth_service.time.time", return_value=0.0):
            with pytest.raises(HTTPException) as exc:
                check_rate_limit("h")
            assert exc.value.status_code == 429
            assert "Retry-After" in exc.value.headers

    def test_redis_down_fails_open(self):
        def _raise(*a, **kw):
            raise ConnectionError("redis unreachable")
        with patch("services.auth_service._get_redis", side_effect=_raise):
            # Must NOT raise — rate limiter is non-blocking on Redis outage.
            check_rate_limit("h")

    def test_first_request_sets_ttl(self):
        fake_redis = _FakeRedis({})
        with patch("services.auth_service._get_redis", return_value=fake_redis), \
             patch("services.auth_service.time.time", return_value=0.0):
            check_rate_limit("h")

        assert fake_redis.store.get("rate:h:0") == 1
        assert fake_redis.ttl_set.get("rate:h:0") == 90  # _RATE_KEY_TTL


class _FakeRedis:
    """Minimal Redis stand-in — supports incr, expire."""
    def __init__(self, initial: dict):
        self.store = dict(initial)
        self.ttl_set: dict[str, int] = {}

    def incr(self, key: str) -> int:
        self.store[key] = self.store.get(key, 0) + 1
        return self.store[key]

    def expire(self, key: str, seconds: int) -> None:
        self.ttl_set[key] = seconds
