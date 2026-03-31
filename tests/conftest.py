"""
pytest fixtures for the SEC Adviser test suite.

Uses a separate test database (sec_adviser_test) that is created fresh for
every test session and torn down afterwards. No real Elasticsearch or Redis
required — those dependencies are mocked where needed.
"""
from __future__ import annotations

import datetime
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ── point at the test DB before importing anything that reads settings ──────
os.environ.setdefault(
    "DATABASE_URL",
    os.environ.get("TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/sec_adviser_test"),
)
os.environ.setdefault("ELASTICSEARCH_URL", "http://localhost:9200")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("SECRET_KEY", "test-secret-key-not-for-production")

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from db import get_db
from main import app
from models.base import Base
from models import firm as _firm_models  # noqa: F401 – registers all models
from models import aum  # noqa: F401
from models import brochure  # noqa: F401
from models import platform  # noqa: F401
from models import alert  # noqa: F401
from models import sync_job  # noqa: F401
from models import export_job  # noqa: F401
from models import api_key  # noqa: F401
from models.firm import Firm
from models.platform import PlatformDefinition


# ── Engine / session factory ─────────────────────────────────────────────────

TEST_DB_URL = os.environ["DATABASE_URL"]

_engine = create_engine(TEST_DB_URL, pool_pre_ping=True)
_TestingSession = sessionmaker(autocommit=False, autoflush=False, bind=_engine)


@pytest.fixture(scope="session", autouse=True)
def create_tables():
    """Create all tables once per test session, drop them afterwards."""
    Base.metadata.create_all(_engine)
    yield
    Base.metadata.drop_all(_engine)


@pytest.fixture()
def db():
    """Yield a DB session that is rolled back after each test."""
    connection = _engine.connect()
    transaction = connection.begin()
    session = _TestingSession(bind=connection)
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


@pytest.fixture()
def client(db):
    """FastAPI TestClient with the test DB session injected."""
    def _override_get_db():
        try:
            yield db
        finally:
            pass

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c
    app.dependency_overrides.clear()


# ── Seeded firm data ─────────────────────────────────────────────────────────

SEED_FIRMS = [
    dict(crd_number=100001, legal_name="Acme Capital Management LLC",     business_name="Acme Capital",     main_city="New York",    main_state="NY", aum_total=500_000_000,   registration_status="Registered"),
    dict(crd_number=100002, legal_name="Blue Ridge Advisors Inc",          business_name="Blue Ridge",        main_city="Atlanta",     main_state="GA", aum_total=250_000_000,   registration_status="Registered"),
    dict(crd_number=100003, legal_name="Coastal Wealth Partners LLC",      business_name="Coastal Wealth",    main_city="Miami",       main_state="FL", aum_total=180_000_000,   registration_status="Registered"),
    dict(crd_number=100004, legal_name="Summit Asset Management Corp",     business_name="Summit AM",         main_city="Denver",      main_state="CO", aum_total=320_000_000,   registration_status="Registered"),
    dict(crd_number=100005, legal_name="Lakeside Financial Advisors LLC",  business_name="Lakeside Financial",main_city="Chicago",     main_state="IL", aum_total=95_000_000,    registration_status="Registered"),
    dict(crd_number=100006, legal_name="Pacific Rim Investment Group",     business_name="Pacific Rim",       main_city="Los Angeles", main_state="CA", aum_total=410_000_000,   registration_status="Registered"),
    dict(crd_number=100007, legal_name="Harbor Point Capital LLC",         business_name="Harbor Point",      main_city="Boston",      main_state="MA", aum_total=730_000_000,   registration_status="Registered"),
    dict(crd_number=100008, legal_name="Desert Sky Wealth Management",     business_name="Desert Sky",        main_city="Phoenix",     main_state="AZ", aum_total=60_000_000,    registration_status="Registered"),
    dict(crd_number=100009, legal_name="Withdrawn Advisory Services Inc",  business_name=None,                main_city="Seattle",     main_state="WA", aum_total=None,           registration_status="Withdrawn"),
    dict(crd_number=100010, legal_name="Northgate Equity Partners LP",     business_name="Northgate Equity",  main_city="Dallas",      main_state="TX", aum_total=1_200_000_000, registration_status="Registered"),
]


@pytest.fixture()
def seeded_firms(db):
    """Insert 10 test firms and return the list of CRD numbers."""
    for data in SEED_FIRMS:
        f = Firm(
            **data,
            last_filing_date=datetime.date(2024, 3, 31),
        )
        db.add(f)
    db.flush()
    return [d["crd_number"] for d in SEED_FIRMS]


@pytest.fixture()
def seeded_platform(db, seeded_firms):
    """Insert one test platform and return it."""
    p = PlatformDefinition(name="Test Platform", description="For testing")
    db.add(p)
    db.flush()
    return p


@pytest.fixture()
def api_key_header(db):
    """
    Create a real API key in the test DB and return the Authorization header dict.
    """
    import hashlib, os
    from models.api_key import ApiKey

    raw_key = os.urandom(32).hex()
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    record = ApiKey(key_hash=key_hash, label="test-key", active=True)
    db.add(record)
    db.flush()
    return {"Authorization": f"Bearer {raw_key}"}
