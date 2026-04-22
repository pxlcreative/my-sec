"""
pytest fixtures for the SEC Adviser test suite.

Isolation strategy
------------------
- Tests ALWAYS hit an isolated `secadv_test` database. We force this before
  importing anything that reads settings, so the container's production
  DATABASE_URL cannot leak in.
- The test DB is auto-created at session start (so `make test` just works
  on a fresh install).
- Schema is rebuilt with `DROP SCHEMA public CASCADE` + create_all at the
  start of every session — this ignores Alembic migrations and doesn't
  create the `firm_aum_annual` view. Tests that need migrations should
  use the `migration_db` fixture instead.
- Each test runs in a transaction that is rolled back, so no test can see
  another test's writes even when they share the session.

Mock fixtures
-------------
External services are mocked by default:
- `mock_iapd`           — canned IAPD responses from tests/fixtures/iapd/
- `mock_es`             — in-memory ES client (records indexes, supports search)
- `mock_smtp`           — captures outbound email without hitting SMTP
- `mock_sec_requests`   — HTTP mock for SEC downloads (reports_metadata,
                          advFilingData ZIPs, advW ZIPs) backed by
                          tests/fixtures/sec/
- `celery_eager`        — runs `.delay()` tasks synchronously in-test
- `tmp_data_dir`        — points `settings.data_dir` at a temp dir so
                          writes never touch ./data
- `frozen_time`         — freezes `datetime.now` / `date.today` at 2026-04-21
"""
from __future__ import annotations

import datetime
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

# ── Test DB URL resolution ──────────────────────────────────────────────────
# Must happen BEFORE importing anything that reads settings.

def _resolve_test_db_url() -> str:
    """
    Pick a sensible default for the test DB URL.

    Explicit override wins: if TEST_DATABASE_URL is set, use it as-is.
    Otherwise choose between in-container and host based on /.dockerenv.
    """
    if explicit := os.environ.get("TEST_DATABASE_URL"):
        return explicit

    # Credentials match docker-compose.yml defaults (secadv/secadv).
    host = "postgres" if Path("/.dockerenv").exists() else "localhost"
    return f"postgresql://secadv:secadv@{host}:5432/secadv_test"


_TEST_DB_URL = _resolve_test_db_url()

# Unconditionally override — do NOT use setdefault; the api container sets
# DATABASE_URL to the production DB via env_file, which would leak into tests.
os.environ["DATABASE_URL"] = _TEST_DB_URL
os.environ.setdefault("ELASTICSEARCH_URL", "http://localhost:9200")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("SECRET_KEY", "test-secret-key-not-for-production")

# Make api/ importable.
_API_DIR = Path(__file__).parent.parent / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))

# Register every model so Base.metadata.create_all knows the full schema.
from sqlalchemy import create_engine, text  # noqa: E402
from sqlalchemy.orm import sessionmaker      # noqa: E402
from fastapi.testclient import TestClient    # noqa: E402

from db import get_db                        # noqa: E402
from main import app                         # noqa: E402
from models.base import Base                 # noqa: E402
from models import (                         # noqa: E402,F401
    firm as _firm_models,
    aum,
    brochure,
    platform,
    alert,
    sync_job,
    sync_manifest,
    export_job,
    api_key,
    cron_schedule,
)
from models.firm import Firm                 # noqa: E402
from models.platform import PlatformDefinition  # noqa: E402


# ── Fixture data paths ──────────────────────────────────────────────────────
FIXTURES_DIR = Path(__file__).parent / "fixtures"
IAPD_FIXTURES_DIR = FIXTURES_DIR / "iapd"
SEC_FIXTURES_DIR = FIXTURES_DIR / "sec"


# ── Engine / session factory ────────────────────────────────────────────────

_engine = create_engine(_TEST_DB_URL, pool_pre_ping=True, future=True)
_TestingSession = sessionmaker(autocommit=False, autoflush=False, bind=_engine)


def _ensure_test_db_exists() -> None:
    """
    CREATE DATABASE secadv_test if it doesn't exist yet.

    Connects to the server-side `postgres` DB using the same credentials as
    the test URL — safe to rerun; idempotent.
    """
    from sqlalchemy.engine.url import make_url

    url = make_url(_TEST_DB_URL)
    db_name = url.database
    # Connect to the default `postgres` maintenance DB to issue CREATE DATABASE.
    admin_url = url.set(database="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", future=True)
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :n"),
                {"n": db_name},
            ).first()
            if not exists:
                # db_name is from our controlled URL config — safe to interpolate.
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    finally:
        admin_engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def create_tables():
    """
    Provision a clean test schema once per pytest session.

    Uses `DROP SCHEMA public CASCADE` rather than `Base.metadata.drop_all`
    so any views left over from a previous migration-based run (e.g.
    firm_aum_annual) don't block the drop.
    """
    _ensure_test_db_exists()
    with _engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    Base.metadata.create_all(_engine)
    yield
    # Leave the schema in place for postmortem debugging; the next session
    # nukes it anyway.


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


# ── Seeded firm data (backwards-compat with existing tests) ─────────────────
# New tests should prefer the factories in tests/fixtures/firms.py.

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
        f = Firm(**data, last_filing_date=datetime.date(2024, 3, 31))
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
    """Create a real API key in the test DB and return an Authorization header."""
    import hashlib
    from models.api_key import ApiKey

    raw_key = os.urandom(32).hex()
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    record = ApiKey(key_hash=key_hash, label="test-key", active=True)
    db.add(record)
    db.flush()
    return {"Authorization": f"Bearer {raw_key}"}


# ── External-service mocks ──────────────────────────────────────────────────

def _load_iapd_fixture(name: str) -> dict:
    """Load one of the canned IAPD iacontent responses from tests/fixtures/iapd/."""
    path = IAPD_FIXTURES_DIR / f"{name}.json"
    if not path.exists():
        pytest.fail(f"IAPD fixture not found: {path}. Run tests/fixtures/generate.py?")
    return json.loads(path.read_text())


@pytest.fixture()
def iapd_fixtures() -> dict[str, dict]:
    """Return a dict of {name: parsed iacontent} for every file in fixtures/iapd/."""
    out: dict[str, dict] = {}
    for f in IAPD_FIXTURES_DIR.glob("*.json"):
        out[f.stem] = json.loads(f.read_text())
    return out


@pytest.fixture()
def mock_iapd(monkeypatch, iapd_fixtures):
    """
    Patch services.iapd_client.fetch_firm to return canned responses.

    Registered CRDs (with fixtures):
      100001 → registered_firm
      100002 → withdrawn_firm
      100003 → edgar_old_firm   (pre-2025, no RegistrationStatus)
      100404 → raises ValueError ("No IAPD results")
      100429 → raises RuntimeError (rate-limited / permanent failure)

    Unknown CRDs raise ValueError by default. Use the `register` helper
    on the returned object to add more mappings inline in a test.
    """
    class _MockIapd:
        def __init__(self):
            self.responses: dict[int, Any] = {
                100001: iapd_fixtures.get("registered_firm"),
                100002: iapd_fixtures.get("withdrawn_firm"),
                100003: iapd_fixtures.get("edgar_old_firm"),
            }
            self.errors: dict[int, Exception] = {
                100404: ValueError(f"No IAPD results"),
                100429: RuntimeError("IAPD rate limit exceeded after retries"),
            }
            self.calls: list[int] = []

        def register(self, crd: int, response: dict | None = None, *, error: Exception | None = None) -> None:
            if error:
                self.errors[crd] = error
            else:
                self.responses[crd] = response

        def fetch(self, crd: int) -> dict:
            self.calls.append(crd)
            if crd in self.errors:
                raise self.errors[crd]
            if crd in self.responses and self.responses[crd] is not None:
                return self.responses[crd]
            raise ValueError(f"No IAPD results for CRD {crd}")

    mock = _MockIapd()

    import services.iapd_client as iapd_client
    monkeypatch.setattr(iapd_client, "fetch_firm", mock.fetch)
    return mock


@pytest.fixture()
def mock_es(monkeypatch):
    """
    In-memory replacement for es_client. Records indexed docs and supports
    a very simple exact/contains search on legal_name.

    Usage:
        def test_something(mock_es):
            mock_es.seed({"crd_number": 1, "legal_name": "Acme", ...})
            hits = mock_es.search("Acme")
    """
    import services.es_client as es_client

    class _MockEs:
        def __init__(self) -> None:
            self.docs: dict[str, dict] = {}  # keyed by str(crd)
            self.index_calls = 0
            self.search_calls = 0

        # Public helpers for tests
        def seed(self, doc: dict) -> None:
            self.docs[str(doc["crd_number"])] = doc

        def seed_many(self, docs: list[dict]) -> None:
            for d in docs:
                self.seed(d)

        def clear(self) -> None:
            self.docs.clear()

        # Functions that replace es_client module-level names
        def _create_index(self) -> None:
            # no-op; we have no real index
            pass

        def _index_firm(self, firm_dict: dict) -> None:
            self.seed(firm_dict)
            self.index_calls += 1

        def _bulk_index(self, firm_dicts: list[dict], batch_size: int = 500) -> int:
            for d in firm_dicts:
                self.seed(d)
            self.index_calls += len(firm_dicts)
            return len(firm_dicts)

        def _search(self, query: str, city: str | None = None, state: str | None = None,
                    size: int = 10) -> list[dict]:
            self.search_calls += 1
            q = (query or "").lower()
            results = []
            for doc in self.docs.values():
                legal = (doc.get("legal_name") or "").lower()
                business = (doc.get("business_name") or "").lower()
                if q in legal or q in business:
                    if state and (doc.get("main_state") or "").upper() != state.upper():
                        continue
                    if city and city.lower() not in (doc.get("main_city") or "").lower():
                        continue
                    results.append({**doc, "_score": 10.0})
                    if len(results) >= size:
                        break
            return results

    mock = _MockEs()
    monkeypatch.setattr(es_client, "create_index_if_not_exists", mock._create_index)
    monkeypatch.setattr(es_client, "index_firm", mock._index_firm)
    monkeypatch.setattr(es_client, "bulk_index_firms", mock._bulk_index)
    monkeypatch.setattr(es_client, "search_firms", mock._search)
    return mock


class _CapturedSmtp:
    """Records messages sent via smtplib.SMTP so tests can assert on them."""
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, str]] = []  # (from, to, body)
        self.login_calls: list[tuple[str, str]] = []
        self.starttls_called = False
        self.connections: list[tuple[str, int]] = []


@pytest.fixture()
def mock_smtp(monkeypatch):
    """
    Capture outbound SMTP instead of hitting a real mail server.

    Usage:
        def test_alert_email(mock_smtp):
            send_alert_email(...)
            assert len(mock_smtp.messages) == 1
    """
    captured = _CapturedSmtp()

    class _FakeSMTP:
        def __init__(self, host: str, port: int, *args, **kwargs) -> None:
            captured.connections.append((host, port))

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def starttls(self) -> None:
            captured.starttls_called = True

        def login(self, user: str, pw: str) -> None:
            captured.login_calls.append((user, pw))

        def send_message(self, msg) -> dict:
            captured.messages.append((
                str(msg.get("From", "")),
                str(msg.get("To", "")),
                msg.get_payload() if hasattr(msg, "get_payload") else str(msg),
            ))
            return {}

    import smtplib
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    return captured


@pytest.fixture()
def mock_sec_requests():
    """
    HTTP mock for SEC endpoints, backed by tests/fixtures/sec/.

    Uses the `responses` library. By default the following requests are
    registered:
      GET reports_metadata.json  → fixtures/sec/reports_metadata.json
      GET any advFilingData ZIP  → fixtures/sec/adv_filing_sample.zip
      GET any advW ZIP           → fixtures/sec/adv_w_sample.zip

    Tests can use `mock_sec_requests.add(...)` to register extra URLs.
    """
    import re
    import responses

    rsps = responses.RequestsMock(assert_all_requests_are_fired=False)
    rsps.start()

    metadata_path = SEC_FIXTURES_DIR / "reports_metadata.json"
    filing_zip_path = SEC_FIXTURES_DIR / "adv_filing_sample.zip"
    advw_zip_path = SEC_FIXTURES_DIR / "adv_w_sample.zip"

    if metadata_path.exists():
        rsps.add(
            responses.GET,
            "https://reports.adviserinfo.sec.gov/reports/foia/reports_metadata.json",
            body=metadata_path.read_text(),
            status=200,
            content_type="application/json",
        )
    if filing_zip_path.exists():
        rsps.add(
            responses.GET,
            re.compile(r"https://reports\.adviserinfo\.sec\.gov/.*advFilingData/.*\.zip"),
            body=filing_zip_path.read_bytes(),
            status=200,
            content_type="application/zip",
        )
    if advw_zip_path.exists():
        rsps.add(
            responses.GET,
            re.compile(r"https://reports\.adviserinfo\.sec\.gov/.*advW/.*\.zip"),
            body=advw_zip_path.read_bytes(),
            status=200,
            content_type="application/zip",
        )

    try:
        yield rsps
    finally:
        rsps.stop()
        rsps.reset()


@pytest.fixture()
def celery_eager():
    """
    Run Celery `.delay()` tasks synchronously in-test.

    Each invocation saves and restores the original app.conf settings, so
    tests can opt in to eager mode without polluting other tests.
    """
    from celery_tasks.app import app as celery_app

    old_always_eager = celery_app.conf.task_always_eager
    old_eager_propagates = celery_app.conf.task_eager_propagates
    old_store_eager_result = celery_app.conf.task_store_eager_result

    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True
    celery_app.conf.task_store_eager_result = True

    try:
        yield celery_app
    finally:
        celery_app.conf.task_always_eager = old_always_eager
        celery_app.conf.task_eager_propagates = old_eager_propagates
        celery_app.conf.task_store_eager_result = old_store_eager_result


@pytest.fixture()
def tmp_data_dir(monkeypatch, tmp_path):
    """
    Point settings.data_dir and related paths at a per-test tmp directory
    so exports, brochures, and bulk downloads don't touch ./data.
    """
    from config import settings

    monkeypatch.setattr(settings, "data_dir", str(tmp_path))

    (tmp_path / "raw" / "csv").mkdir(parents=True, exist_ok=True)
    (tmp_path / "exports").mkdir(parents=True, exist_ok=True)
    (tmp_path / "brochures").mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture()
def frozen_time():
    """
    Freeze `datetime.now()` and `date.today()` at 2026-04-21 (current test date).

    Apply with `with frozen_time:` to bracket a block, or access
    `frozen_time.move_to("2027-01-01")` to jump forward.
    """
    from freezegun import freeze_time

    with freeze_time("2026-04-21") as frozen:
        yield frozen


# ── Migration-backed schema fixture (for migration roundtrip tests) ────────

@pytest.fixture(scope="session")
def migration_engine():
    """
    Separate engine connected to `secadv_migration_test` DB, provisioned via
    `alembic upgrade head` so migration-created objects (views, triggers)
    are present. Use this for tests that verify schema parity or migration
    roundtrips; regular tests should use `db` instead.
    """
    from sqlalchemy.engine.url import make_url
    from alembic import command
    from alembic.config import Config as AlembicConfig

    base_url = make_url(_TEST_DB_URL)
    mig_db_name = (base_url.database or "secadv_test") + "_migration"
    admin_engine = create_engine(
        base_url.set(database="postgres"),
        isolation_level="AUTOCOMMIT",
        future=True,
    )
    try:
        with admin_engine.connect() as conn:
            conn.execute(
                text(f'DROP DATABASE IF EXISTS "{mig_db_name}" WITH (FORCE)')
            )
            conn.execute(text(f'CREATE DATABASE "{mig_db_name}"'))
    finally:
        admin_engine.dispose()

    mig_url = base_url.set(database=mig_db_name)
    engine = create_engine(mig_url, future=True)

    # SQLAlchemy's URL.__str__ masks the password with ***; we need the real
    # DSN string when handing it to Alembic and to the environment, or the
    # inner connection attempt will fail auth.
    mig_url_str = mig_url.render_as_string(hide_password=False)

    alembic_ini = Path(__file__).parent.parent / "alembic.ini"
    cfg = AlembicConfig(str(alembic_ini))
    cfg.set_main_option("sqlalchemy.url", mig_url_str)
    cfg.set_main_option("script_location", str(Path(__file__).parent.parent / "alembic"))

    # alembic/env.py reads DATABASE_URL directly from os.environ, so swap it
    # for the duration of the upgrade and restore the test URL afterwards.
    saved_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = mig_url_str
    try:
        command.upgrade(cfg, "head")
    finally:
        if saved_url is not None:
            os.environ["DATABASE_URL"] = saved_url
        else:
            os.environ.pop("DATABASE_URL", None)

    yield engine
    engine.dispose()
