"""
Firm factory helpers for tests.

Usage:
    def test_something(db):
        firm = firm_factory(db, crd_number=100001, main_state="CA")

Factories return the persisted ORM object (already flushed so the PK is set).
"""
from __future__ import annotations

import datetime
import itertools
from typing import Any


# Monotonic CRD counter shared across factory calls so tests don't have to
# pick unique CRDs by hand. Starts at 900001 to avoid colliding with the
# static SEED_FIRMS (100001–100010).
_CRD_COUNTER = itertools.count(900001)


def _next_crd() -> int:
    return next(_CRD_COUNTER)


def firm_factory(
    db,
    *,
    crd_number: int | None = None,
    legal_name: str | None = None,
    business_name: str | None = None,
    main_city: str = "Test City",
    main_state: str = "NY",
    main_zip: str = "10001",
    aum_total: int | None = 100_000_000,
    registration_status: str = "Registered",
    last_filing_date: datetime.date | None = None,
    **extra: Any,
):
    """
    Create and flush a Firm row with sensible defaults for any field not given.

    Returns the ORM object (PK populated).
    """
    from models.firm import Firm

    crd = crd_number if crd_number is not None else _next_crd()
    name = legal_name or f"Test Firm {crd} LLC"
    last_date = last_filing_date or datetime.date(2025, 3, 31)

    firm = Firm(
        crd_number=crd,
        legal_name=name,
        business_name=business_name,
        main_city=main_city,
        main_state=main_state,
        main_zip=main_zip,
        aum_total=aum_total,
        registration_status=registration_status,
        last_filing_date=last_date,
        **extra,
    )
    db.add(firm)
    db.flush()
    return firm


def registered_firm(db, **overrides):
    """Shortcut: an active Registered firm with post-2025 last_filing_date."""
    defaults = {
        "registration_status": "Registered",
        "last_filing_date": datetime.date(2025, 3, 31),
        "aum_total": 250_000_000,
    }
    defaults.update(overrides)
    return firm_factory(db, **defaults)


def withdrawn_firm(db, **overrides):
    """Shortcut: a Withdrawn firm (post-2025, advW-sourced)."""
    defaults = {
        "registration_status": "Withdrawn",
        "last_filing_date": datetime.date(2025, 2, 15),
        "aum_total": None,
    }
    defaults.update(overrides)
    return firm_factory(db, **defaults)


def inactive_firm(db, **overrides):
    """Shortcut: a pre-2025 Inactive firm (derived status, no advW record)."""
    defaults = {
        "registration_status": "Inactive",
        "last_filing_date": datetime.date(2019, 6, 30),
        "aum_total": 50_000_000,
    }
    defaults.update(overrides)
    return firm_factory(db, **defaults)


def make_firms(db, n: int, **shared_overrides):
    """Bulk factory — inserts `n` firms sharing the given overrides."""
    return [firm_factory(db, **shared_overrides) for _ in range(n)]
