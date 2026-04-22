"""
Migration roundtrip and schema-parity tests.

These tests provision a dedicated `secadv_test_migration` database via the
`migration_engine` fixture (conftest.py) and exercise:

1. Roundtrip: `alembic downgrade base` then `alembic upgrade head` leaves
   the schema identical. Catches migrations that are not bidirectional.
2. Head-reachable: upgrade head runs without error against an empty DB.
3. Schema parity (smoke): every ORM-declared table in Base.metadata exists
   in the migration-provisioned DB. Any table missing here means the model
   was added without the corresponding `alembic revision --autogenerate`.
4. firm_aum_annual view exists (created by migration 0005).
"""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect, text
from alembic import command
from alembic.config import Config as AlembicConfig

from models.base import Base


_ALEMBIC_INI = Path(__file__).parent.parent / "alembic.ini"
_ALEMBIC_DIR = Path(__file__).parent.parent / "alembic"


def _alembic_cfg(db_url_str: str) -> AlembicConfig:
    cfg = AlembicConfig(str(_ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url_str)
    cfg.set_main_option("script_location", str(_ALEMBIC_DIR))
    return cfg


# ── Schema parity ───────────────────────────────────────────────────────────

class TestSchemaParity:
    def test_all_orm_tables_present_in_migrated_db(self, migration_engine):
        """Every table declared via ORM models must also exist in migrated DB."""
        inspector = inspect(migration_engine)
        migrated_tables = set(inspector.get_table_names())
        orm_tables = {t.name for t in Base.metadata.tables.values()}

        missing = orm_tables - migrated_tables
        assert not missing, (
            f"{len(missing)} ORM table(s) missing from migrated DB — "
            f"likely need `alembic revision --autogenerate`: {sorted(missing)}"
        )

    def test_firm_aum_annual_view_exists(self, migration_engine):
        """Migration 0005 creates this view; it's how AUM trend queries run."""
        with migration_engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM information_schema.views "
                    "WHERE table_name = 'firm_aum_annual'"
                )
            ).first()
        assert row is not None, "firm_aum_annual view not created by migrations"

    def test_sync_manifest_unique_constraint(self, migration_engine):
        """sync_manifest has a UNIQUE (file_type, file_name) — load scripts rely on it."""
        inspector = inspect(migration_engine)
        unique_constraints = inspector.get_unique_constraints("sync_manifest")
        cols_sets = [tuple(c["column_names"]) for c in unique_constraints]
        assert ("file_type", "file_name") in cols_sets


# ── Roundtrip ───────────────────────────────────────────────────────────────

class TestRoundtrip:
    def test_downgrade_and_upgrade_preserves_schema(self, migration_engine):
        """downgrade → upgrade should leave the inspector-visible schema unchanged."""
        import os

        db_url_str = str(migration_engine.url.render_as_string(hide_password=False))

        inspector = inspect(migration_engine)
        tables_before = set(inspector.get_table_names())

        cfg = _alembic_cfg(db_url_str)

        saved = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = db_url_str
        try:
            command.downgrade(cfg, "base")
            # After full downgrade, only the alembic_version table should remain.
            inspector_post_down = inspect(migration_engine)
            tables_after_down = set(inspector_post_down.get_table_names())
            assert tables_after_down <= {"alembic_version"}

            command.upgrade(cfg, "head")
        finally:
            if saved is not None:
                os.environ["DATABASE_URL"] = saved
            else:
                os.environ.pop("DATABASE_URL", None)

        inspector_post_up = inspect(migration_engine)
        tables_after = set(inspector_post_up.get_table_names())
        assert tables_after == tables_before, (
            f"Roundtrip drifted the schema. Lost: {tables_before - tables_after}. "
            f"Gained: {tables_after - tables_before}"
        )
