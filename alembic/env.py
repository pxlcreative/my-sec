import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# ---------------------------------------------------------------------------
# Path setup — make `api/` importable so models can be discovered
# ---------------------------------------------------------------------------
# alembic/ and api/ are siblings under the project root in both environments:
#   - Container:  /project/alembic/  →  /project/api/
#   - Locally:    sec-adv/alembic/   →  sec-adv/api/
_api_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "api")
sys.path.insert(0, os.path.normpath(_api_dir))

# ---------------------------------------------------------------------------
# Import all models so Base.metadata is populated for autogenerate
# ---------------------------------------------------------------------------
import models  # noqa: F401 — side-effect: registers all ORM classes
from models.base import Base

# ---------------------------------------------------------------------------
# Alembic Config object
# ---------------------------------------------------------------------------
config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# ---------------------------------------------------------------------------
# Inject DATABASE_URL from environment / .env
# ---------------------------------------------------------------------------
def get_database_url() -> str:
    # Try environment variable first (set by Docker Compose)
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    # Fall back to reading .env from the project root
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        from dotenv import load_dotenv
        load_dotenv(env_path)
        url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy .env.example → .env and fill it in."
        )
    return url


# ---------------------------------------------------------------------------
# Offline mode (no DB connection, just generates SQL)
# ---------------------------------------------------------------------------
def run_migrations_offline() -> None:
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Online mode (connects to DB)
# ---------------------------------------------------------------------------
def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_database_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
