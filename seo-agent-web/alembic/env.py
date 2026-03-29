from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
import sqlalchemy as sa
from sqlalchemy import pool


def _normalize_database_url(url: str) -> str:
    u = (url or "").strip()
    if u.startswith("postgres://"):
        return "postgresql+psycopg://" + u[len("postgres://") :]
    if u.startswith("postgresql://"):
        return "postgresql+psycopg://" + u[len("postgresql://") :]
    return u


def _default_sqlite_url() -> str:
    web_root = Path(__file__).resolve().parents[1]
    data_dir = Path(os.environ.get("SEO_AGENT_DATA_DIR") or (web_root / "data")).expanduser()
    db_path = (data_dir / "seo-agent.db").resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{db_path}"


def _database_url() -> str:
    raw = str(os.environ.get("DATABASE_URL") or "").strip()
    if raw:
        return _normalize_database_url(raw)
    return _default_sqlite_url()


# Ensure `backend.*` imports work even if running Alembic from the repo root.
WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


from backend.models import Base  # noqa: E402  (import after sys.path tweak)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = _database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        render_as_batch=url.startswith("sqlite:///"),
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    url = _database_url()
    connect_args: dict[str, object] = {}
    if url.startswith("sqlite:///"):
        connect_args = {"check_same_thread": False}

    engine = sa.create_engine(
        url,
        poolclass=pool.NullPool,
        connect_args=connect_args,
        future=True,
    )

    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            render_as_batch=url.startswith("sqlite:///"),
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

