from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .models import Base


def _normalize_database_url(url: str) -> str:
    u = (url or "").strip()
    if u.startswith("postgres://"):
        return "postgresql+psycopg://" + u[len("postgres://") :]
    if u.startswith("postgresql://"):
        return "postgresql+psycopg://" + u[len("postgresql://") :]
    return u


def _default_sqlite_url(data_dir: Path) -> str:
    db_path = (data_dir / "seo-agent.db").resolve()
    return f"sqlite:///{db_path}"


class Database:
    def __init__(self, *, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

        raw_url = str(os.environ.get("DATABASE_URL") or "").strip()
        url = _normalize_database_url(raw_url) if raw_url else _default_sqlite_url(self.data_dir)

        connect_args: dict[str, object] = {}
        if url.startswith("sqlite:///"):
            connect_args = {"check_same_thread": False}

        self.engine: Engine = create_engine(
            url,
            pool_pre_ping=True,
            future=True,
            connect_args=connect_args,
        )
        if url.startswith("sqlite:///"):
            # SQLite ignores foreign keys unless asked, per connection. Without this, every
            # `ondelete="CASCADE"` in models.py is decoration: deleting a project leaves its
            # issue tasks, backlink opportunities and tracked keywords behind, and a row can be
            # written against a parent that does not exist. Postgres — production — enforces
            # them, so the two engines disagreed silently, which is the worst way to disagree.
            @event.listens_for(self.engine, "connect")
            def _enforce_sqlite_foreign_keys(dbapi_connection, _record):  # noqa: ANN001
                cursor = dbapi_connection.cursor()
                try:
                    cursor.execute("PRAGMA foreign_keys=ON")
                finally:
                    cursor.close()

        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            future=True,
        )

    def create_tables(self) -> None:
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session(self) -> Iterator[Session]:
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()

