"""Only report a database read that was actually observed; never expose its exception."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select


def database_read_available(database: Any) -> bool:
    try:
        with database.session() as session:
            session.execute(select(1))
        return True
    except Exception:
        # Driver exceptions may contain connection strings or SQL parameters.
        return False
