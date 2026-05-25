"""add timezone, country, language to users table

Revision ID: 20260525_0007
Revises: 20260515_0006
Create Date: 2026-05-25
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260525_0007"
down_revision = "20260515_0006"
branch_labels = None
depends_on = None


def _has_column(conn, table: str, column: str) -> bool:
    try:
        cols = [c["name"] for c in inspect(conn).get_columns(table)]
        return column in cols
    except Exception:
        return False


def upgrade() -> None:
    conn = op.get_bind()
    if not _has_column(conn, "users", "timezone"):
        op.add_column("users", sa.Column("timezone", sa.String(64), nullable=True))
    if not _has_column(conn, "users", "country"):
        op.add_column("users", sa.Column("country", sa.String(4), nullable=True))
    if not _has_column(conn, "users", "language"):
        op.add_column("users", sa.Column("language", sa.String(10), nullable=True))


def downgrade() -> None:
    conn = op.get_bind()
    for col in ["language", "country", "timezone"]:
        if _has_column(conn, "users", col):
            op.drop_column("users", col)
