"""add rate_limit_buckets table

Revision ID: 20260510_0003
Revises: 20260510_0002
Create Date: 2026-05-10
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260510_0003"
down_revision = "20260510_0002"
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    try:
        return bool(inspect(conn).has_table(name))
    except Exception:
        try:
            return name in set(inspect(conn).get_table_names())
        except Exception:
            return False


def upgrade() -> None:
    conn = op.get_bind()
    if not _has_table(conn, "rate_limit_buckets"):
        op.create_table(
            "rate_limit_buckets",
            sa.Column("key", sa.String(255), primary_key=True, nullable=False),
            sa.Column("hits_json", sa.Text(), nullable=False, server_default="[]"),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
        )
        op.create_index("ix_rate_limit_buckets_updated_at", "rate_limit_buckets", ["updated_at"])


def downgrade() -> None:
    conn = op.get_bind()
    if _has_table(conn, "rate_limit_buckets"):
        op.drop_table("rate_limit_buckets")
