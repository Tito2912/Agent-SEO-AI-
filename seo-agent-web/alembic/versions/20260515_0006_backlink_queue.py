"""Add queue fields to backlink_opportunities

Revision ID: 20260515_0006
Revises: 20260515_0005
Create Date: 2026-05-15
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260515_0006"
down_revision = "20260515_0005"
branch_labels = None
depends_on = None


def _has_column(conn, table: str, col: str) -> bool:
    try:
        return col in {c["name"] for c in inspect(conn).get_columns(table)}
    except Exception:
        return False


def upgrade() -> None:
    conn = op.get_bind()

    if not _has_column(conn, "backlink_opportunities", "queue_status"):
        op.add_column(
            "backlink_opportunities",
            sa.Column("queue_status", sa.String(32), nullable=True),
        )

    if not _has_column(conn, "backlink_opportunities", "posted_at"):
        op.add_column(
            "backlink_opportunities",
            sa.Column("posted_at", sa.DateTime(timezone=True), nullable=True),
        )

    if not _has_column(conn, "backlink_opportunities", "reddit_post_id"):
        op.add_column(
            "backlink_opportunities",
            sa.Column("reddit_post_id", sa.String(128), nullable=True),
        )

    if not _has_column(conn, "backlink_opportunities", "auto_found"):
        op.add_column(
            "backlink_opportunities",
            sa.Column(
                "auto_found",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            ),
        )


def downgrade() -> None:
    pass
