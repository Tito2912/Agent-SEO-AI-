"""add backlink_opportunities table

Revision ID: 20260510_0002
Revises: 20260329_0001
Create Date: 2026-05-10
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260510_0002"
down_revision = "20260329_0001"
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

    if not _has_table(conn, "backlink_opportunities"):
        op.create_table(
            "backlink_opportunities",
            sa.Column("id", sa.String(36), primary_key=True, nullable=False),
            sa.Column(
                "project_id",
                sa.String(36),
                sa.ForeignKey("projects.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "user_id",
                sa.String(36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("source", sa.String(32), nullable=False),
            sa.Column("title", sa.String(1024), nullable=False),
            sa.Column("url", sa.String(2048), nullable=False),
            sa.Column("snippet", sa.Text(), nullable=True),
            sa.Column("opportunity_score", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("status", sa.String(32), nullable=False, server_default="new"),
            sa.Column("reply", sa.Text(), nullable=True),
            sa.Column("target_url", sa.String(2048), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("project_id", "url", name="uq_backlink_opp_project_url"),
        )
        op.create_index("ix_backlink_opp_project_status", "backlink_opportunities", ["project_id", "status"])
        op.create_index("ix_backlink_opp_user_created", "backlink_opportunities", ["user_id", "created_at"])
        op.create_index("ix_backlink_opportunities_project_id", "backlink_opportunities", ["project_id"])
        op.create_index("ix_backlink_opportunities_user_id", "backlink_opportunities", ["user_id"])


def downgrade() -> None:
    conn = op.get_bind()
    if _has_table(conn, "backlink_opportunities"):
        op.drop_table("backlink_opportunities")
