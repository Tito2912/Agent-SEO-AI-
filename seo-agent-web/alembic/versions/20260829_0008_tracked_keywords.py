"""tracked keywords and their per-day snapshots

Search Console keeps roughly sixteen months and offers no per-project history, so following a
query over time means storing it. Two tables rather than one: the keyword is a DECISION (this
query matters, this page should win it) and a snapshot is an OBSERVATION. Editing the target
page must not rewrite the past, and re-measuring a day already recorded must overwrite rather
than accumulate — which is what the unique constraint on (keyword_id, captured_on) buys.

Revision ID: 20260829_0008
Revises: 20260525_0007
Create Date: 2026-08-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260829_0008"
down_revision = "20260525_0007"
branch_labels = None
depends_on = None


def _has_table(conn, table: str) -> bool:
    try:
        return table in inspect(conn).get_table_names()
    except Exception:
        return False


def upgrade() -> None:
    conn = op.get_bind()

    # Guarded like the migrations before it: `DB.create_tables()` runs on boot, so a fresh
    # deploy may well have created these already and a blind CREATE would fail the deploy.
    if not _has_table(conn, "tracked_keywords"):
        op.create_table(
            "tracked_keywords",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("project_id", sa.String(length=36),
                      sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("user_id", sa.String(length=36),
                      sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
            sa.Column("query", sa.String(length=512), nullable=False),
            sa.Column("target_url", sa.String(length=2048), nullable=True),
            sa.Column("source", sa.String(length=32), nullable=False, server_default="manual"),
            sa.Column("status", sa.String(length=32), nullable=False, server_default="tracked"),
            sa.Column("notes", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("project_id", "query", name="uq_tracked_kw_project_query"),
        )
        op.create_index("ix_tracked_keywords_project_id", "tracked_keywords", ["project_id"])
        op.create_index("ix_tracked_keywords_user_id", "tracked_keywords", ["user_id"])
        op.create_index("ix_tracked_kw_project_status", "tracked_keywords",
                        ["project_id", "status"])

    if not _has_table(conn, "tracked_keyword_snapshots"):
        op.create_table(
            "tracked_keyword_snapshots",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("keyword_id", sa.String(length=36),
                      sa.ForeignKey("tracked_keywords.id", ondelete="CASCADE"), nullable=False),
            sa.Column("captured_on", sa.Date(), nullable=False),
            sa.Column("clicks", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("impressions", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("ctr", sa.Float(), nullable=False, server_default="0"),
            sa.Column("position", sa.Float(), nullable=False, server_default="0"),
            sa.Column("page", sa.String(length=2048), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("keyword_id", "captured_on", name="uq_tracked_kw_snapshot_day"),
        )
        op.create_index("ix_tracked_keyword_snapshots_keyword_id",
                        "tracked_keyword_snapshots", ["keyword_id"])
        op.create_index("ix_tracked_kw_snapshot_kw_day", "tracked_keyword_snapshots",
                        ["keyword_id", "captured_on"])


def downgrade() -> None:
    conn = op.get_bind()
    # Snapshots first: they carry the foreign key.
    if _has_table(conn, "tracked_keyword_snapshots"):
        op.drop_table("tracked_keyword_snapshots")
    if _has_table(conn, "tracked_keywords"):
        op.drop_table("tracked_keywords")
