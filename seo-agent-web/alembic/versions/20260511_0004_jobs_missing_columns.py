"""add missing columns (owner_user_id, kind, slug) to jobs table

These columns were added to the JobRecord model after some deployments used
create_all() to bootstrap the schema. This migration adds them safely if absent.

Revision ID: 20260511_0004
Revises: 20260510_0003
Create Date: 2026-05-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260511_0004"
down_revision = "20260510_0003"
branch_labels = None
depends_on = None


def _has_column(conn, table: str, column: str) -> bool:
    try:
        cols = [c["name"] for c in inspect(conn).get_columns(table)]
        return column in cols
    except Exception:
        return False


def _has_index(conn, table: str, index_name: str) -> bool:
    try:
        idxs = [i["name"] for i in inspect(conn).get_indexes(table)]
        return index_name in idxs
    except Exception:
        return False


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

    if not _has_table(conn, "jobs"):
        return

    # owner_user_id — added as nullable (existing rows have no owner; FK not enforced here
    # to avoid constraint failures on historical data).
    if not _has_column(conn, "jobs", "owner_user_id"):
        op.add_column("jobs", sa.Column("owner_user_id", sa.String(36), nullable=True))
        if not _has_index(conn, "jobs", "ix_jobs_owner_user_id"):
            op.create_index("ix_jobs_owner_user_id", "jobs", ["owner_user_id"])

    if not _has_column(conn, "jobs", "kind"):
        op.add_column(
            "jobs",
            sa.Column("kind", sa.String(32), nullable=False, server_default=""),
        )
        if not _has_index(conn, "jobs", "ix_jobs_kind"):
            op.create_index("ix_jobs_kind", "jobs", ["kind"])

    if not _has_column(conn, "jobs", "slug"):
        op.add_column(
            "jobs",
            sa.Column("slug", sa.String(128), nullable=False, server_default=""),
        )
        if not _has_index(conn, "jobs", "ix_jobs_slug"):
            op.create_index("ix_jobs_slug", "jobs", ["slug"])

    # Composite indices (may already exist)
    if not _has_index(conn, "jobs", "ix_jobs_status_created"):
        try:
            op.create_index("ix_jobs_status_created", "jobs", ["status", "created_at"])
        except Exception:
            pass

    if not _has_index(conn, "jobs", "ix_jobs_owner_created"):
        try:
            op.create_index("ix_jobs_owner_created", "jobs", ["owner_user_id", "created_at"])
        except Exception:
            pass


def downgrade() -> None:
    conn = op.get_bind()
    if not _has_table(conn, "jobs"):
        return
    for idx in ["ix_jobs_owner_created", "ix_jobs_owner_user_id", "ix_jobs_kind", "ix_jobs_slug"]:
        if _has_index(conn, "jobs", idx):
            try:
                op.drop_index(idx, table_name="jobs")
            except Exception:
                pass
    for col in ["slug", "kind", "owner_user_id"]:
        if _has_column(conn, "jobs", col):
            op.drop_column("jobs", col)
