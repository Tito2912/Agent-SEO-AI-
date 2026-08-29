"""rival domains a project is compared against, and the pages they publish

The crawled pages live in this table rather than in a report on disk. A competitor crawl
produces exactly one thing the product needs — what each page is about — and the worker has no
disk, so writing a report would mean an S3 round trip to read back three fields per page. A
hundred pages of {url, title, h1} is a few kilobytes, and keeping them here lets the comparison
be recomputed on every page load against the customer's own latest crawl.

Revision ID: 20260829_0009
Revises: 20260829_0008
Create Date: 2026-08-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260829_0009"
down_revision = "20260829_0008"
branch_labels = None
depends_on = None


def _has_table(conn, table: str) -> bool:
    try:
        return table in inspect(conn).get_table_names()
    except Exception:
        return False


def upgrade() -> None:
    conn = op.get_bind()

    # Guarded like every migration before it: `DB.create_tables()` runs on boot, so a fresh
    # deploy may already have this table and a blind CREATE would fail the deploy.
    if not _has_table(conn, "competitor_sites"):
        op.create_table(
            "competitor_sites",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("project_id", sa.String(length=36),
                      sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("user_id", sa.String(length=36),
                      sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
            sa.Column("domain", sa.String(length=255), nullable=False),
            sa.Column("base_url", sa.String(length=2048), nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False, server_default="new"),
            sa.Column("last_job_id", sa.String(length=36), nullable=True),
            sa.Column("last_crawled_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("pages_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("pages", sa.JSON(), nullable=True),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("project_id", "domain", name="uq_competitor_project_domain"),
        )
        op.create_index("ix_competitor_sites_project_id", "competitor_sites", ["project_id"])
        op.create_index("ix_competitor_sites_user_id", "competitor_sites", ["user_id"])
        op.create_index("ix_competitor_project_status", "competitor_sites",
                        ["project_id", "status"])


def downgrade() -> None:
    conn = op.get_bind()
    if _has_table(conn, "competitor_sites"):
        op.drop_table("competitor_sites")
