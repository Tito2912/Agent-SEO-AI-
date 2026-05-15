"""add issue_tasks table

Revision ID: 20260515_0005
Revises: 20260511_0004
Create Date: 2026-05-15
"""
from __future__ import annotations
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "20260515_0005"
down_revision = "20260511_0004"
branch_labels = None
depends_on = None


def _has_table(conn, name):
    try:
        return bool(inspect(conn).has_table(name))
    except Exception:
        try:
            return name in set(inspect(conn).get_table_names())
        except Exception:
            return False


def upgrade():
    conn = op.get_bind()
    if not _has_table(conn, "issue_tasks"):
        op.create_table(
            "issue_tasks",
            sa.Column("id", sa.String(36), primary_key=True, nullable=False),
            sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("user_id", sa.String(36), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
            sa.Column("issue_key", sa.String(255), nullable=False),
            sa.Column("issue_label", sa.String(512), nullable=False, server_default=""),
            sa.Column("crawl_ts", sa.String(32), nullable=False, server_default=""),
            sa.Column("url", sa.String(2048), nullable=True),
            sa.Column("severity", sa.String(32), nullable=False, server_default="notice"),
            sa.Column("status", sa.String(32), nullable=False, server_default="todo"),
            sa.Column("note", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("project_id", "issue_key", "url", name="uq_issue_task_project_issue_url"),
        )
        op.create_index("ix_issue_tasks_project_status", "issue_tasks", ["project_id", "status"])
        op.create_index("ix_issue_tasks_project_id", "issue_tasks", ["project_id"])
        op.create_index("ix_issue_tasks_issue_key", "issue_tasks", ["issue_key"])
        op.create_index("ix_issue_tasks_status", "issue_tasks", ["status"])


def downgrade():
    conn = op.get_bind()
    if _has_table(conn, "issue_tasks"):
        op.drop_table("issue_tasks")
