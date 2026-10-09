"""Durable correction receipts and external-write recovery intents."""

import sqlalchemy as sa
from alembic import op

revision = "20261003_0013"
down_revision = "20260919_0012"
branch_labels = None
depends_on = None


def upgrade():
    if "correction_operations" in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        "correction_operations",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("payer_id", sa.String(36), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
        sa.Column("request_key", sa.String(64), nullable=True),
        sa.Column("action", sa.String(64), nullable=False),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("charged", sa.Boolean(), nullable=False),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("request_key"),
    )
    for column in ("payer_id", "project_id", "state"):
        op.create_index("ix_correction_operations_" + column, "correction_operations", [column])


def downgrade():
    op.drop_table("correction_operations")
