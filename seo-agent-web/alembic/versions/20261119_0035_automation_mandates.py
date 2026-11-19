"""Functional automation grants and single-use runs, separate from retained evidence."""
from alembic import op
import sqlalchemy as sa

revision = "20261119_0035"
down_revision = "20261119_0034"
branch_labels = None
depends_on = None


def upgrade():
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "automation_mandates" not in tables:
        op.create_table("automation_mandates",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("mode", sa.String(32), nullable=False),
            sa.Column("scope_key", sa.String(64), nullable=False),
            sa.Column("signing_secret", sa.String(64), nullable=False),
            sa.Column("config", sa.JSON(), nullable=False),
            sa.Column("actor_id", sa.String(36), sa.ForeignKey("users.id", ondelete="SET NULL")),
            sa.Column("receipt_id", sa.String(36), sa.ForeignKey("cms_publication_consents.id", ondelete="SET NULL")),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("project_id", "mode", name="uq_automation_mandate_project_mode"))
        op.create_index("ix_automation_mandates_project_id", "automation_mandates", ["project_id"])
    if "automation_runs" not in tables:
        op.create_table("automation_runs",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("mode", sa.String(32), nullable=False),
            sa.Column("mandate_key", sa.String(36), nullable=False),
            sa.Column("request_key", sa.String(64), nullable=False),
            sa.Column("state", sa.String(24), nullable=False),
            sa.Column("reason", sa.String(48)),
            sa.Column("data", sa.JSON(), nullable=False),
            sa.Column("provider_calls", sa.Integer(), nullable=False),
            sa.Column("writes", sa.Integer(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("finished_at", sa.DateTime(timezone=True)),
            sa.UniqueConstraint("project_id", "mode", "request_key", name="uq_automation_run_request"))
        op.create_index("ix_automation_runs_project_id", "automation_runs", ["project_id"])
        op.create_index("ix_automation_runs_state", "automation_runs", ["state"])


def downgrade():
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    # Removing live grants/runs would discard revocation or duplicate protection.
    for name in ("automation_runs", "automation_mandates"):
        if name in tables and op.get_bind().execute(sa.text("SELECT COUNT(*) FROM " + name)).scalar():
            raise RuntimeError("Descente refusee : des autorisations ou executions automatiques sont conservees.")
    for name in ("automation_runs", "automation_mandates"):
        if name in tables:
            op.drop_table(name)
