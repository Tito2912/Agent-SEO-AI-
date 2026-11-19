"""Project-scoped action decisions, publication intents and observed page events."""
from alembic import op
import sqlalchemy as sa

revision = "20261117_0033"
down_revision = "20261115_0032"
branch_labels = None
depends_on = None


def upgrade():
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "cms_media_transfers" not in tables:
        op.create_table(
            "cms_media_transfers",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("task_id", sa.String(36), sa.ForeignKey("issue_tasks.id", ondelete="CASCADE"), nullable=False),
            sa.Column("media_key", sa.String(64), nullable=False),
            sa.Column("platform", sa.String(24), nullable=False),
            sa.Column("source_name", sa.String(255), nullable=False),
            sa.Column("state", sa.String(24), nullable=False),
            sa.Column("remote_url", sa.String(2048)),
            sa.Column("remote_id", sa.String(2048)),
            sa.Column("approved_by", sa.String(36), sa.ForeignKey("users.id", ondelete="SET NULL")),
            sa.Column("consent_digest", sa.String(64)),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("project_id", "media_key", name="uq_cms_media_transfer"),
        )
        op.create_index("ix_cms_media_transfers_project_id", "cms_media_transfers", ["project_id"])
    if "agent_action_choices" not in tables:
        op.create_table(
            "agent_action_choices",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("action_key", sa.String(64), nullable=False),
            sa.Column("state", sa.String(24), nullable=False),
            sa.Column("created_by", sa.String(36), sa.ForeignKey("users.id", ondelete="SET NULL")),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("project_id", "action_key", name="uq_agent_action_choice"),
        )
        op.create_index("ix_agent_action_choices_project_id", "agent_action_choices", ["project_id"])
    if "cms_publications" not in tables:
        op.create_table(
            "cms_publications",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("task_id", sa.String(36), sa.ForeignKey("issue_tasks.id", ondelete="CASCADE"), nullable=False),
            sa.Column("created_by", sa.String(36), sa.ForeignKey("users.id", ondelete="SET NULL")),
            sa.Column("intent_key", sa.String(64), nullable=False),
            sa.Column("platform", sa.String(24), nullable=False),
            sa.Column("language", sa.String(12), nullable=False),
            sa.Column("state", sa.String(24), nullable=False),
            sa.Column("data", sa.JSON(), nullable=False),
            sa.Column("remote_id", sa.String(255)),
            sa.Column("remote_url", sa.String(2048)),
            sa.Column("remote_revision", sa.String(255)),
            sa.Column("error_code", sa.String(80)),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("project_id", "intent_key", name="uq_cms_publication_intent"),
        )
        for name in ("project_id", "task_id", "state"):
            op.create_index("ix_cms_publications_" + name, "cms_publications", [name])
    if "cms_publication_consents" not in tables:
        op.create_table(
            "cms_publication_consents",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("publication_id", sa.String(36), sa.ForeignKey("cms_publications.id", ondelete="CASCADE"), nullable=False),
            sa.Column("actor_id", sa.String(36), sa.ForeignKey("users.id", ondelete="SET NULL")),
            sa.Column("operation", sa.String(24), nullable=False),
            sa.Column("content_digest", sa.String(64), nullable=False),
            sa.Column("targets", sa.JSON(), nullable=False),
            sa.Column("approved_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
        for name in ("project_id", "publication_id"):
            op.create_index("ix_cms_publication_consents_" + name, "cms_publication_consents", [name])
    if "agent_page_events" not in tables:
        op.create_table(
            "agent_page_events",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("project_id", sa.String(36), sa.ForeignKey("projects.id", ondelete="CASCADE"), nullable=False),
            sa.Column("event_key", sa.String(100), nullable=False),
            sa.Column("page_url", sa.String(2048), nullable=False),
            sa.Column("kind", sa.String(32), nullable=False),
            sa.Column("data", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("project_id", "event_key", name="uq_agent_page_event"),
        )
        op.create_index("ix_agent_page_events_project_id", "agent_page_events", ["project_id"])


def downgrade():
    for name in ("cms_publication_consents", "cms_media_transfers", "agent_page_events", "cms_publications", "agent_action_choices"):
        if name in sa.inspect(op.get_bind()).get_table_names():
            op.drop_table(name)
