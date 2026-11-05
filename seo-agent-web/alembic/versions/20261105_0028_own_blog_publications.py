"""Own blog publication receipts; upgrade grants no publication authority."""

import sqlalchemy as sa
from alembic import op

revision = "20261105_0028"
down_revision = "20261105_0027"
branch_labels = None
depends_on = None


def upgrade():
    if "own_blog_publications" in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        "own_blog_publications",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "owner_id",
            sa.String(36),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "project_id",
            sa.String(36),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "fr_id",
            sa.String(36),
            sa.ForeignKey("own_seo_drafts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "en_id",
            sa.String(36),
            sa.ForeignKey("own_seo_drafts.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("workspace_token", sa.String(32), nullable=False),
        sa.Column("authorization_key", sa.String(32), nullable=False),
        sa.Column("connection_hash", sa.String(64), nullable=False),
        sa.Column("slug", sa.String(80), nullable=False),
        sa.Column("proposal", sa.JSON(), nullable=False),
        sa.Column("receipt", sa.JSON(), nullable=False),
        sa.Column("intents", sa.JSON(), nullable=False),
        sa.Column("state", sa.String(24), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("reads", sa.Integer(), nullable=False),
        sa.Column("writes", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(40), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_check_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_token", sa.String(32), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "owner_id", "authorization_key", name="uq_own_blog_authorization"
        ),
    )
    op.create_index(
        "ix_own_blog_publications_owner_id", "own_blog_publications", ["owner_id"]
    )


def downgrade():
    op.drop_table("own_blog_publications")
