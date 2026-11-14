"""Owner-only social demo production. Additive and create_all-compatible."""

from alembic import op
import sqlalchemy as sa

revision = "20261114_0031"
down_revision = "20261112_0030"
branch_labels = None
depends_on = None


def upgrade():
    if "own_social_media" in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        "own_social_media",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "owner_id",
            sa.String(36),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("authorization_key", sa.String(32), nullable=False),
        sa.Column("binding", sa.String(64), nullable=False),
        sa.Column("connection_hash", sa.String(64), nullable=False),
        sa.Column("input_hash", sa.String(64), nullable=False),
        sa.Column("packet", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=False),
        sa.Column("state", sa.String(24), nullable=False),
        sa.Column("error_code", sa.String(40)),
        sa.Column("provider_calls", sa.Integer(), nullable=False),
        sa.Column("render_attempts", sa.Integer(), nullable=False),
        sa.Column("provider_started_at", sa.DateTime(timezone=True)),
        sa.Column("lease_token", sa.String(32)),
        sa.Column("lease_until", sa.DateTime(timezone=True)),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "owner_id", "authorization_key", name="uq_own_social_media_authorization"
        ),
    )
    op.create_index("ix_own_social_media_owner_id", "own_social_media", ["owner_id"])


def downgrade():
    if "own_social_media" in sa.inspect(op.get_bind()).get_table_names():
        op.drop_table("own_social_media")
