"""Owner-selected media call limits; existing attempts and mandates stay untouched."""

from alembic import op
import sqlalchemy as sa

revision = "20261115_0032"
down_revision = "20261114_0031"
branch_labels = None
depends_on = None


def upgrade():
    if "own_social_media_policy" in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        "own_social_media_policy",
        sa.Column(
            "owner_id",
            sa.String(36),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("daily_calls", sa.Integer(), nullable=False),
        sa.Column("token", sa.String(32), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )


def downgrade():
    if "own_social_media_policy" in sa.inspect(op.get_bind()).get_table_names():
        op.drop_table("own_social_media_policy")
