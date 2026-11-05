"""Private owner approvals; no enabled channel or mandate is created by upgrade."""

import sqlalchemy as sa
from alembic import op

revision = "20261105_0026"
down_revision = "20261104_0025"
branch_labels = None
depends_on = None


def upgrade():
    tables = sa.inspect(op.get_bind()).get_table_names()
    if "telegram_approval_channels" not in tables:
        op.create_table(
            "telegram_approval_channels",
            sa.Column("id", sa.String(16), primary_key=True),
            sa.Column(
                "owner_id",
                sa.String(36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("enabled", sa.Boolean(), nullable=False),
            sa.Column("binding", sa.String(32), nullable=False),
            sa.Column("config_hash", sa.String(64), nullable=False),
            sa.Column("bot_id", sa.String(24), nullable=False),
            sa.Column("chat_id", sa.String(24), nullable=False),
            sa.Column("lang", sa.String(2), nullable=False),
            sa.Column("offset", sa.String(24), nullable=False),
            sa.Column("lease_token", sa.String(32), nullable=True),
            sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
            sa.Column("error_code", sa.String(40), nullable=True),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
        )
    if "telegram_approval_requests" not in tables:
        op.create_table(
            "telegram_approval_requests",
            sa.Column("id", sa.String(32), primary_key=True),
            sa.Column(
                "owner_id",
                sa.String(36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("binding", sa.String(32), nullable=False),
            sa.Column("kind", sa.String(24), nullable=False),
            sa.Column("target_id", sa.String(80), nullable=False),
            sa.Column("version", sa.String(64), nullable=False),
            sa.Column("pages", sa.JSON(), nullable=False),
            sa.Column("state", sa.String(24), nullable=False),
            sa.Column("nonce", sa.String(12), nullable=False),
            sa.Column("confirmation", sa.String(12), nullable=True),
            sa.Column("choice", sa.String(8), nullable=True),
            sa.Column("seen", sa.Integer(), nullable=False),
            sa.Column("delivery", sa.String(16), nullable=False),
            sa.Column("delivery_until", sa.DateTime(timezone=True), nullable=True),
            sa.Column("message_id", sa.String(24), nullable=True),
            sa.Column("error_code", sa.String(40), nullable=True),
            sa.Column("result_id", sa.String(36), nullable=True),
            sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.UniqueConstraint(
                "binding",
                "kind",
                "target_id",
                "version",
                name="uq_telegram_approval_version",
            ),
        )


def downgrade():
    op.drop_table("telegram_approval_requests")
    op.drop_table("telegram_approval_channels")
