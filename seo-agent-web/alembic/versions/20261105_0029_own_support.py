"""Owner support mailbox and durable replies; upgrade enables no channel or send."""

import sqlalchemy as sa
from alembic import op

revision = "20261105_0029"
down_revision = "20261105_0028"
branch_labels = None
depends_on = None


def upgrade():
    tables = sa.inspect(op.get_bind()).get_table_names()
    if "own_support_channels" not in tables:
        op.create_table(
            "own_support_channels",
            sa.Column("id", sa.String(16), primary_key=True),
            sa.Column(
                "owner_id",
                sa.String(36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("binding", sa.String(32), nullable=False),
            sa.Column("mailbox", sa.String(320), nullable=False),
            sa.Column("label_name", sa.String(80), nullable=False),
            sa.Column("label_id", sa.String(100), nullable=True),
            sa.Column("active", sa.Boolean(), nullable=False),
            sa.Column("gmail_credentials", sa.Text(), nullable=False),
            sa.Column("sender_credentials", sa.Text(), nullable=True),
            sa.Column("oauth_nonce_hash", sa.String(64), nullable=True),
            sa.Column("oauth_until", sa.DateTime(timezone=True), nullable=True),
            sa.Column("cursor", sa.String(32), nullable=True),
            sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("pending", sa.JSON(), nullable=False),
            sa.Column("read_count", sa.Integer(), nullable=False),
            sa.Column("read_window", sa.DateTime(timezone=True), nullable=True),
            sa.Column("next_poll_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("error_code", sa.String(40), nullable=True),
            sa.Column("lease_token", sa.String(32), nullable=True),
            sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
        )
    if "own_support_tickets" not in tables:
        op.create_table(
            "own_support_tickets",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column(
                "owner_id",
                sa.String(36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "channel_id",
                sa.String(16),
                sa.ForeignKey("own_support_channels.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("binding", sa.String(32), nullable=False),
            sa.Column("google_id", sa.String(40), nullable=False),
            sa.Column("thread_id", sa.String(40), nullable=False),
            sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("payload_encrypted", sa.Text(), nullable=False),
            sa.Column("input_hash", sa.String(64), nullable=False),
            sa.Column("lang", sa.String(2), nullable=False),
            sa.Column("revision", sa.Integer(), nullable=False),
            sa.Column("state", sa.String(24), nullable=False),
            sa.Column("reason", sa.String(40), nullable=True),
            sa.Column("dossier", sa.JSON(), nullable=False),
            sa.Column("draft_encrypted", sa.Text(), nullable=True),
            sa.Column("outbox_encrypted", sa.Text(), nullable=True),
            sa.Column("draft_hash", sa.String(64), nullable=True),
            sa.Column("provider_calls", sa.Integer(), nullable=False),
            sa.Column("provider_started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("authorized_until", sa.DateTime(timezone=True), nullable=True),
            sa.Column("sender_hash", sa.String(64), nullable=True),
            sa.Column("send_intent_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("receipt", sa.JSON(), nullable=False),
            sa.Column("error_code", sa.String(40), nullable=True),
            sa.Column("lease_token", sa.String(32), nullable=True),
            sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
            sa.Column("next_check_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.UniqueConstraint("binding", "google_id", name="uq_own_support_message"),
        )
        op.create_index(
            "ix_own_support_tickets_owner_id", "own_support_tickets", ["owner_id"]
        )


def downgrade():
    op.drop_table("own_support_tickets")
    op.drop_table("own_support_channels")
