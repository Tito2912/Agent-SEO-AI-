"""initial schema

This migration is written to be safe on existing databases that were previously
bootstrapped via SQLAlchemy `create_all()` (it only creates missing tables).
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision = "20260329_0001"
down_revision = None
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    try:
        return bool(inspect(conn).has_table(name))
    except Exception:
        try:
            return name in set(inspect(conn).get_table_names())
        except Exception:
            return False


def upgrade() -> None:
    conn = op.get_bind()

    if not _has_table(conn, "users"):
        op.create_table(
            "users",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column("email", sa.String(length=320), nullable=False, unique=True),
            sa.Column("password_hash", sa.String(length=512), nullable=False),
            sa.Column("is_admin", sa.Boolean(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
        op.create_index("ix_users_email", "users", ["email"], unique=False)

    if not _has_table(conn, "projects"):
        op.create_table(
            "projects",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column(
                "owner_user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("slug", sa.String(length=128), nullable=False),
            sa.Column("base_url", sa.String(length=2048), nullable=False),
            sa.Column("site_name", sa.String(length=255), nullable=False),
            sa.Column("settings", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("owner_user_id", "slug", name="uq_projects_owner_slug"),
        )
        op.create_index("ix_projects_owner_user_id", "projects", ["owner_user_id"], unique=False)

    if not _has_table(conn, "user_connections"):
        op.create_table(
            "user_connections",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column(
                "user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("key", sa.String(length=128), nullable=False),
            sa.Column("secret_value", sa.Text(), nullable=False),
            sa.Column("meta", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("user_id", "key", name="uq_user_connections_user_key"),
        )
        op.create_index("ix_user_connections_user_id", "user_connections", ["user_id"], unique=False)
        op.create_index("ix_user_connections_key", "user_connections", ["key"], unique=False)

    if not _has_table(conn, "oauth_identities"):
        op.create_table(
            "oauth_identities",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column(
                "user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("provider", sa.String(length=32), nullable=False),
            sa.Column("provider_user_id", sa.String(length=255), nullable=False),
            sa.Column("email", sa.String(length=320), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("provider", "provider_user_id", name="uq_oauth_identities_provider_user"),
        )
        op.create_index("ix_oauth_identities_user_id", "oauth_identities", ["user_id"], unique=False)
        op.create_index("ix_oauth_identities_provider", "oauth_identities", ["provider"], unique=False)
        op.create_index("ix_oauth_identities_provider_email", "oauth_identities", ["provider", "email"], unique=False)

    if not _has_table(conn, "audit_logs"):
        op.create_table(
            "audit_logs",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column(
                "actor_user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column("actor_email", sa.String(length=320), nullable=True),
            sa.Column("action", sa.String(length=128), nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False),
            sa.Column("target_type", sa.String(length=64), nullable=True),
            sa.Column("target_id", sa.String(length=255), nullable=True),
            sa.Column("ip_address", sa.String(length=64), nullable=True),
            sa.Column("user_agent", sa.String(length=512), nullable=True),
            sa.Column("meta", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
        op.create_index("ix_audit_logs_actor_user_id", "audit_logs", ["actor_user_id"], unique=False)
        op.create_index("ix_audit_logs_actor_email", "audit_logs", ["actor_email"], unique=False)
        op.create_index("ix_audit_logs_action", "audit_logs", ["action"], unique=False)
        op.create_index("ix_audit_logs_status", "audit_logs", ["status"], unique=False)
        op.create_index("ix_audit_logs_actor_created", "audit_logs", ["actor_user_id", "created_at"], unique=False)
        op.create_index("ix_audit_logs_action_created", "audit_logs", ["action", "created_at"], unique=False)

    if not _has_table(conn, "billing_customers"):
        op.create_table(
            "billing_customers",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column(
                "user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
                unique=True,
            ),
            sa.Column("stripe_customer_id", sa.String(length=255), nullable=False, unique=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
        op.create_index("ix_billing_customers_user_id", "billing_customers", ["user_id"], unique=True)
        op.create_index(
            "ix_billing_customers_stripe_customer_id",
            "billing_customers",
            ["stripe_customer_id"],
            unique=True,
        )

    if not _has_table(conn, "billing_subscriptions"):
        op.create_table(
            "billing_subscriptions",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column(
                "user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("stripe_customer_id", sa.String(length=255), nullable=False),
            sa.Column("stripe_subscription_id", sa.String(length=255), nullable=False, unique=True),
            sa.Column("stripe_price_id", sa.String(length=255), nullable=False),
            sa.Column("plan_key", sa.String(length=64), nullable=False),
            sa.Column("status", sa.String(length=64), nullable=False),
            sa.Column("cancel_at_period_end", sa.Boolean(), nullable=False),
            sa.Column("current_period_start", sa.DateTime(timezone=True), nullable=True),
            sa.Column("current_period_end", sa.DateTime(timezone=True), nullable=True),
            sa.Column("trial_end", sa.DateTime(timezone=True), nullable=True),
            sa.Column("stripe_data", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
        op.create_index("ix_billing_subscriptions_user_id", "billing_subscriptions", ["user_id"], unique=False)
        op.create_index("ix_billing_subscriptions_stripe_customer_id", "billing_subscriptions", ["stripe_customer_id"], unique=False)
        op.create_index(
            "ix_billing_subscriptions_stripe_subscription_id",
            "billing_subscriptions",
            ["stripe_subscription_id"],
            unique=True,
        )
        op.create_index("ix_billing_subscriptions_stripe_price_id", "billing_subscriptions", ["stripe_price_id"], unique=False)
        op.create_index("ix_billing_subscriptions_plan_key", "billing_subscriptions", ["plan_key"], unique=False)
        op.create_index("ix_billing_subscriptions_status", "billing_subscriptions", ["status"], unique=False)

    if not _has_table(conn, "usage_events"):
        op.create_table(
            "usage_events",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column(
                "user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("period", sa.String(length=7), nullable=False),
            sa.Column("metric", sa.String(length=64), nullable=False),
            sa.Column("amount", sa.Integer(), nullable=False),
            sa.Column("meta", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
        op.create_index("ix_usage_events_user_id", "usage_events", ["user_id"], unique=False)
        op.create_index("ix_usage_events_period", "usage_events", ["period"], unique=False)
        op.create_index("ix_usage_events_metric", "usage_events", ["metric"], unique=False)
        op.create_index(
            "ix_usage_user_period_metric",
            "usage_events",
            ["user_id", "period", "metric"],
            unique=False,
        )

    if not _has_table(conn, "jobs"):
        op.create_table(
            "jobs",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column("status", sa.String(length=24), nullable=False),
            sa.Column("kind", sa.String(length=32), nullable=False),
            sa.Column(
                "owner_user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("slug", sa.String(length=128), nullable=False),
            sa.Column("created_at", sa.Float(), nullable=False),
            sa.Column("updated_at", sa.Float(), nullable=False),
            sa.Column("started_at", sa.Float(), nullable=True),
            sa.Column("finished_at", sa.Float(), nullable=True),
            sa.Column("pid", sa.Integer(), nullable=True),
            sa.Column("config_path", sa.String(length=2048), nullable=True),
            sa.Column("command", sa.JSON(), nullable=True),
            sa.Column("returncode", sa.Integer(), nullable=True),
            sa.Column("stdout", sa.Text(), nullable=True),
            sa.Column("stderr", sa.Text(), nullable=True),
            sa.Column("progress", sa.JSON(), nullable=True),
            sa.Column("result", sa.JSON(), nullable=True),
            sa.Column("attempts", sa.Integer(), nullable=False),
            sa.Column("max_attempts", sa.Integer(), nullable=False),
            sa.Column("run_after", sa.Float(), nullable=True),
            sa.Column("worker_id", sa.String(length=64), nullable=True),
        )
        op.create_index("ix_jobs_status", "jobs", ["status"], unique=False)
        op.create_index("ix_jobs_kind", "jobs", ["kind"], unique=False)
        op.create_index("ix_jobs_owner_user_id", "jobs", ["owner_user_id"], unique=False)
        op.create_index("ix_jobs_slug", "jobs", ["slug"], unique=False)
        op.create_index("ix_jobs_status_created", "jobs", ["status", "created_at"], unique=False)
        op.create_index("ix_jobs_owner_created", "jobs", ["owner_user_id", "created_at"], unique=False)

    if not _has_table(conn, "password_reset_tokens"):
        op.create_table(
            "password_reset_tokens",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column(
                "user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("token_hash", sa.String(length=64), nullable=False, unique=True),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
        op.create_index("ix_password_reset_tokens_user_id", "password_reset_tokens", ["user_id"], unique=False)
        op.create_index("ix_password_reset_tokens_token_hash", "password_reset_tokens", ["token_hash"], unique=True)
        op.create_index(
            "ix_password_reset_tokens_user_created",
            "password_reset_tokens",
            ["user_id", "created_at"],
            unique=False,
        )

    if not _has_table(conn, "email_verification_tokens"):
        op.create_table(
            "email_verification_tokens",
            sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
            sa.Column(
                "user_id",
                sa.String(length=36),
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("token_hash", sa.String(length=64), nullable=False, unique=True),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
        op.create_index("ix_email_verification_tokens_user_id", "email_verification_tokens", ["user_id"], unique=False)
        op.create_index(
            "ix_email_verification_tokens_token_hash",
            "email_verification_tokens",
            ["token_hash"],
            unique=True,
        )
        op.create_index(
            "ix_email_verification_tokens_user_created",
            "email_verification_tokens",
            ["user_id", "created_at"],
            unique=False,
        )


def downgrade() -> None:
    conn = op.get_bind()

    for name in [
        "email_verification_tokens",
        "password_reset_tokens",
        "jobs",
        "usage_events",
        "billing_subscriptions",
        "billing_customers",
        "audit_logs",
        "oauth_identities",
        "user_connections",
        "projects",
        "users",
    ]:
        if _has_table(conn, name):
            op.drop_table(name)

