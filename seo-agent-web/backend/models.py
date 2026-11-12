from __future__ import annotations

import uuid
import time
from datetime import date, datetime
from typing import Any

from sqlalchemy import JSON, Boolean, Date, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TelegramApprovalChannel(Base):
    __tablename__ = 'telegram_approval_channels'
    id: Mapped[str] = mapped_column(String(16), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(36), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    binding: Mapped[str] = mapped_column(String(32), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    bot_id: Mapped[str] = mapped_column(String(24), nullable=False)
    chat_id: Mapped[str] = mapped_column(String(24), nullable=False)
    lang: Mapped[str] = mapped_column(String(2), default='fr', nullable=False)
    # IDs fit in signed 64 bits; decimal strings avoid platform-specific integer limits.
    offset: Mapped[str] = mapped_column(String(24), default='0', nullable=False)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class TelegramApprovalRequest(Base):
    __tablename__ = 'telegram_approval_requests'
    __table_args__ = (UniqueConstraint('binding', 'kind', 'target_id', 'version', name='uq_telegram_approval_version'),)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(36), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    binding: Mapped[str] = mapped_column(String(32), nullable=False)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    target_id: Mapped[str] = mapped_column(String(80), nullable=False)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    pages: Mapped[list] = mapped_column(JSON, nullable=False)
    state: Mapped[str] = mapped_column(String(24), default='pending', nullable=False)
    nonce: Mapped[str] = mapped_column(String(12), nullable=False)
    confirmation: Mapped[str | None] = mapped_column(String(12), nullable=True)
    choice: Mapped[str | None] = mapped_column(String(8), nullable=True)
    seen: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    delivery: Mapped[str] = mapped_column(String(16), default='queued', nullable=False)
    delivery_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    message_id: Mapped[str | None] = mapped_column(String(24), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    result_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(512))
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    timezone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    country: Mapped[str | None] = mapped_column(String(4), nullable=True)
    language: Mapped[str | None] = mapped_column(String(10), nullable=True)

    projects: Mapped[list["Project"]] = relationship(back_populates="owner", cascade="all, delete-orphan")


class AcquisitionCampaign(Base):
    """System-owner campaign labels; no click or visitor records."""
    __tablename__ = "acquisition_campaigns"
    __table_args__ = (UniqueConstraint("source", "medium", "name", name="uq_acquisition_campaign_labels"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    source: Mapped[str] = mapped_column(String(40), nullable=False)
    medium: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class LegalNotice(Base):
    """Un changement des conditions ou du service, annoncé aux comptes (CGU, article 13)."""
    __tablename__ = "legal_notices"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    # terms | privacy | service (backend/legal_notices.DOCUMENTS)
    document: Mapped[str] = mapped_column(String(32), nullable=False)
    summary_fr: Mapped[str] = mapped_column(Text, nullable=False)
    summary_en: Mapped[str] = mapped_column(Text, nullable=False)
    effective_on: Mapped[date] = mapped_column(Date, nullable=False)
    # Adresse de la nouvelle version, si elle est déjà publiée ; sinon l'e-mail donne celle du document.
    new_version_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_by_user_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class LegalNoticeDelivery(Base):
    """L'envoi d'une annonce à un compte : la preuve qu'il a été prévenu. Effacé avec le compte."""
    __tablename__ = "legal_notice_deliveries"
    __table_args__ = (UniqueConstraint("notice_id", "user_id", name="uq_legal_notice_delivery"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    notice_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("legal_notices.id", ondelete="CASCADE"), index=True, nullable=False)
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    # fr | en | fr+en (le compte n'a pas choisi de langue : l'e-mail porte les deux)
    lang: Mapped[str] = mapped_column(String(10), nullable=False)
    # pending | sent | failed
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    error: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AccountActivation(Base):
    """First observed milestones survive project removal; account deletion removes the row."""
    __tablename__ = "account_activations"

    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    signup_observed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    invited_signup: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    project_added_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    audit_available_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    correction_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    clean_audit_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acquisition_campaign_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("acquisition_campaigns.id", ondelete="SET NULL"), nullable=True)
    acquisition_consented_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Project(Base):
    __tablename__ = "projects"
    __table_args__ = (UniqueConstraint("owner_user_id", "slug", name="uq_projects_owner_slug"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    owner_user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True)

    slug: Mapped[str] = mapped_column(String(128))
    base_url: Mapped[str] = mapped_column(String(2048))
    site_name: Mapped[str] = mapped_column(String(255))
    settings: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    owner: Mapped[User] = relationship(back_populates="projects")


class UserConnection(Base):
    __tablename__ = "user_connections"
    __table_args__ = (UniqueConstraint("user_id", "key", name="uq_user_connections_user_key"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )

    key: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    secret_value: Mapped[str] = mapped_column(Text, nullable=False)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class OAuthIdentity(Base):
    __tablename__ = "oauth_identities"
    __table_args__ = (
        UniqueConstraint("provider", "provider_user_id", name="uq_oauth_identities_provider_user"),
        Index("ix_oauth_identities_provider_email", "provider", "email"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )

    provider: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    provider_user_id: Mapped[str] = mapped_column(String(255), nullable=False)
    email: Mapped[str] = mapped_column(String(320), nullable=False, default="")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class AuditLog(Base):
    __tablename__ = "audit_logs"
    __table_args__ = (
        Index("ix_audit_logs_actor_created", "actor_user_id", "created_at"),
        Index("ix_audit_logs_action_created", "action", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    actor_user_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True
    )
    actor_email: Mapped[str | None] = mapped_column(String(320), index=True, nullable=True)

    action: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(32), index=True, nullable=False, default="ok")
    target_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    target_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    ip_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(512), nullable=True)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ProjectRecipeAccess(Base):
    """One expiring, origin-scoped permission to add a demonstration project."""
    __tablename__ = "project_recipe_access"

    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    grant_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    base_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    reason: Mapped[str] = mapped_column(String(320), nullable=False)
    granted_by_user_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    granted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class BillingCustomer(Base):
    __tablename__ = "billing_customers"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), unique=True, index=True, nullable=False
    )
    stripe_customer_id: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class BillingSubscription(Base):
    __tablename__ = "billing_subscriptions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )

    stripe_customer_id: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    stripe_subscription_id: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    stripe_price_id: Mapped[str] = mapped_column(String(255), index=True, nullable=False)

    plan_key: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    cancel_at_period_end: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    current_period_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    current_period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    trial_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    stripe_data: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class UsageEvent(Base):
    __tablename__ = "usage_events"
    __table_args__ = (Index("ix_usage_user_period_metric", "user_id", "period", "metric"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True)

    # YYYY-MM (ex: 2026-03)
    period: Mapped[str] = mapped_column(String(7), index=True, nullable=False)
    metric: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    amount: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class JobRecord(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_status_created", "status", "created_at"),
        Index("ix_jobs_owner_created", "owner_user_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(24), index=True, nullable=False)
    kind: Mapped[str] = mapped_column(String(32), index=True, nullable=False, default="")

    owner_user_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=True
    )
    slug: Mapped[str] = mapped_column(String(128), index=True, nullable=False, default="")

    created_at: Mapped[float] = mapped_column(Float, nullable=False, default=time.time)
    updated_at: Mapped[float] = mapped_column(Float, nullable=False, default=time.time)
    started_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    finished_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)

    config_path: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    command: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    returncode: Mapped[int | None] = mapped_column(Integer, nullable=True)

    stdout: Mapped[str | None] = mapped_column(Text, nullable=True)
    stderr: Mapped[str | None] = mapped_column(Text, nullable=True)
    progress: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    run_after: Mapped[float | None] = mapped_column(Float, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String(64), nullable=True)


class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"
    __table_args__ = (Index("ix_password_reset_tokens_user_created", "user_id", "created_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )

    # sha256 hex digest, salted with SEO_AGENT_SECRET_KEY (pepper) to avoid storing the raw token.
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)

    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EmailVerificationToken(Base):
    __tablename__ = "email_verification_tokens"
    __table_args__ = (Index("ix_email_verification_tokens_user_created", "user_id", "created_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )

    # sha256 hex digest, salted with SEO_AGENT_SECRET_KEY (pepper) to avoid storing the raw token.
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)

    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class BacklinkOpportunity(Base):
    __tablename__ = "backlink_opportunities"
    __table_args__ = (
        UniqueConstraint("project_id", "url", name="uq_backlink_opp_project_url"),
        Index("ix_backlink_opp_project_status", "project_id", "status"),
        Index("ix_backlink_opp_user_created", "user_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), index=True, nullable=False
    )
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )

    # `user_id` dit chez QUEL COMPTE la ligne vit ; `created_by` dit QUI l'a posee. Les deux
    # etaient la meme personne avant les comptes d'equipe, et le second aurait ete perdu sans
    # cette colonne. Nullable : les lignes d'avant ont ete creees par leur `user_id`, et
    # recopier cette valeur ferait passer une deduction pour un fait constate.
    created_by: Mapped[str | None] = mapped_column(String(36), nullable=True)

    source: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str] = mapped_column(String(1024), nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    snippet: Mapped[str | None] = mapped_column(Text, nullable=True)
    opportunity_score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="new")
    reply: Mapped[str | None] = mapped_column(Text, nullable=True)
    target_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    queue_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    posted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reddit_post_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    auto_found: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class TrackedKeyword(Base):
    """A query the customer decided to follow, and the page meant to answer it.

    Search Console only keeps ~16 months and gives no per-project history, so following a query
    over time means storing it. The row is the DECISION (this query matters, this page should
    win it); the measurements live in TrackedKeywordSnapshot, because a decision and an
    observation have different lifetimes — editing the target page must not rewrite the past.
    """

    __tablename__ = "tracked_keywords"
    __table_args__ = (
        # One row per query per project: adding the same keyword twice is the same decision.
        UniqueConstraint("project_id", "query", name="uq_tracked_kw_project_query"),
        Index("ix_tracked_kw_project_status", "project_id", "status"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), index=True, nullable=False
    )
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )

    # `user_id` dit chez QUEL COMPTE la ligne vit ; `created_by` dit QUI l'a posee. Les deux
    # etaient la meme personne avant les comptes d'equipe, et le second aurait ete perdu sans
    # cette colonne. Nullable : les lignes d'avant ont ete creees par leur `user_id`, et
    # recopier cette valeur ferait passer une deduction pour un fait constate.
    created_by: Mapped[str | None] = mapped_column(String(36), nullable=True)

    query: Mapped[str] = mapped_column(String(512), nullable=False)
    target_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    # Where the keyword came from: "gsc_opportunity" when the product proposed it, "manual" when
    # the customer typed it. Worth keeping — a suggestion that gets tracked is the product
    # working, and that is measurable.
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="tracked")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class TrackedKeywordSnapshot(Base):
    """One measurement of one tracked keyword, on one day.

    Separate from the keyword itself so history survives every edit to the decision, and so a
    re-measure on a day already recorded overwrites rather than accumulates — the unique
    constraint is what makes a refresh idempotent instead of doubling the curve.
    """

    __tablename__ = "tracked_keyword_snapshots"
    __table_args__ = (
        UniqueConstraint("keyword_id", "captured_on", name="uq_tracked_kw_snapshot_day"),
        Index("ix_tracked_kw_snapshot_kw_day", "keyword_id", "captured_on"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    keyword_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("tracked_keywords.id", ondelete="CASCADE"), index=True, nullable=False
    )

    captured_on: Mapped[date] = mapped_column(Date, nullable=False)
    clicks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    impressions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Stored as they come from Search Console rather than recomputed: position is an average
    # over the window and CTR is clicks/impressions for that window, so deriving either later
    # from the other two would quietly invent a number.
    ctr: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    position: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    page: Mapped[str | None] = mapped_column(String(2048), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CompetitorSite(Base):
    """A rival domain the customer wants their pages compared against.

    The crawled pages are stored HERE, as a trimmed list, rather than as a report on disk: a
    competitor crawl produces one thing the product needs — what each page is about — and the
    worker has no disk (see the S3 note in render.yaml). A hundred pages of {url, title, h1}
    is a few kilobytes, so the comparison can be recomputed on every page load from the
    customer's own latest crawl without fetching anything.

    Not a Project: a rival is not a site we audit. Making it one would put it in the customer's
    site quota, run PageSpeed on it, score anomalies nobody will fix, and offer to open pull
    requests against a repository that is not theirs.
    """

    __tablename__ = "competitor_sites"
    __table_args__ = (
        # One row per rival per project. Adding the same domain twice is the same decision, and
        # a second crawl of it would spend worker time to produce the same answer.
        UniqueConstraint("project_id", "domain", name="uq_competitor_project_domain"),
        Index("ix_competitor_project_status", "project_id", "status"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), index=True, nullable=False
    )
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )

    # `user_id` dit chez QUEL COMPTE la ligne vit ; `created_by` dit QUI l'a posee. Les deux
    # etaient la meme personne avant les comptes d'equipe, et le second aurait ete perdu sans
    # cette colonne. Nullable : les lignes d'avant ont ete creees par leur `user_id`, et
    # recopier cette valeur ferait passer une deduction pour un fait constate.
    created_by: Mapped[str | None] = mapped_column(String(36), nullable=True)

    domain: Mapped[str] = mapped_column(String(255), nullable=False)
    base_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    # "new" until a crawl has run, then "crawling" / "ready" / "failed". The UI shows all four
    # differently on purpose: "never analysed" and "analysed, nothing found" are different
    # answers, the same distinction the keywords page makes.
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="new")
    last_job_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    last_crawled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pages_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # [{url, title, h1}] — everything `competitors.page_terms` reads, and nothing else.
    pages: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class IssueTask(Base):
    __tablename__ = "issue_tasks"
    __table_args__ = (
        UniqueConstraint("project_id", "issue_key", "url", name="uq_issue_task_project_issue_url"),
        Index("ix_issue_tasks_project_status", "project_id", "status"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("projects.id", ondelete="CASCADE"), index=True, nullable=False
    )
    user_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True
    )

    # `user_id` dit chez QUEL COMPTE la ligne vit ; `created_by` dit QUI l'a posee. Les deux
    # etaient la meme personne avant les comptes d'equipe, et le second aurait ete perdu sans
    # cette colonne. Nullable : les lignes d'avant ont ete creees par leur `user_id`, et
    # recopier cette valeur ferait passer une deduction pour un fait constate.
    created_by: Mapped[str | None] = mapped_column(String(36), nullable=True)

    issue_key: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    issue_label: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    crawl_ts: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    severity: Mapped[str] = mapped_column(String(32), nullable=False, default="notice")

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="todo", index=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class CmsChange(Base):
    __tablename__ = "cms_changes"
    __table_args__ = (Index("ix_cms_changes_project_created", "project_id", "created_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    task_id: Mapped[str] = mapped_column(String(36), ForeignKey("issue_tasks.id", ondelete="CASCADE"), nullable=False)
    created_by: Mapped[str] = mapped_column(String(36), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="preview")
    data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CorrectionObservation(Base):
    """Immutable control results; never client notes, prompts, or inferred ranking gains."""
    __tablename__ = "correction_observations"
    __table_args__ = (Index("ix_correction_observations_project_control", "project_id", "control_started_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    task_id: Mapped[str] = mapped_column(String(36), ForeignKey("issue_tasks.id", ondelete="CASCADE"), nullable=False)
    observation_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    attempt_key: Mapped[str] = mapped_column(String(64), nullable=False)
    issue_key: Mapped[str] = mapped_column(String(255), nullable=False)
    baseline_ts: Mapped[str] = mapped_column(String(40), nullable=False)
    control_ts: Mapped[str] = mapped_column(String(40), nullable=False)
    control_started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    result: Mapped[str] = mapped_column(String(24), nullable=False)
    baseline_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CorrectionDecision(Base):
    """Current decision per trusted control; revisions are recorded in the audit log."""
    __tablename__ = "correction_decisions"
    __table_args__ = (Index("ix_correction_decisions_project_task", "project_id", "task_id"),)

    observation_id: Mapped[str] = mapped_column(String(36), ForeignKey("correction_observations.id", ondelete="CASCADE"), primary_key=True)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    task_id: Mapped[str] = mapped_column(String(36), ForeignKey("issue_tasks.id", ondelete="CASCADE"), nullable=False)
    scope_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    application_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    application_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    publication_basis: Mapped[str | None] = mapped_column(String(24), nullable=True)
    policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
    decision_key: Mapped[str] = mapped_column(String(64), nullable=False)
    code: Mapped[str] = mapped_column(String(40), nullable=False)
    failed_applications: Mapped[int] = mapped_column(Integer, nullable=False)
    history_complete: Mapped[bool] = mapped_column(Boolean, nullable=False)
    assessed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CorrectionOperation(Base):
    __tablename__ = "correction_operations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    payer_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), index=True)
    request_key: Mapped[str | None] = mapped_column(String(64), unique=True, nullable=True)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="running", index=True)
    charged: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class AutocorrectionRun(Base):
    """A one-preview owner mandate, durable queue item and bounded execution receipt."""
    __tablename__ = "autocorrection_runs"
    __table_args__ = (
        UniqueConstraint("task_id", "application_key", name="uq_autocorrection_task_application"),
        Index("ix_autocorrection_state_created", "state", "created_at"),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), index=True)
    task_id: Mapped[str] = mapped_column(String(36), ForeignKey("issue_tasks.id", ondelete="CASCADE"))
    owner_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id", ondelete="CASCADE"))
    application_key: Mapped[str] = mapped_column(String(64), nullable=False)
    decision_key: Mapped[str] = mapped_column(String(64), nullable=False)
    config_key: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_version: Mapped[int] = mapped_column(Integer, nullable=False)
    consent_version: Mapped[int] = mapped_column(Integer, nullable=False)
    lang: Mapped[str] = mapped_column(String(2), nullable=False)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default="queued")
    reason: Mapped[str] = mapped_column(String(40), nullable=False, default="authorized")
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    limits: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    reserved_credits: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    provider_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    input_chars: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    operation_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("correction_operations.id", ondelete="SET NULL"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CorrectionPublication(Base):
    """Owner approval of one exact preview, one merge and one bounded control."""
    __tablename__ = "correction_publications"
    __table_args__ = (Index("ix_correction_publications_phase_due", "phase", "next_check_at"),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id: Mapped[str] = mapped_column(String(36), ForeignKey("autocorrection_runs.id", ondelete="CASCADE"), unique=True, nullable=False)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey("projects.id", ondelete="CASCADE"), index=True)
    task_id: Mapped[str] = mapped_column(String(36), ForeignKey("issue_tasks.id", ondelete="CASCADE"))
    owner_id: Mapped[str] = mapped_column(String(36), ForeignKey("users.id", ondelete="CASCADE"))
    application_key: Mapped[str] = mapped_column(String(64), nullable=False)
    applied_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    config_key: Mapped[str] = mapped_column(String(64), nullable=False)
    preview_key: Mapped[str] = mapped_column(String(64), nullable=False)
    analysis_key: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    lang: Mapped[str] = mapped_column(String(2), nullable=False)
    environment: Mapped[str] = mapped_column(String(64), nullable=False)
    pages_budget: Mapped[int] = mapped_column(Integer, nullable=False)
    reserved_pages: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    control_settled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    job_issued: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reads: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    writes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    phase: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str] = mapped_column(String(40), nullable=False)
    receipt: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    operation_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("correction_operations.id", ondelete="SET NULL"), nullable=True)
    job_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True)
    observation_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("correction_observations.id", ondelete="SET NULL"), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    next_check_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class AccountMember(Base):
    """Un utilisateur qui travaille sur les projets d'un AUTRE compte.

    Le produit n'a pas d'objet « organisation » : le compte qui paie EST un utilisateur, et cette
    table dit simplement qui d'autre a le droit d'y travailler. Une agence de trois consultants
    est donc un compte proprietaire et deux lignes ici. Le proprietaire garde pour lui la
    facturation et la gestion des membres ; tout le reste est commun.

    UNE SEULE ADHESION PAR PERSONNE, et ce n'est pas une simplification gratuite : les slugs de
    projet sont uniques PAR PROPRIETAIRE (`uq_projects_owner_slug`), pas globalement. Un membre
    de deux comptes qui possedent chacun un projet `mon-site` rendrait `/projects/mon-site`
    ambigu, et la resolution devrait DEVINER lequel montrer. La contrainte d'unicite refuse la
    seconde adhesion avec un message clair, au lieu de laisser une ambiguite s'installer en base.
    Le jour ou plusieurs adherences seront necessaires, il faudra d'abord rendre les slugs
    uniques globalement — c'est cette migration-la le vrai prix, pas la table.

    `role` n'a qu'une valeur aujourd'hui. Elle existe quand meme pour qu'ajouter un lecteur plus
    tard ne demande pas de migration.
    """

    __tablename__ = "account_members"
    __table_args__ = (
        UniqueConstraint("member_user_id", name="uq_account_member_unique_membership"),
        Index("ix_account_member_owner", "owner_user_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    # Le compte dont les projets sont partages : celui qui paie, et dont les quotas sont debites.
    owner_user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )
    member_user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), unique=True, index=True, nullable=False
    )
    role: Mapped[str] = mapped_column(String(32), nullable=False, default="member")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class RateLimitBucket(Base):
    __tablename__ = "rate_limit_buckets"

    # key = "{bucket}:{subject}" e.g. "auth_login_ip:1.2.3.4"
    key: Mapped[str] = mapped_column(String(255), primary_key=True, nullable=False)
    # JSON array of Unix timestamps (float) for hits within the current window
    hits_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class AccountInvite(Base):
    """Une invitation en attente : une adresse, un jeton a usage unique, une date limite.

    LIEE A UNE ADRESSE, et ce n'est pas decoratif. Un lien d'invitation circule — il est
    transfere, colle dans une conversation, retrouve dans une boite partagee. S'il suffisait de
    le detenir, n'importe qui entrerait dans le compte d'une agence avec acces a ses depots
    GitHub. L'acceptation verifie donc que la personne connectee porte bien l'adresse invitee.

    Le jeton n'est pas stocke : seule son empreinte l'est, salee avec `SEO_AGENT_SECRET_KEY`,
    comme pour la verification d'email et la reinitialisation de mot de passe. Une fuite de la
    base ne rend donc aucune invitation utilisable.

    `used_at` et `expires_at` sont separes a dessein. Une invitation acceptee et une invitation
    perimee ne se racontent pas pareil a l'ecran, et le proprietaire doit pouvoir distinguer
    « il n'a jamais repondu » de « il est entre ».
    """

    # Pas de `__table_args__` : les trois index viennent des `index=True` ci-dessous. En
    # declarer un second sur `email` produisait le MEME nom et faisait echouer la creation de
    # la table — en local comme en production.
    __tablename__ = "account_invites"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    owner_user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), index=True, nullable=False
    )
    # Normalisee en minuscules a l'ecriture : l'adresse sert de comparaison, pas d'affichage.
    email: Mapped[str] = mapped_column(String(320), index=True, nullable=False)

    # sha256 hexadecimal, sale avec SEO_AGENT_SECRET_KEY — le jeton brut n'est jamais stocke.
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)

    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Qui a accepte. Peut differer de rien du tout si l'invitation n'a jamais ete utilisee.
    accepted_by_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    # Qui a invite : le proprietaire aujourd'hui, mais la colonne existe pour le jour ou un
    # membre pourra inviter a son tour, sans redemander une migration.
    invited_by_user_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class SeoKnowledgeSource(Base):
    """Global public-document watch; no customer data or editable source URLs."""
    __tablename__ = "seo_knowledge_sources"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    latest_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    approved_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    guidance_fr: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    guidance_en: Mapped[str] = mapped_column(Text, nullable=False, default="", server_default="")
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reviewed_by: Mapped[str | None] = mapped_column(String(36), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_check_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")


class SeoKnowledgeRevision(Base):
    """Bounded metadata/excerpt history; original documents remain on publisher sites."""
    __tablename__ = "seo_knowledge_revisions"
    __table_args__ = (Index("ix_seo_knowledge_revisions_source_seen", "source_key", "first_seen_at"),)

    source_key: Mapped[str] = mapped_column(String(64), ForeignKey("seo_knowledge_sources.key", ondelete="CASCADE"), primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(64), primary_key=True)
    url: Mapped[str] = mapped_column(String(512), nullable=False)
    text_chars: Mapped[int] = mapped_column(Integer, nullable=False)
    excerpt: Mapped[str] = mapped_column(Text, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class OwnSeoWorkspace(Base):
    """A single explicit binding to the system owner's Noyaru project."""
    __tablename__ = 'own_seo_workspace'
    id: Mapped[str] = mapped_column(String(16), primary_key=True)
    owner_id: Mapped[str | None] = mapped_column(String(36), ForeignKey('users.id', ondelete='SET NULL'), nullable=True)
    project_id: Mapped[str | None] = mapped_column(String(36), ForeignKey('projects.id', ondelete='SET NULL'), nullable=True)
    token: Mapped[str] = mapped_column(String(32), nullable=False)
    paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class OwnSeoBrief(Base):
    """Evidence for an editorial brief; never a publication or an AI mandate."""
    __tablename__ = 'own_seo_briefs'
    __table_args__ = (UniqueConstraint('workspace_token','candidate_key',name='uq_own_seo_brief_candidate'),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    owner_id: Mapped[str] = mapped_column(String(36), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey('projects.id', ondelete='CASCADE'), index=True, nullable=False)
    workspace_token: Mapped[str] = mapped_column(String(32), nullable=False)
    candidate_key: Mapped[str] = mapped_column(String(64), nullable=False)
    lang: Mapped[str] = mapped_column(String(2), nullable=False)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    evidence: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default='prepared')
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class OwnSeoDraft(Base):
    """One explicit writing authorization, one transport attempt, no publication rights."""
    __tablename__ = 'own_seo_drafts'
    __table_args__ = (UniqueConstraint('owner_id','authorization_key',name='uq_own_seo_draft_authorization'),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    owner_id: Mapped[str] = mapped_column(String(36), ForeignKey('users.id',ondelete='CASCADE'), index=True, nullable=False)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey('projects.id',ondelete='CASCADE'), nullable=False)
    brief_id: Mapped[str] = mapped_column(String(36), ForeignKey('own_seo_briefs.id',ondelete='CASCADE'), nullable=False)
    workspace_token: Mapped[str] = mapped_column(String(32), nullable=False)
    authorization_key: Mapped[str] = mapped_column(String(32), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    bundle: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    content: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default='queued')
    error_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    provider_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    provider_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    authorized_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class OwnBlogPublication(Base):
    """Two separate owner decisions, fixed own blog only; no AI or client authority."""
    __tablename__ = 'own_blog_publications'
    __table_args__ = (UniqueConstraint('owner_id', 'authorization_key', name='uq_own_blog_authorization'),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    owner_id: Mapped[str] = mapped_column(String(36), ForeignKey('users.id', ondelete='CASCADE'), index=True, nullable=False)
    project_id: Mapped[str] = mapped_column(String(36), ForeignKey('projects.id', ondelete='CASCADE'), nullable=False)
    fr_id: Mapped[str] = mapped_column(String(36), ForeignKey('own_seo_drafts.id', ondelete='CASCADE'), nullable=False)
    en_id: Mapped[str | None] = mapped_column(String(36), ForeignKey('own_seo_drafts.id', ondelete='CASCADE'), nullable=True)
    workspace_token: Mapped[str] = mapped_column(String(32), nullable=False)
    authorization_key: Mapped[str] = mapped_column(String(32), nullable=False)
    connection_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    slug: Mapped[str] = mapped_column(String(80), nullable=False)
    proposal: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    receipt: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    intents: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default='proposal')
    generation: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    reads: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    writes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    next_check_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class OwnSupportChannel(Base):
    """An explicit owner-only Gmail support binding, independent of client Google grants."""
    __tablename__ = 'own_support_channels'
    id: Mapped[str] = mapped_column(String(16), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(36), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    binding: Mapped[str] = mapped_column(String(32), nullable=False)
    mailbox: Mapped[str] = mapped_column(String(320), nullable=False)
    label_name: Mapped[str] = mapped_column(String(80), nullable=False)
    label_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    gmail_credentials: Mapped[str] = mapped_column(Text, nullable=False)
    sender_credentials: Mapped[str | None] = mapped_column(Text, nullable=True)
    oauth_nonce_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    oauth_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cursor: Mapped[str | None] = mapped_column(String(32), nullable=True)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pending: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    read_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    read_window: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class OwnSupportTicket(Base):
    """Reduced encrypted request, versioned reply and a one-attempt outbox."""
    __tablename__ = 'own_support_tickets'
    __table_args__ = (UniqueConstraint('binding', 'google_id', name='uq_own_support_message'),)
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    owner_id: Mapped[str] = mapped_column(String(36), ForeignKey('users.id', ondelete='CASCADE'), index=True, nullable=False)
    channel_id: Mapped[str] = mapped_column(String(16), ForeignKey('own_support_channels.id', ondelete='CASCADE'), nullable=False)
    binding: Mapped[str] = mapped_column(String(32), nullable=False)
    google_id: Mapped[str] = mapped_column(String(40), nullable=False)
    thread_id: Mapped[str] = mapped_column(String(40), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    lang: Mapped[str] = mapped_column(String(2), nullable=False, default='fr')
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    state: Mapped[str] = mapped_column(String(24), nullable=False, default='received')
    reason: Mapped[str | None] = mapped_column(String(40), nullable=True)
    dossier: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    draft_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    outbox_encrypted: Mapped[str | None] = mapped_column(Text, nullable=True)
    draft_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    provider_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    authorized_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sender_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    send_intent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    receipt: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    error_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_check_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DpaAcceptance(Base):
    """La preuve d'une acceptation de l'accord de traitement des donnees (DPA, clause B1).

    Gardee pendant l'accord puis cinq ans : elle SURVIT a la suppression du compte (le lien
    devient nul, `termine_le` date la fin de l'accord), ce que le journal d'audit ne permet pas,
    puisqu'il retire l'identite des evenements d'un compte supprime. 05/11/2026.
    """
    __tablename__ = "dpa_acceptances"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    organisation: Mapped[str] = mapped_column(String(200), nullable=False)
    immatriculation: Mapped[str | None] = mapped_column(String(40), nullable=True)
    # L'adresse de l'organisation (annexe I du DPA, demandee par les clauses de la Commission ;
    # 12/11/2026). Obligatoire dans le formulaire ; nulle seulement pour une preuve d'avant.
    adresse: Mapped[str | None] = mapped_column(String(300), nullable=True)
    signataire: Mapped[str] = mapped_column(String(120), nullable=False)
    fonction: Mapped[str] = mapped_column(String(120), nullable=False)
    version: Mapped[str] = mapped_column(String(40), nullable=False)
    langue: Mapped[str] = mapped_column(String(5), nullable=False)
    texte_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    ip_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(512), nullable=True)
    accepte_le: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    termine_le: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
