"""Official public-source knowledge monitoring, independent from customer projects."""
import sqlalchemy as sa
from alembic import op

revision = '20261102_0020'
down_revision = '20261102_0019'
branch_labels = None
depends_on = None


def upgrade():
    tables = sa.inspect(op.get_bind()).get_table_names()
    if 'seo_knowledge_sources' not in tables:
        op.create_table('seo_knowledge_sources',
            sa.Column('key', sa.String(64), primary_key=True),
            sa.Column('latest_hash', sa.String(64), nullable=True),
            sa.Column('approved_hash', sa.String(64), nullable=True),
            sa.Column('guidance_fr', sa.Text(), nullable=False, server_default=''),
            sa.Column('guidance_en', sa.Text(), nullable=False, server_default=''),
            sa.Column('reviewed_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('reviewed_by', sa.String(36), sa.ForeignKey('users.id', ondelete='SET NULL'), nullable=True),
            sa.Column('checked_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('last_attempt_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('next_check_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('error_code', sa.String(32), nullable=True),
            sa.Column('lease_token', sa.String(36), nullable=True),
            sa.Column('lease_until', sa.DateTime(timezone=True), nullable=True),
            sa.Column('paused', sa.Boolean(), nullable=False, server_default=sa.false()))
    if 'seo_knowledge_revisions' not in tables:
        op.create_table('seo_knowledge_revisions',
            sa.Column('source_key', sa.String(64), sa.ForeignKey('seo_knowledge_sources.key', ondelete='CASCADE'), primary_key=True),
            sa.Column('fingerprint', sa.String(64), primary_key=True),
            sa.Column('url', sa.String(512), nullable=False),
            sa.Column('text_chars', sa.Integer(), nullable=False),
            sa.Column('excerpt', sa.Text(), nullable=False),
            sa.Column('first_seen_at', sa.DateTime(timezone=True), nullable=False))
        op.create_index('ix_seo_knowledge_revisions_source_seen', 'seo_knowledge_revisions', ['source_key', 'first_seen_at'])


def downgrade():
    op.drop_table('seo_knowledge_revisions')
    op.drop_table('seo_knowledge_sources')
