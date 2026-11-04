"""Private sourced writing operations; upgrading grants no writing authorization."""
import sqlalchemy as sa
from alembic import op

revision='20261104_0025'
down_revision='20261104_0024'
branch_labels=None
depends_on=None


def upgrade():
    if 'own_seo_drafts' in sa.inspect(op.get_bind()).get_table_names():return
    op.create_table('own_seo_drafts',sa.Column('id',sa.String(36),primary_key=True),
        sa.Column('owner_id',sa.String(36),sa.ForeignKey('users.id',ondelete='CASCADE'),nullable=False),
        sa.Column('project_id',sa.String(36),sa.ForeignKey('projects.id',ondelete='CASCADE'),nullable=False),
        sa.Column('brief_id',sa.String(36),sa.ForeignKey('own_seo_briefs.id',ondelete='CASCADE'),nullable=False),
        sa.Column('workspace_token',sa.String(32),nullable=False),sa.Column('authorization_key',sa.String(32),nullable=False),
        sa.Column('input_hash',sa.String(64),nullable=False),sa.Column('bundle',sa.JSON(),nullable=False),
        sa.Column('content',sa.JSON(),nullable=False),sa.Column('content_hash',sa.String(64),nullable=True),
        sa.Column('status',sa.String(24),nullable=False),sa.Column('error_code',sa.String(40),nullable=True),
        sa.Column('provider_calls',sa.Integer(),nullable=False),sa.Column('provider_started_at',sa.DateTime(timezone=True),nullable=True),
        sa.Column('authorized_until',sa.DateTime(timezone=True),nullable=False),sa.Column('lease_token',sa.String(32),nullable=True),
        sa.Column('lease_until',sa.DateTime(timezone=True),nullable=True),sa.Column('reviewed_at',sa.DateTime(timezone=True),nullable=True),
        sa.Column('created_at',sa.DateTime(timezone=True),server_default=sa.func.now(),nullable=False),
        sa.UniqueConstraint('owner_id','authorization_key',name='uq_own_seo_draft_authorization'))
    op.create_index('ix_own_seo_drafts_owner_id','own_seo_drafts',['owner_id'])
    op.create_index('ix_own_seo_drafts_provider_started_at','own_seo_drafts',['provider_started_at'])


def downgrade():
    op.drop_table('own_seo_drafts')
