"""Private own-site workspace and evidence briefs; no project or grant is created."""
import sqlalchemy as sa
from alembic import op
revision='20261104_0024'
down_revision='20261103_0023'
branch_labels=None
depends_on=None


def upgrade():
    tables=sa.inspect(op.get_bind()).get_table_names()
    if 'own_seo_workspace' not in tables:
        op.create_table('own_seo_workspace',sa.Column('id',sa.String(16),primary_key=True),
            sa.Column('owner_id',sa.String(36),sa.ForeignKey('users.id',ondelete='SET NULL'),nullable=True),
            sa.Column('project_id',sa.String(36),sa.ForeignKey('projects.id',ondelete='SET NULL'),nullable=True),
            sa.Column('token',sa.String(32),nullable=False),sa.Column('paused',sa.Boolean(),nullable=False),
            sa.Column('updated_at',sa.DateTime(timezone=True),server_default=sa.func.now(),nullable=False))
    if 'own_seo_briefs' not in tables:
        op.create_table('own_seo_briefs',sa.Column('id',sa.String(36),primary_key=True),
            sa.Column('owner_id',sa.String(36),sa.ForeignKey('users.id',ondelete='CASCADE'),nullable=False),
            sa.Column('project_id',sa.String(36),sa.ForeignKey('projects.id',ondelete='CASCADE'),nullable=False),
            sa.Column('workspace_token',sa.String(32),nullable=False),sa.Column('candidate_key',sa.String(64),nullable=False),
            sa.Column('lang',sa.String(2),nullable=False),sa.Column('kind',sa.String(24),nullable=False),
            sa.Column('title',sa.String(200),nullable=False),sa.Column('evidence',sa.JSON(),nullable=False),
            sa.Column('status',sa.String(24),nullable=False),sa.Column('reviewed_at',sa.DateTime(timezone=True),nullable=True),
            sa.Column('created_at',sa.DateTime(timezone=True),server_default=sa.func.now(),nullable=False),
            sa.UniqueConstraint('workspace_token','candidate_key',name='uq_own_seo_brief_candidate'))
        op.create_index('ix_own_seo_briefs_project_id','own_seo_briefs',['project_id'])


def downgrade():
    op.drop_table('own_seo_briefs')
    op.drop_table('own_seo_workspace')
