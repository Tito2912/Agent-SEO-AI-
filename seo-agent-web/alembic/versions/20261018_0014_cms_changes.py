"""Journal of approved CMS edits and recovery states."""

import sqlalchemy as sa
from alembic import op

revision = '20261018_0014'
down_revision = '20261003_0013'
branch_labels = None
depends_on = None


def upgrade():
    if 'cms_changes' in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        'cms_changes',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('project_id', sa.String(36), sa.ForeignKey('projects.id', ondelete='CASCADE'), nullable=False),
        sa.Column('task_id', sa.String(36), sa.ForeignKey('issue_tasks.id', ondelete='CASCADE'), nullable=False),
        sa.Column('created_by', sa.String(36), nullable=False),
        sa.Column('state', sa.String(32), nullable=False),
        sa.Column('data', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index('ix_cms_changes_project_created', 'cms_changes', ['project_id', 'created_at'])


def downgrade():
    op.drop_table('cms_changes')
