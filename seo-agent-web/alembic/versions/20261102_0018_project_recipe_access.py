"""Expiring permission for a single demonstration project, independent from billing."""
import sqlalchemy as sa
from alembic import op

revision = '20261102_0018'
down_revision = '20261021_0017'
branch_labels = None
depends_on = None


def upgrade():
    if 'project_recipe_access' in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table('project_recipe_access',
        sa.Column('user_id', sa.String(36), sa.ForeignKey('users.id', ondelete='CASCADE'), primary_key=True),
        sa.Column('grant_id', sa.String(36), nullable=False, unique=True),
        sa.Column('base_url', sa.String(2048), nullable=False),
        sa.Column('reason', sa.String(320), nullable=False),
        sa.Column('granted_by_user_id', sa.String(36), sa.ForeignKey('users.id', ondelete='SET NULL'), nullable=True),
        sa.Column('granted_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('revoked_at', sa.DateTime(timezone=True), nullable=True))


def downgrade():
    op.drop_table('project_recipe_access')
