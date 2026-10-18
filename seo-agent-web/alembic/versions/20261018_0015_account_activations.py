"""First observed account activation milestones; historical signups stay unmeasured."""

import sqlalchemy as sa
from alembic import op

revision = '20261018_0015'
down_revision = '20261018_0014'
branch_labels = None
depends_on = None


def upgrade():
    if 'account_activations' in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        'account_activations',
        sa.Column('user_id', sa.String(36), sa.ForeignKey('users.id', ondelete='CASCADE'), primary_key=True),
        sa.Column('signup_observed', sa.Boolean(), nullable=False),
        sa.Column('invited_signup', sa.Boolean(), nullable=False),
        sa.Column('project_added_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('audit_available_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('correction_verified_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('clean_audit_at', sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_table('account_activations')
