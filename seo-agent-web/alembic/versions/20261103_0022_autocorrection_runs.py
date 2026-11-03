"""One-preview mandates and durable execution receipts."""
import sqlalchemy as sa
from alembic import op

revision = '20261103_0022'
down_revision = '20261103_0021'
branch_labels = None
depends_on = None


def upgrade():
    if 'autocorrection_runs' in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table('autocorrection_runs',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('project_id', sa.String(36), sa.ForeignKey('projects.id', ondelete='CASCADE'), nullable=False),
        sa.Column('task_id', sa.String(36), sa.ForeignKey('issue_tasks.id', ondelete='CASCADE'), nullable=False),
        sa.Column('owner_id', sa.String(36), sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
        sa.Column('application_key', sa.String(64), nullable=False),
        sa.Column('decision_key', sa.String(64), nullable=False),
        sa.Column('config_key', sa.String(64), nullable=False),
        sa.Column('policy_version', sa.Integer(), nullable=False),
        sa.Column('consent_version', sa.Integer(), nullable=False),
        sa.Column('lang', sa.String(2), nullable=False),
        sa.Column('state', sa.String(24), nullable=False),
        sa.Column('reason', sa.String(40), nullable=False),
        sa.Column('revoked_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('limits', sa.JSON(), nullable=False),
        sa.Column('reserved_credits', sa.Integer(), nullable=False),
        sa.Column('provider_calls', sa.Integer(), nullable=False),
        sa.Column('input_chars', sa.Integer(), nullable=False),
        sa.Column('output_tokens', sa.Integer(), nullable=False),
        sa.Column('operation_id', sa.String(36), sa.ForeignKey('correction_operations.id', ondelete='SET NULL'), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint('task_id', 'application_key', name='uq_autocorrection_task_application'))
    op.create_index('ix_autocorrection_runs_project_id', 'autocorrection_runs', ['project_id'])
    op.create_index('ix_autocorrection_state_created', 'autocorrection_runs', ['state', 'created_at'])


def downgrade():
    op.drop_table('autocorrection_runs')
