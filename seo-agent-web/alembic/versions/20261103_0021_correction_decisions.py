"""Record scope and versioned decisions without changing existing observations."""
import sqlalchemy as sa
from alembic import op

revision = '20261103_0021'
down_revision = '20261102_0020'
branch_labels = None
depends_on = None


def upgrade():
    if 'correction_decisions' not in sa.inspect(op.get_bind()).get_table_names():
        op.create_table('correction_decisions',
            sa.Column('observation_id', sa.String(36), sa.ForeignKey('correction_observations.id', ondelete='CASCADE'), primary_key=True),
            sa.Column('project_id', sa.String(36), sa.ForeignKey('projects.id', ondelete='CASCADE'), nullable=False),
            sa.Column('task_id', sa.String(36), sa.ForeignKey('issue_tasks.id', ondelete='CASCADE'), nullable=False),
            sa.Column('scope_key', sa.String(64), nullable=True),
            sa.Column('application_key', sa.String(64), nullable=True),
            sa.Column('application_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('publication_basis', sa.String(24), nullable=True),
            sa.Column('policy_version', sa.Integer(), nullable=False),
            sa.Column('decision_key', sa.String(64), nullable=False),
            sa.Column('code', sa.String(40), nullable=False),
            sa.Column('failed_applications', sa.Integer(), nullable=False),
            sa.Column('history_complete', sa.Boolean(), nullable=False),
            sa.Column('assessed_at', sa.DateTime(timezone=True), nullable=False))
        op.create_index('ix_correction_decisions_project_task','correction_decisions',['project_id','task_id'])


def downgrade():
    op.drop_table('correction_decisions')
