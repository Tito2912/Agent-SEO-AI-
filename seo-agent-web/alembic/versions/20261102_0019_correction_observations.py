"""Append-only, project-bound correction controls for factual assistant memory."""
import sqlalchemy as sa
from alembic import op

revision = '20261102_0019'
down_revision = '20261102_0018'
branch_labels = None
depends_on = None


def upgrade():
    if 'correction_observations' in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table('correction_observations',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('project_id', sa.String(36), sa.ForeignKey('projects.id', ondelete='CASCADE'), nullable=False),
        sa.Column('task_id', sa.String(36), sa.ForeignKey('issue_tasks.id', ondelete='CASCADE'), nullable=False),
        sa.Column('observation_key', sa.String(64), nullable=False, unique=True),
        sa.Column('attempt_key', sa.String(64), nullable=False),
        sa.Column('issue_key', sa.String(255), nullable=False),
        sa.Column('baseline_ts', sa.String(40), nullable=False),
        sa.Column('control_ts', sa.String(40), nullable=False),
        sa.Column('control_started_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('result', sa.String(24), nullable=False),
        sa.Column('baseline_confirmed', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False))
    op.create_index('ix_correction_observations_project_control', 'correction_observations',
                    ['project_id', 'control_started_at'])


def downgrade():
    op.drop_table('correction_observations')
