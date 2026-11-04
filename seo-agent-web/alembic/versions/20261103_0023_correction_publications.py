"""Explicit publication mandates; existing previews are never activated."""
import sqlalchemy as sa
from alembic import op

revision = '20261103_0023'
down_revision = '20261103_0022'
branch_labels = None
depends_on = None


def upgrade():
    if 'correction_publications' in sa.inspect(op.get_bind()).get_table_names():
        return
    columns = [sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('run_id', sa.String(36), sa.ForeignKey('autocorrection_runs.id', ondelete='CASCADE'), unique=True, nullable=False)]
    for name, table in [('project_id','projects'),('task_id','issue_tasks'),('owner_id','users')]:
        columns.append(sa.Column(name, sa.String(36), sa.ForeignKey(table+'.id', ondelete='CASCADE'), nullable=False))
    for name in ('application_key','config_key','preview_key','analysis_key'):
        columns.append(sa.Column(name, sa.String(64), nullable=False))
    columns.append(sa.Column('applied_key', sa.String(64), nullable=True))
    for name in ('version','pages_budget','reserved_pages','reads','writes'):
        columns.append(sa.Column(name, sa.Integer(), nullable=False))
    for name in ('control_settled','job_issued'):
        columns.append(sa.Column(name, sa.Boolean(), nullable=False))
    for name, size in [('lang',2),('environment',64),('phase',32),('reason',40)]:
        columns.append(sa.Column(name, sa.String(size), nullable=False))
    columns.append(sa.Column('receipt', sa.JSON(), nullable=False))
    for name, table in [('operation_id','correction_operations'),('job_id','jobs'),('observation_id','correction_observations')]:
        columns.append(sa.Column(name, sa.String(36), sa.ForeignKey(table+'.id', ondelete='SET NULL'), nullable=True))
    for name in ('expires_at','next_check_at'):
        columns.append(sa.Column(name, sa.DateTime(timezone=True), nullable=False))
    columns.extend([sa.Column('revoked_at', sa.DateTime(timezone=True), nullable=True),
                    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False)])
    op.create_table('correction_publications', *columns)
    op.create_index('ix_correction_publications_project_id','correction_publications',['project_id'])
    op.create_index('ix_correction_publications_phase_due','correction_publications',['phase','next_check_at'])


def downgrade():
    op.drop_table('correction_publications')
