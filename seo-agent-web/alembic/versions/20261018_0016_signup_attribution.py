"""Registered campaign links and optional signup attribution."""
import sqlalchemy as sa
from alembic import op

revision = '20261018_0016'
down_revision = '20261018_0015'
branch_labels = None
depends_on = None


def upgrade():
    inspector = sa.inspect(op.get_bind())
    if 'acquisition_campaigns' not in inspector.get_table_names():
        op.create_table('acquisition_campaigns',
                        sa.Column('id', sa.String(36), primary_key=True),
                        sa.Column('source', sa.String(40), nullable=False),
                        sa.Column('medium', sa.String(32), nullable=False),
                        sa.Column('name', sa.String(64), nullable=False),
                        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
                        sa.UniqueConstraint('source', 'medium', 'name', name='uq_acquisition_campaign_labels'))
    columns = {column['name'] for column in inspector.get_columns('account_activations')}
    with op.batch_alter_table('account_activations') as batch:
        if 'acquisition_campaign_id' not in columns:
            batch.add_column(sa.Column('acquisition_campaign_id', sa.String(36), nullable=True))
            batch.create_foreign_key('fk_activation_campaign', 'acquisition_campaigns', ['acquisition_campaign_id'], ['id'], ondelete='SET NULL')
        if 'acquisition_consented_at' not in columns:
            batch.add_column(sa.Column('acquisition_consented_at', sa.DateTime(timezone=True), nullable=True))


def downgrade():
    with op.batch_alter_table('account_activations') as batch:
        batch.drop_column('acquisition_campaign_id')
        batch.drop_column('acquisition_consented_at')
    op.drop_table('acquisition_campaigns')
