"""Changes to the terms or the service announced to every account, with one delivery row each."""
import sqlalchemy as sa
from alembic import op

revision = '20261021_0017'
down_revision = '20261018_0016'
branch_labels = None
depends_on = None


def upgrade():
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if 'legal_notices' not in tables:
        op.create_table('legal_notices',
                        sa.Column('id', sa.String(36), primary_key=True),
                        sa.Column('document', sa.String(32), nullable=False),
                        sa.Column('summary_fr', sa.Text(), nullable=False),
                        sa.Column('summary_en', sa.Text(), nullable=False),
                        sa.Column('effective_on', sa.Date(), nullable=False),
                        sa.Column('new_version_url', sa.String(512), nullable=True),
                        sa.Column('created_by_user_id', sa.String(36),
                                  sa.ForeignKey('users.id', ondelete='SET NULL'), nullable=True),
                        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False))
    if 'legal_notice_deliveries' not in tables:
        op.create_table('legal_notice_deliveries',
                        sa.Column('id', sa.String(36), primary_key=True),
                        sa.Column('notice_id', sa.String(36),
                                  sa.ForeignKey('legal_notices.id', ondelete='CASCADE'), nullable=False),
                        sa.Column('user_id', sa.String(36),
                                  sa.ForeignKey('users.id', ondelete='CASCADE'), nullable=False),
                        sa.Column('email', sa.String(320), nullable=False),
                        sa.Column('lang', sa.String(10), nullable=False),
                        sa.Column('status', sa.String(16), nullable=False),
                        sa.Column('error', sa.String(255), nullable=True),
                        sa.Column('sent_at', sa.DateTime(timezone=True), nullable=True),
                        sa.UniqueConstraint('notice_id', 'user_id', name='uq_legal_notice_delivery'))
        op.create_index('ix_legal_notice_deliveries_notice_id', 'legal_notice_deliveries', ['notice_id'])


def downgrade():
    op.drop_index('ix_legal_notice_deliveries_notice_id', table_name='legal_notice_deliveries')
    op.drop_table('legal_notice_deliveries')
    op.drop_table('legal_notices')
