"""Preuves d'acceptation du DPA ; elles survivent a la suppression du compte (05/11/2026)."""
import sqlalchemy as sa
from alembic import op

revision = '20261105_0026'
down_revision = '20261104_0025'
branch_labels = None
depends_on = None


def upgrade():
    if 'dpa_acceptances' in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        'dpa_acceptances',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('user_id', sa.String(36), sa.ForeignKey('users.id', ondelete='SET NULL'), nullable=True),
        sa.Column('email', sa.String(320), nullable=False),
        sa.Column('organisation', sa.String(200), nullable=False),
        sa.Column('immatriculation', sa.String(40), nullable=True),
        sa.Column('signataire', sa.String(120), nullable=False),
        sa.Column('fonction', sa.String(120), nullable=False),
        sa.Column('version', sa.String(40), nullable=False),
        sa.Column('langue', sa.String(5), nullable=False),
        sa.Column('texte_sha256', sa.String(64), nullable=False),
        sa.Column('ip_address', sa.String(64), nullable=True),
        sa.Column('user_agent', sa.String(512), nullable=True),
        sa.Column('accepte_le', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('termine_le', sa.DateTime(timezone=True), nullable=True))
    op.create_index('ix_dpa_acceptances_user_id', 'dpa_acceptances', ['user_id'])
    op.create_index('ix_dpa_acceptances_termine_le', 'dpa_acceptances', ['termine_le'])


def downgrade():
    op.drop_table('dpa_acceptances')
