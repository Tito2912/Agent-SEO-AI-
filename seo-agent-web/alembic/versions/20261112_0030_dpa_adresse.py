"""Adresse de l'organisation dans les preuves d'acceptation du DPA (annexe I ; 12/11/2026)."""
import sqlalchemy as sa
from alembic import op

revision = '20261112_0030'
down_revision = '20261105_0029'
branch_labels = None
depends_on = None


def _colonnes() -> set[str]:
    inspecteur = sa.inspect(op.get_bind())
    if 'dpa_acceptances' not in inspecteur.get_table_names():
        return set()
    return {c['name'] for c in inspecteur.get_columns('dpa_acceptances')}


def upgrade():
    colonnes = _colonnes()
    # Rejouable : une base creee par create_tables() a deja la colonne.
    if colonnes and 'adresse' not in colonnes:
        op.add_column('dpa_acceptances', sa.Column('adresse', sa.String(300), nullable=True))


def downgrade():
    if 'adresse' in _colonnes():
        with op.batch_alter_table('dpa_acceptances') as table:
            table.drop_column('adresse')
