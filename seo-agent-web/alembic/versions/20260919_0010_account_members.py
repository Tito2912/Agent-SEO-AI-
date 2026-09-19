"""qui a le droit de travailler sur les projets d'un autre compte

Le produit n'a pas d'objet « organisation » : le compte qui paie EST un utilisateur, et cette
table dit simplement qui d'autre a le droit d'y travailler. Une agence de trois consultants est
donc un compte proprietaire et deux lignes ici.

CETTE MIGRATION ARRIVE APRES LE CODE QUI LA LIT, et il faut le dire plutot que le cacher. La
table a ete introduite dans le modele puis deployee sans migration : sur Render, `create_all()`
ne tourne pas, seul Alembic le fait, donc `select(AccountMember)` echouait. La lecture est
gardee — une table absente rend a chacun ses propres projets au lieu de remonter l'erreur —, si
bien que personne n'a rien vu : la fonctionnalite etait simplement inerte. C'est exactement le
scenario que le test `test_une_table_ABSENTE_ne_ferme_pas_l_acces_a_ses_propres_projets`
decrivait, arrive pour de vrai.

UNE SEULE ADHESION PAR PERSONNE, impose ici par l'unicite de `member_user_id`. Les slugs de
projet sont uniques PAR PROPRIETAIRE : un membre de deux comptes possedant chacun `mon-site`
rendrait `/projects/mon-site` ambigu. La base refuse la seconde adhesion au lieu de laisser la
resolution deviner.

Revision ID: 20260919_0010
Revises: 20260829_0009
Create Date: 2026-09-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260919_0010"
down_revision = "20260829_0009"
branch_labels = None
depends_on = None


def _has_table(conn, table: str) -> bool:
    try:
        return table in inspect(conn).get_table_names()
    except Exception:
        return False


def upgrade() -> None:
    conn = op.get_bind()

    # Gardee comme toutes celles d'avant : `DB.create_tables()` tourne au demarrage en local,
    # donc un environnement frais peut deja avoir la table et un CREATE nu casserait le deploi.
    if not _has_table(conn, "account_members"):
        op.create_table(
            "account_members",
            sa.Column("id", sa.String(length=36), primary_key=True),
            # Le compte dont les projets sont partages : celui qui paie, et dont les quotas
            # sont debites.
            sa.Column("owner_user_id", sa.String(length=36),
                      sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
            sa.Column("member_user_id", sa.String(length=36),
                      sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
            sa.Column("role", sa.String(length=32), nullable=False, server_default="member"),
            sa.Column("created_at", sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("member_user_id", name="uq_account_member_unique_membership"),
        )
        op.create_index("ix_account_members_owner_user_id", "account_members", ["owner_user_id"])
        op.create_index("ix_account_members_member_user_id", "account_members",
                        ["member_user_id"], unique=True)
        op.create_index("ix_account_member_owner", "account_members", ["owner_user_id"])


def downgrade() -> None:
    conn = op.get_bind()
    if _has_table(conn, "account_members"):
        op.drop_table("account_members")
