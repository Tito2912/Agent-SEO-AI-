"""garder qui a cree une ligne, maintenant que `user_id` designe le COMPTE

Avec les comptes d'equipe, `user_id` change de sens sur ces quatre tables : il ne dit plus « la
personne qui a fait ca » mais « le compte chez qui ca a ete fait ». Le changement est voulu —
sinon une agence ne retrouverait pas le travail de ses propres consultants, et tout
disparaitrait de sa vue le jour ou l'un d'eux quitte l'equipe.

Mais il detruit une information qui existait : QUI a agi. `created_by` la garde, comme
`started_by` le fait deja dans le resultat d'un job de crawl.

NULLABLE, et pas par facilite : les lignes ecrites avant cette migration ont ete creees par
leur `user_id`, qui etait alors la personne elle-meme. Remplir la colonne avec `user_id` serait
exact aujourd'hui et trompeur demain, parce qu'on ne saurait plus distinguer une valeur
constatee d'une valeur recopiee. Une colonne vide dit « on ne sait pas », ce qui est la verite.

Revision ID: 20260919_0011
Revises: 20260919_0010
Create Date: 2026-09-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260919_0011"
down_revision = "20260919_0010"
branch_labels = None
depends_on = None

TABLES = ("issue_tasks", "tracked_keywords", "competitor_sites", "backlink_opportunities")


def _colonnes(conn, table: str) -> set[str]:
    try:
        return {c["name"] for c in inspect(conn).get_columns(table)}
    except Exception:
        return set()


def _existe(conn, table: str) -> bool:
    try:
        return table in inspect(conn).get_table_names()
    except Exception:
        return False


def upgrade() -> None:
    conn = op.get_bind()
    for table in TABLES:
        # Gardee des deux cotes : la table peut manquer sur un environnement frais ou
        # `create_all()` vient de tout poser avec la colonne deja presente.
        if not _existe(conn, table) or "created_by" in _colonnes(conn, table):
            continue
        op.add_column(table, sa.Column("created_by", sa.String(length=36), nullable=True))


def downgrade() -> None:
    conn = op.get_bind()
    for table in TABLES:
        if _existe(conn, table) and "created_by" in _colonnes(conn, table):
            op.drop_column(table, "created_by")
