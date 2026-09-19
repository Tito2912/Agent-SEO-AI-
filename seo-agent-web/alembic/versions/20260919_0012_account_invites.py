"""les invitations en attente : une adresse, un jeton a usage unique, une date limite

Le jeton n'est pas stocke, seule son empreinte salee l'est — meme traitement que la
verification d'email et la reinitialisation de mot de passe. Une fuite de la base ne rend donc
aucune invitation utilisable.

`email` est indexee parce qu'elle est LUE A CHAQUE ACCEPTATION : le lien ne suffit pas, la
personne connectee doit porter l'adresse invitee. Sans cette verification, un lien transfere
ouvrirait le compte d'une agence — et ses depots GitHub — a qui le detient.

Revision ID: 20260919_0012
Revises: 20260919_0011
Create Date: 2026-09-19
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "20260919_0012"
down_revision = "20260919_0011"
branch_labels = None
depends_on = None


def _has_table(conn, table: str) -> bool:
    try:
        return table in inspect(conn).get_table_names()
    except Exception:
        return False


def upgrade() -> None:
    conn = op.get_bind()
    if not _has_table(conn, "account_invites"):
        op.create_table(
            "account_invites",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("owner_user_id", sa.String(length=36),
                      sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
            sa.Column("email", sa.String(length=320), nullable=False),
            sa.Column("token_hash", sa.String(length=64), nullable=False),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("accepted_by_user_id", sa.String(length=36), nullable=True),
            sa.Column("invited_by_user_id", sa.String(length=36), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True),
                      server_default=sa.func.now(), nullable=False),
        )
        # Exactement les trois index que les `index=True` du modele produisent, ni plus ni
        # moins : un quatrieme sur la meme colonne se serait heurte au nom du premier.
        op.create_index("ix_account_invites_owner_user_id", "account_invites", ["owner_user_id"])
        op.create_index("ix_account_invites_email", "account_invites", ["email"])
        op.create_index("ix_account_invites_token_hash", "account_invites",
                        ["token_hash"], unique=True)


def downgrade() -> None:
    conn = op.get_bind()
    if _has_table(conn, "account_invites"):
        op.drop_table("account_invites")
