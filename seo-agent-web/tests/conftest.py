"""Rendre un passage local equivalent a celui de la CI.

Le 09 et le 10/09/2026, la suite locale a affiche SEPT echecs qui n'existaient pas en CI, sur
les tests d'inscription et de lien de verification. Ils m'ont fait douter trois fois de
resultats parfaitement valides, et une fois soupconner un correctif qui n'y etait pour rien.

La cause : le limiteur de debit d'inscription range ses compteurs EN BASE (`rate_limit_buckets`,
via `_rate_limit_retry_after`), pas en memoire. Ils survivent donc d'une execution de pytest a
la suivante, dans la fenetre de dix minutes. La CI part d'une base vierge a chaque fois et ne
voit jamais le probleme ; en local, relancer la suite trois fois suffit a epuiser le quota et
les tests repondent alors « Trop de tentatives. Reessaie dans 9 min. »

Un passage complet reste SOUS le plafond — c'est bien pourquoi la CI est verte. On remet donc
simplement le compteur a zero au debut de la session, ce qui aligne le local sur la CI sans
toucher ni aux tests ni au limiteur lui-meme.

Portee volontairement etroite : uniquement les compteurs `auth_*`, uniquement au demarrage. Les
autres (facturation, correctifs GitHub) ne genent personne et ne nous appartiennent pas.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_DISABLE_TELEGRAM_RECEIVER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")


# Les adresses que la suite fabrique. Motifs volontairement etroits : `@exemple.fr` pour les
# tests d'inscription, `@example.com` prefixe pour ceux du correcteur, `@example.invalid` pour
# les auteurs de commit. Verifie sur la base locale — 1668 comptes, TOUS de ce moule, aucun
# compte reel. Rien d'autre n'est touche.
_TEST_ACCOUNT_PATTERNS = (
    "%@exemple.fr",
    "corrections-%@example.com",
    "c-%@exemple.fr",
    "%@example.invalid",
)


def _ajouter_les_colonnes_absentes(engine) -> list[str]:
    """TROISIEME residu (12/11/2026) : le SCHEMA de la base locale retarde sur le modele.

    `create_tables()` (create_all) cree les tables absentes, jamais une colonne absente d'une
    table existante ; en production, c'est la migration qui l'ajoute. La base SQLite locale,
    creee par create_all lors des passages precedents, n'avait donc pas `dpa_acceptances.adresse`
    (migration 0030) : 26 echecs « no such column » sur la barriere locale, aucun en CI.

    Ajoute seulement les colonnes FACULTATIVES sans valeur par defaut du serveur, comme le ferait
    une migration additive. Rien n'est supprime ni modifie ; une colonne obligatoire absente
    reste absente, et le test qui en depend le dira."""
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import Column, inspect

    from backend.models import Base

    if engine.dialect.name != "sqlite":
        return []
    ajoutees: list[str] = []
    with engine.begin() as connexion:
        inspecteur = inspect(connexion)
        existantes = set(inspecteur.get_table_names())
        operations = Operations(MigrationContext.configure(connexion))
        for table in Base.metadata.sorted_tables:
            if table.name not in existantes:
                continue
            presentes = {c["name"] for c in inspecteur.get_columns(table.name)}
            for colonne in table.columns:
                if colonne.name in presentes or not colonne.nullable or colonne.server_default is not None:
                    continue
                operations.add_column(table.name, Column(colonne.name, colonne.type, nullable=True))
                ajoutees.append("%s.%s" % (table.name, colonne.name))
    return ajoutees


@pytest.fixture(scope="session", autouse=True)
def _reset_accumulated_test_state() -> None:
    """Rendre la base locale equivalente a celle, vierge, de la CI.

    DEUX residus, et le premier masquait le second. Les compteurs du limiteur faisaient echouer
    les tests avec « Trop de tentatives » ; une fois vides, le vrai message est apparu : « Ce
    compte existe deja ». La suite cree des comptes et ne les retire jamais — 1668 accumules sur
    la base locale, dont `autre@exemple.fr` que le test s'attend a creer.

    Mesure apres un passage partant de zero : 9 tentatives sur les 20 autorisees par IP, 1 sur
    10 par adresse. La suite n'est donc pas gourmande, elle est seulement jouee plusieurs fois —
    ce que la CI ne fait jamais.
    """
    try:
        from sqlalchemy import text

        from backend import app as app_module
    except Exception:
        return
    try:
        _ajouter_les_colonnes_absentes(app_module.DB.engine)
    except Exception:
        # Meme regle que ci-dessous : un confort, jamais une condition.
        pass
    try:
        with app_module.DB.session() as db:
            db.execute(text("DELETE FROM rate_limit_buckets WHERE key LIKE 'auth\\_%' ESCAPE '\\'"))
            for pattern in _TEST_ACCOUNT_PATTERNS:
                db.execute(text("DELETE FROM users WHERE email LIKE :p"), {"p": pattern})
            db.commit()
    except Exception:
        # Base absente, table pas encore creee, moteur different : ce nettoyage est un confort,
        # jamais une condition. Il ne doit pas faire echouer une suite qu'il vient aider.
        return
