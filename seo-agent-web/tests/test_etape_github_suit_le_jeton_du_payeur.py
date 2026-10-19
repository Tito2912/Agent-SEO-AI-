"""L'étape « Connecter GitHub » de l'accueil suit le jeton qui poussera vraiment.

Les routes qui poussent lisent le jeton du PAYEUR, le propriétaire du projet (décision des
comptes d'équipe du 19/09/2026). L'étape lisait la personne connectée, et cochait aussi un jeton
de plateforme, qui n'est pas une connexion du client.
"""
from __future__ import annotations

from backend import app as m
from test_first_verified_correction import _state, journey as journey


def _etape(data):
    return next(step for step in _state(data)["steps"] if step["key"] == "github")


def _jetons(monkeypatch, sources):
    monkeypatch.setattr(m, "_build_github_connection_state",
                        lambda *, user_id, db: {"source": sources.get(user_id, "none")})


def test_le_membre_voit_le_jeton_du_proprietaire(journey, monkeypatch):
    _jetons(monkeypatch, {journey["owner"]: "user"})
    assert _etape(journey)["done"]


def test_le_jeton_personnel_du_membre_ne_pousse_pas_sur_ce_projet(journey, monkeypatch):
    _jetons(monkeypatch, {journey["member"]: "user"})
    assert not _etape(journey)["done"]


def test_un_jeton_de_plateforme_n_est_pas_une_connexion(journey, monkeypatch):
    _jetons(monkeypatch, {journey["owner"]: "system", journey["member"]: "system"})
    assert not _etape(journey)["done"]
