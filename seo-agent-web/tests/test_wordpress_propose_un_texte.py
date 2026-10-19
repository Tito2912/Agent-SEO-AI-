"""Dans l'éditeur WordPress, Noyaru peut rédiger le texte lui-même, comme le correcteur le fait
dans un dépôt.

L'éditeur ne faisait que pré-remplir la valeur actuelle : le client écrivait seul le titre ou la
description, alors que Noyaru sait les écrire. La proposition passe par le rédacteur de longueur
du correcteur (mêmes bornes, même refus d'inventer, même contrôle de langue), arrive comme un
aperçu ordinaire à relire, et compte pour UNE correction IA — seulement si elle tient dans les
bornes du crawler.
"""
from __future__ import annotations

import json

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func, select

from backend import app as m, billing, cms_routes
from backend.models import CmsChange, IssueTask, UsageEvent
from test_cms_integrations import cms as cms, connect, journey as journey, post, row, writes
from test_first_verified_correction import BASELINE, URL

PROPOSITION = "Réparation de vélos à Lyon : entretien, révision et pièces — Atelier"


@pytest.fixture()
def redacteur(cms, monkeypatch):
    """Le rédacteur du correcteur, sans IA : il note ce qu'on lui donne et rend `cms["valeur"]`."""
    appels = []

    def rediger(**kw):
        appels.append(kw)
        return cms["valeur"]

    monkeypatch.setattr(m, "_length_value_for_page", rediger)
    cms["valeur"], cms["appels"] = PROPOSITION, appels
    chemin = cms["runs_dir"] / cms["slug"] / BASELINE / "audit" / "report.json"
    rapport = json.loads(chemin.read_text())
    rapport["pages"][0].update({"h1": ["Atelier de réparation de vélos"], "h2": ["Révision", "Pièces"], "lang": "fr-FR"})
    chemin.write_text(json.dumps(rapport))
    connect(cms)
    return cms


def _proposer(data):
    reponse = post(data, "/wordpress/suggest", task_id=data["task"].id)
    assert reponse.status_code == 303
    return reponse.headers["location"]


def _unites(data):
    with m.DB.session() as db:
        return int(db.scalar(select(func.coalesce(func.sum(UsageEvent.amount), 0)).where(
            UsageEvent.user_id == data["owner"], UsageEvent.metric == "ai_corrections_month")))


def _apercus(data):
    with m.DB.session() as db:
        return list(db.scalars(select(CmsChange).where(CmsChange.project_id == data["project_id"])))


def test_la_proposition_devient_un_apercu_a_relire_et_compte_une_correction(redacteur):
    lieu = _proposer(redacteur)
    assert "/wordpress/changes/" in lieu and "err=" not in lieu
    change = lieu.split("/changes/", 1)[1]
    assert row(change).state == "preview" and row(change).data["suggested"]
    assert row(change).data["after"]["seo_title"] == PROPOSITION
    assert not writes(redacteur)  # rien n'est écrit dans WordPress avant la validation
    assert _unites(redacteur) == 1
    [appel] = redacteur["appels"]
    assert appel["current"] == "" and appel["kind"] == "title" and appel["langue"] == "fr"
    assert "Atelier de réparation de vélos" in appel["contexte"] and "Notre service" in appel["contexte"]
    assert appel["model_override"] == billing.plan_catalog()["free"]["correction"]["model"]
    page = redacteur["client"].get(lieu)
    assert "Texte proposé par Noyaru" in page.text and PROPOSITION in page.text


@pytest.mark.parametrize("valeur", ["Court", "", "x" * 71, "Titre <b>gras</b> pour la page atelier"])
def test_une_proposition_hors_limites_n_est_ni_proposee_ni_decomptee(redacteur, valeur):
    redacteur["valeur"] = valeur
    lieu = _proposer(redacteur)
    assert "/wordpress/edit?" in lieu and "err=" in lieu
    assert not _apercus(redacteur) and _unites(redacteur) == 0


def test_sans_correction_restante_rien_n_est_redige(redacteur):
    with m.DB.session() as db:
        billing.usage_add(db, user_id=redacteur["owner"], metric="ai_corrections_month", amount=2)
    lieu = _proposer(redacteur)
    assert "err=" in lieu and not redacteur["appels"] and not _apercus(redacteur)


def test_un_doublon_n_est_pas_redige(redacteur):
    chemin = redacteur["runs_dir"] / redacteur["slug"] / BASELINE / "audit" / "report.json"
    rapport = json.loads(chemin.read_text())
    rapport["issues"] = {"duplicate_titles": {"count": 1, "examples": [URL]}}
    chemin.write_text(json.dumps(rapport))
    with m.DB.session() as db:
        db.get(IssueTask, redacteur["task"].id).issue_key = "duplicate_titles"
        db.commit()
    editeur = redacteur["client"].get(redacteur["base"] + f'/wordpress/edit?task_id={redacteur["task"].id}')
    assert editeur.status_code == 200 and "Proposer un texte" not in editeur.text
    assert "err=" in _proposer(redacteur) and not redacteur["appels"] and _unites(redacteur) == 0


def test_modifier_reprend_le_texte_propose(redacteur):
    change = _proposer(redacteur).split("/changes/", 1)[1]
    base = redacteur["base"] + f'/wordpress/edit?task_id={redacteur["task"].id}'
    saisie = BeautifulSoup(redacteur["client"].get(base + f"&change={change}").text, "html.parser")
    assert saisie.select_one('input[name="value"]')["value"] == PROPOSITION
    # Sans aperçu désigné, c'est la valeur enregistrée dans WordPress (vide ici).
    assert BeautifulSoup(redacteur["client"].get(base).text, "html.parser").select_one('input[name="value"]')["value"] == ""
    assert cms_routes.issue_family("title_too_long_indexable") == "title_too_long"


def test_une_valeur_ecrite_depuis_rien_garde_la_langue_de_la_page(monkeypatch):
    francais = "Réparation de vélos à Lyon avec un entretien complet et des pièces pour tous les vélos de la ville"
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"value": francais})
    ecrire = dict(current="", kind="description", url=URL, site_name="Atelier")
    assert m._length_value_for_page(**ecrire, langue="de") == ""
    assert m._length_value_for_page(**ecrire) != ""
