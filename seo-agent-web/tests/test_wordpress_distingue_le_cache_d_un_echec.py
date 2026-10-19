"""Un contrôle WordPress « encore présent » dit si la page servie est simplement en retard.

La plupart des sites WordPress ont un cache de pages. Le crawl de contrôle pouvait lire
l'ANCIENNE page alors que WordPress avait confirmé la nouvelle valeur, et l'écran annonçait un
échec en invitant à réécrire un texte juste. Or les deux sources sont là : la valeur confirmée
par WordPress, et celle que le crawl a lue sur la page en ligne.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from bs4 import BeautifulSoup

from backend import app as m
from test_cms_integrations import action, change_page, cms as cms, connect, journey as journey, preview
from test_first_verified_correction import URL, _verify

ANCIEN = "Service — Atelier"  # le titre rendu avant la correction, lu dans WordPress
NOUVEAU = "Service de réparation de vélos à Lyon — Atelier"


def _controle_encore_present(data, titre_lu):
    connect(data)
    change = preview(data, value=NOUVEAU)
    action(data, change, "apply")
    ts = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y%m%d-%H%M%S")
    _verify(data, issues={"missing_title": {"count": 1, "examples": [URL]}}, ts=ts)
    chemin = data["runs_dir"] / data["slug"] / ts / "audit" / "report.json"
    rapport = json.loads(chemin.read_text())
    rapport["pages"][0]["title"] = titre_lu
    chemin.write_text(json.dumps(rapport))
    return change


def _bouton_principal(page):
    return BeautifulSoup(page.text, "html.parser").select_one("#verification .btn.primary").get_text(strip=True)


@pytest.mark.parametrize("titre_lu", [ANCIEN, "service &mdash;  ATELIER "])
def test_l_ancienne_valeur_encore_servie_designe_le_cache(cms, titre_lu):
    page = change_page(cms, _controle_encore_present(cms, titre_lu))
    assert "Votre site affiche encore l’ancienne version" in page.text and "cache" in page.text
    assert "Le problème est encore présent" not in page.text
    assert "Valeur lue sur la page par l’analyse" in page.text
    assert "Revoir mon texte" not in page.text and _bouton_principal(page) == "Relancer la vérification"


def test_la_nouvelle_valeur_en_ligne_qui_ne_suffit_pas_le_dit(cms):
    page = change_page(cms, _controle_encore_present(cms, NOUVEAU))
    assert "La nouvelle valeur est bien en ligne" in page.text and "Le problème est encore présent" in page.text
    assert _bouton_principal(page) == "Revoir mon texte"


@pytest.mark.parametrize("titre_lu", ["Tout autre chose", None])
def test_une_lecture_ambigue_garde_le_message_prudent(cms, titre_lu):
    page = change_page(cms, _controle_encore_present(cms, titre_lu))
    assert "Le problème est encore présent" in page.text
    assert "Votre site affiche encore l’ancienne version" not in page.text
    assert "La nouvelle valeur est bien en ligne" not in page.text


def test_le_suivi_automatique_ne_relit_pas_le_rapport(cms, monkeypatch):
    change = _controle_encore_present(cms, ANCIEN)
    monkeypatch.setattr(m.dash, "load_report_json", lambda *a, **k: pytest.fail("rapport relu par le suivi"))
    reponse = cms["client"].get(cms["base"] + "/wordpress/changes/" + change + "/status")
    assert reponse.json() == {"state": "still_present", "poll": False}
