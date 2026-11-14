# -*- coding: utf-8 -*-
"""La confidentialité déclare l'usage limité (« Limited Use ») des données Google du SAV.

14/11/2026. Pour valider l'accès Gmail du SAV (portée gmail.readonly), Google exige une
déclaration explicite d'adhésion à sa politique sur les données des utilisateurs, « y compris les
exigences d'usage limité », publiée dans l'application ou sur son site
(developers.google.com/workspace/workspace-api-user-data-developer-policy, section Limited Use).
La page ne l'avait pas (message C007 de Codex). Texte validé par le propriétaire le 14/11.

Sa phrase anglaise est reprise MOT POUR MOT sur les deux pages : on ne sait pas laquelle la fiche
Google du projet désigne, et un relecteur la cherche telle quelle.
"""
from __future__ import annotations

import html
import os
import re
import sys
import tempfile
from datetime import date
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-limited-use-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-limited-use-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402

PHRASE_GOOGLE = ("The use of information received from Google Workspace scopes will adhere to the "
                 "Google User Data Policy, including the Limited Use requirements.")
POLITIQUE = "https://developers.google.com/terms/api-services-user-data-policy"
PAGES = {"/privacy": "fr", "/en/privacy": "en"}


def _page(chemin: str) -> str:
    r = TestClient(m.app).get(chemin, follow_redirects=False)
    assert r.status_code == 200, (chemin, r.status_code)
    return r.text


def _paragraphe(page: str) -> str:
    trouve = re.search(r'<p id="google-limited-use">(.*?)</p>', page, re.S)
    assert trouve, "le paragraphe « Limited Use » est absent"
    return trouve.group(1)


def _texte(fragment: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", fragment))).strip()


@pytest.mark.parametrize("chemin", PAGES, ids=list(PAGES.values()))
def test_la_phrase_de_Google_est_publiee_MOT_POUR_MOT(chemin: str) -> None:
    assert PHRASE_GOOGLE in _texte(_paragraphe(_page(chemin)))


@pytest.mark.parametrize("chemin", PAGES, ids=list(PAGES.values()))
def test_la_declaration_renvoie_a_la_politique_de_Google(chemin: str) -> None:
    assert 'href="%s"' % POLITIQUE in _paragraphe(_page(chemin))


@pytest.mark.parametrize("chemin", PAGES, ids=list(PAGES.values()))
def test_la_declaration_suit_le_paragraphe_du_SAV_dans_la_meme_section(chemin: str) -> None:
    """Elle porte sur les données Gmail du SAV : elle vient juste après leur description, avant
    la section suivante, pas perdue ailleurs dans la page."""
    page = _page(chemin)
    sav, declaration = page.index('id="support-email"'), page.index('id="google-limited-use"')
    assert sav < declaration, "la déclaration doit suivre le paragraphe du SAV"
    assert "<h2" not in page[sav:declaration], "un titre de section les sépare"


def test_les_engagements_sont_dits_en_francais() -> None:
    texte = _texte(_paragraphe(_page("/privacy")))
    for engagement in ("Usage limité des données Google", "que pour faire fonctionner le SAV",
                       "ne les vend pas", "publicité", "entraîner ou améliorer des modèles",
                       "qu'aux prestataires nommés sur cette page"):
        assert engagement in texte, engagement


def test_les_engagements_sont_dits_en_anglais() -> None:
    texte = _texte(_paragraphe(_page("/en/privacy")))
    for engagement in ("Limited Use of Google data", "only to run the support feature",
                       "does not sell this data", "advertising", "train or improve artificial intelligence",
                       "only with the providers named on this page"):
        assert engagement in texte, engagement


def test_la_page_est_datee_de_l_ajout() -> None:
    assert m.legal.UPDATED["privacy"] >= date(2026, 11, 14)
