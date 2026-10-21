"""Les pages juridiques existent en français et en anglais, avec les mêmes droits et obligations.

Audit juridique d'octobre 2026 (docs/legal/LEGAL-CHANGES.md) : CGU, confidentialité et mentions
légales ont une version anglaise (`/en/terms`, `/en/privacy`, `/en/mentions-legales`), écrite à
part pour être relue comme un document entier. Ces tests vérifient ce qu'un relecteur ne voit pas
d'un coup d'œil : les deux versions ont les mêmes articles, la version anglaise ne laisse pas de
français, aucune valeur juridique n'apparaît entre crochets, et les deux versions s'annoncent
l'une l'autre.
"""
from __future__ import annotations

import html
import os
import re
import sys
import tempfile
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-juridique-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-juridique-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import legal  # noqa: E402

PAGES = ("/terms", "/privacy", "/mentions-legales")
# Noms propres et références officielles qu'une page anglaise garde en français.
NOMS_OFFICIELS = (legal.MEDIATOR["name"], legal.PUBLISHER["registration"], "R.C.S.")
MOTS_FRANCAIS = re.compile(
    r"\b(le|les|des|du|vous|votre|vos|une|pour|avec|dans|sur|est|sont|qui|que|aux|ces|cette|nous|"
    r"données|société|droit|délai|jours|mois|résiliation|rétractation|conditions générales)\b", re.I)


def _page(chemin: str, client: TestClient | None = None) -> str:
    r = (client or TestClient(m.app)).get(chemin)
    assert r.status_code == 200, (chemin, r.status_code)
    return r.text


def _corps(page: str) -> str:
    mains = BeautifulSoup(page, "html.parser").find_all("main")
    assert len(mains) == 1, "un seul contenu principal attendu"
    assert "container" in mains[0].get("class", [])
    return mains[0].decode_contents()


def _texte(page: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", _corps(page)))


@pytest.mark.parametrize("chemin", PAGES)
def test_la_version_anglaise_a_les_MEMES_articles(chemin: str) -> None:
    fr, en = _corps(_page(chemin)), _corps(_page("/en" + chemin))
    for balise in ("<h2", "<li", "<tr", "<dt", "<strong"):
        assert fr.count(balise) == en.count(balise), (chemin, balise)
    assert re.findall(r'id="([\w-]+)"', fr) == re.findall(r'id="([\w-]+)"', en), "les ancres des liens"


@pytest.mark.parametrize("chemin", PAGES)
def test_la_version_anglaise_ne_laisse_PAS_DE_FRANCAIS(chemin: str) -> None:
    texte = _texte(_page("/en" + chemin))
    for nom in NOMS_OFFICIELS:
        texte = texte.replace(html.unescape(nom), " ")
    assert MOTS_FRANCAIS.findall(texte) == []
    assert '<html lang="en">' in _page("/en" + chemin)


@pytest.mark.parametrize("chemin", PAGES + tuple("/en" + p for p in PAGES))
def test_aucune_valeur_juridique_n_est_affichee_ENTRE_CROCHETS(chemin: str) -> None:
    texte = _texte(_page(chemin))
    assert not re.search(r"\[[^\]]*\]", texte) and "{{" not in texte and "None" not in texte


@pytest.mark.parametrize("chemin", PAGES)
def test_les_deux_versions_s_ANNONCENT_et_ont_leur_canonical(chemin: str) -> None:
    fr, en = _page(chemin), _page("/en" + chemin)
    base = "http://testserver"
    for page, propre in ((fr, chemin), (en, "/en" + chemin)):
        tete = page.split("</head>", 1)[0]
        assert f'<link rel="canonical" href="{base}{propre}" />' in tete
        assert f'<link rel="alternate" hreflang="fr" href="{base}{chemin}" />' in tete
        assert f'<link rel="alternate" hreflang="en" href="{base}/en{chemin}" />' in tete
        assert f'<link rel="alternate" hreflang="x-default" href="{base}{chemin}" />' in tete
    # Le bouton de langue mène à l'autre version.
    assert f'href="/en{chemin}" hreflang="en"' in fr
    assert f'href="{chemin}?lang=fr" hreflang="fr"' in en


def test_les_liens_d_une_page_anglaise_restent_en_ANGLAIS() -> None:
    page = _page("/en/terms")
    for cible in ("/en/privacy", "/en/mentions-legales", "/en/pricing"):
        assert f'href="{cible}"' in page, cible
    pied = page.split("<footer", 1)[1]
    for cible, libelle in (("/en/terms", "Terms and Conditions"), ("/en/privacy", "Privacy Policy"),
                           ("/en/mentions-legales", "Legal Notice")):
        assert f'href="{cible}"' in pied and libelle in pied, cible


def test_les_pages_anglaises_sont_au_PLAN_DU_SITE_et_PUBLIQUES(monkeypatch) -> None:
    plan = _page("/sitemap.xml")
    for chemin in PAGES:
        assert f"<loc>http://testserver/en{chemin}</loc>" in plan, chemin
    monkeypatch.setattr(m, "_beta_basic_auth_expected", lambda: ("beta", "secret"))
    for chemin in PAGES:
        _page("/en" + chemin)


def test_les_clauses_qui_PROTEGENT_sont_dans_les_deux_langues() -> None:
    fr, en = (re.sub(r"\s+", " ", html.unescape(_page(c))) for c in ("/terms", "/en/terms"))
    paires = (
        ("<strong>14 jours</strong>", "<strong>14 days</strong>"),
        ("<strong>12 mois</strong>", "<strong>12 months</strong>"),
        ("<strong>1 000 €</strong>", "<strong>€1,000</strong>"),
        ("ne vous fait pas perdre votre droit de rétractation", "does not take away your right of withdrawal"),
        ("garantie légale de conformité", "legal guarantee of conformity"),
        ("faute lourde ou de dol", "gross negligence or wilful misconduct"),
        ("la version française constitue la version de référence", "the French version shall serve as the reference version"),
        ("ne garantissent pas l'absence d'erreurs", "do not guarantee the absence of errors"),
        ("au moins 30 jours avant son entrée en vigueur", "at least 30 days before it takes effect"),
        ("article L224-25-26 du Code de la consommation", "Article L224-25-26 of the French Consumer Code"),
    )
    for francais, anglais in paires:
        assert francais in fr and anglais in en, (francais, anglais)


def test_chaque_mention_INCONNUE_est_listee_dans_le_fichier_de_validation() -> None:
    validation = (WEB_ROOT / "docs" / "legal" / "LEGAL-VALIDATION-REQUIRED.md").read_text(encoding="utf-8")
    for cle in legal.missing():
        assert f"`{cle}`" in validation, cle
