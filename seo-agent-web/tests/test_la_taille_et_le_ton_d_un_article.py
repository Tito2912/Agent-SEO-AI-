# -*- coding: utf-8 -*-
"""Taille et ton d'un article, dans les deux modes (demande du propriétaire, 28/09/2026).

Décisions : « comme la section » par défaut (l'article imite une page existante, comme avant),
trois tailles, quatre tons ; un choix explicite PRIME sur la page imitée ; un article compte
pour UN au quota quelle que soit sa taille. Ce que ces tests défendent : le choix arrive au
modèle dans les deux modes, un article long a le plafond ET le délai qu'il lui faut, une
traduction suit sa référence au lieu de recevoir une longueur, un choix inconnu ne coûte rien.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_une_page_neuve_part_en_brouillon as _brouillon  # noqa: E402
from test_un_article_sans_depot import CRAWL, LIENS  # noqa: E402

app_module = _brouillon.app_module
m = app_module
customer = _brouillon.customer
plan = _brouillon.plan
github = _brouillon.github


# ── les regles ────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("taille, ton", [("", ""), ("auto", "auto"), ("LONG", " Expert ")])
def test_un_choix_connu_passe_et_auto_vaut_comme_la_section(taille, ton) -> None:
    style, refus = m._style_valide(taille, ton)
    assert refus == ""
    assert style == ({"taille": "long", "ton": "expert"} if taille == "LONG" else {"taille": "", "ton": ""})


@pytest.mark.parametrize("taille, ton", [("geant", ""), ("", "sarcastique")])
def test_un_choix_INCONNU_est_refuse(taille, ton) -> None:
    style, refus = m._style_valide(taille, ton)
    assert style == {} and "inconnu" in refus


def test_sans_choix_la_consigne_ne_change_pas() -> None:
    assert m._consigne_de_style(None) == "" and m._consigne_de_style({"taille": "", "ton": ""}) == ""


def test_la_consigne_dit_la_longueur_et_le_ton_et_qu_ils_PRIMENT() -> None:
    c = m._consigne_de_style({"taille": "moyen", "ton": "pedagogique"})
    assert "environ 1500 mots (entre 1275 et 1725)" in c and c.count("PRIME") == 2
    assert "pedagogique" in c


def test_un_article_long_a_le_plafond_ET_le_delai_qu_il_lui_faut() -> None:
    """Un appel sans flux ne rend rien avant sa fin : a 90 s fixes, 9 000 tokens echouaient."""
    long = {"taille": "long", "ton": ""}
    assert m._plafond_de_tokens(None, traduction=False) == 4000, "rien ne change par defaut"
    assert m._plafond_de_tokens(long, traduction=False) == 9000
    assert m._plafond_de_tokens(long, traduction=True) == 13500
    assert m._delai_pour(4000) == 130 and m._delai_pour(1000) == 90
    assert m._delai_pour(13500) * 40 >= 13500, "le delai doit couvrir ~40 tokens/s"


def test_le_delai_est_celui_de_l_appel_reel(monkeypatch) -> None:
    vu = {}

    class _Rep:
        status_code = 200

        def json(self):
            return {"content": [{"type": "text", "text": "{}"}]}

    def _post(url, **kw):
        vu["timeout"] = kw.get("timeout")
        return _Rep()

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
    monkeypatch.setattr(m.requests, "post", _post)
    m._anthropic_messages_text(system="s", user_msg="u", model="x", max_tokens=9000)
    assert vu["timeout"] == m._delai_pour(9000) > 90


# ── mode contenu seul ─────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def appels(monkeypatch):
    vus: list[dict] = []

    def _ai(*, system, user_msg, **kw):
        langue = re.search(r"LANGUE DE L'ARTICLE : (\S+)", user_msg)
        vus.append({"user": user_msg, "max_tokens": kw.get("max_tokens")})
        if langue:
            code = langue.group(1)
            return {"titre": "Titre %s" % code, "description": "Desc %s" % code,
                    "slug": "levier-%s" % code, "markdown": "## Partie\n\n%s\n" % LIENS[code]}
        return {"contenu": _brouillon.REDIGE}

    monkeypatch.setattr(m, "_correction_ai_json", _ai)
    return vus


def _post(client, slug, chemin, **body):
    client.get(f"/projects/{slug}")
    token = client.cookies.get(m._CSRF_COOKIE_NAME, "")
    return client.post(f"/api/projects/{slug}/content/{chemin}", json=body,
                       headers={m._CSRF_HEADER_NAME: token})


def test_contenu_seul_le_choix_arrive_au_modele_et_la_TRADUCTION_suit_sa_reference(
        customer, plan, appels, monkeypatch) -> None:
    client, slug, _pid, _uid = customer
    monkeypatch.setattr(m, "_own_pages_for_project", lambda runs_dir, slug: (list(CRAWL), "ts"))
    r = _post(client, slug, "article", sujet="Le levier", section="/fr/guides", langues=["de"],
              taille="long", ton="expert")
    assert r.status_code == 200, r.text
    principale, traduction = appels[0], appels[1]
    assert "environ 2500 mots" in principale["user"] and "expert :" in principale["user"]
    assert principale["max_tokens"] == 9000
    assert "LONGUEUR DU CORPS" not in traduction["user"] and "TON :" not in traduction["user"]
    assert traduction["max_tokens"] == 13500, "une traduction longue a besoin de plus"
    assert [d["amount"] for d in plan["debits"]] == [2], "une version = un article, long ou pas"


def test_contenu_seul_sans_choix_rien_ne_change(customer, plan, appels, monkeypatch) -> None:
    client, slug, _pid, _uid = customer
    monkeypatch.setattr(m, "_own_pages_for_project", lambda runs_dir, slug: (list(CRAWL), "ts"))
    assert _post(client, slug, "article", sujet="Le levier", section="/fr/guides").status_code == 200
    assert "LONGUEUR DU CORPS" not in appels[0]["user"] and appels[0]["max_tokens"] == 4000


@pytest.mark.parametrize("chemin, corps", [
    ("article", {"sujet": "Le levier", "section": "/fr/guides"}),
    ("draft", {"sujet": _brouillon.SUJET, "route": _brouillon.ROUTE}),
])
def test_un_choix_inconnu_ne_coute_RIEN(customer, plan, appels, chemin, corps) -> None:
    client, slug, _pid, _uid = customer
    r = _post(client, slug, chemin, taille="geant", **corps)
    assert r.status_code == 400 and "inconnue" in r.json()["error"]
    assert appels == [] and plan["debits"] == []


# ── mode GitHub ───────────────────────────────────────────────────────────────────────────────

def test_github_le_choix_arrive_au_modele(customer, plan, github, appels) -> None:
    client, slug, _pid, _uid = customer
    r = _post(client, slug, "draft", sujet=_brouillon.SUJET, route=_brouillon.ROUTE,
              taille="court", ton="conversationnel")
    assert r.status_code == 200, r.text
    assert "environ 800 mots" in appels[0]["user"] and "conversationnel :" in appels[0]["user"]
    assert appels[0]["max_tokens"] == 4000


def test_l_ecran_offre_les_choix_de_la_redaction(customer, plan) -> None:
    client, slug, _pid, _uid = customer
    page = client.get(f"/projects/{slug}/content").text
    for cle in list(m._TAILLES_ARTICLE) + list(m._TONS_ARTICLE):
        assert 'value="%s"' % cle in page, cle
    # Deux pour la redaction manuelle, deux pour le mode automatique : memes tables.
    assert page.count(">Comme la section</option>") == 4
    assert page.count("taille: choixStyle('c-taille'), ton: choixStyle('c-ton')") == 2, \
        "les DEUX modes envoient le choix"
