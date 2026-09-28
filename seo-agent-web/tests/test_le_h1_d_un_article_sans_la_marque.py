# -*- coding: utf-8 -*-
"""La marque en fin de titre suit la CONVENTION DU SITE, et ne va jamais dans le H1.

Relevé en production le 28/09/2026 (prosperfactory.com, premier article « Long ») : un article a
UN titre, qui sert de balise title ET de H1, et le H1 exporté en Markdown et en HTML devenait
« Comment analyser un projet crypto en profondeur avant d'investir | Prosper Factory ».

Première hypothèse : le modèle imitait la marque des titles du site. FAUSSE — mesuré sur le vrai
crawl, 99 titles sur 102 n'ont aucun suffixe : le modèle l'avait ajoutée de lui-même. Les deux
cas se traitent donc, chacun selon ce que le crawl MESURE :

* le site met une marque en fin de title : le title la garde, le H1 non ;
* le site n'en met pas : celle que le modèle ajoute — si elle NOMME le site — part des deux.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_un_article_sans_depot as _articles  # noqa: E402
import test_une_page_neuve_part_en_brouillon as _brouillon  # noqa: E402

m = _articles.m
customer = _brouillon.customer
plan = _brouillon.plan


# ── la mesure ─────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("titres, attendu", [
    (["DCA crypto | Prosper Factory", "Débuter | Prosper Factory", "Levier | Prosper Factory"],
     " | Prosper Factory"),
    # La marque de la majorite, meme si une page a un autre separateur interne.
    (["A - B | Marque", "C | Marque", "D : guide | Marque", "Accueil"], " | Marque"),
    # Aucun suffixe partage : rien a retirer.
    (["DCA crypto : guide", "Débuter", "Krypto DCA"], ""),
    # Un tiret DANS un titre n'est pas une marque s'il n'est pas partage.
    (["Avant - après", "Pourquoi - comment", "Levier"], ""),
    # Une seule page : on ne conclut pas.
    (["Seul | Marque"], ""),
    # Deux pages sur six partagent une fin de titre : une minorite n'est pas la marque du site.
    (["X | Blog", "Y | Blog", "A", "B", "C", "D"], ""),
])
def test_le_suffixe_de_marque_se_MESURE(titres, attendu) -> None:
    assert m._suffixe_de_marque(titres) == attendu


def test_le_h1_perd_la_marque_mais_jamais_tout_le_titre() -> None:
    assert m._h1_sans_marque("Analyser un projet crypto | Prosper Factory", " | Prosper Factory") == \
        "Analyser un projet crypto"
    assert m._h1_sans_marque("Autre sujet - Marque", " | Prosper Factory") == "Autre sujet - Marque"
    assert m._h1_sans_marque("Court | Marque", " | Marque") == "Court | Marque", "trop court : on garde"
    assert m._h1_sans_marque("Un titre sans marque", "") == "Un titre sans marque"


@pytest.mark.parametrize("titre, site, attendu", [
    ("Analyser un projet crypto | Prosper Factory", "prosperfactory.com", " | Prosper Factory"),
    ("Analyser un projet crypto | Prosper Factory", "https://www.prosperfactory.com/", " | Prosper Factory"),
    ("Analyser un projet crypto - ProsperFactory.com", "prosperfactory.com", " - ProsperFactory.com"),
    # Une fin de titre qui ne NOMME pas le site fait partie du titre.
    ("eToro vs Bitpanda - guide 2026", "prosperfactory.com", ""),
    # Egalite, pas inclusion : « blog.fr » ne mange pas « Blogging guide ».
    ("Ecrire mieux - Blogging guide", "blog.fr", ""),
    ("Analyser | Prosper Factory", "autre.fr", ""),
])
def test_la_marque_AJOUTEE_se_reconnait_au_nom_du_site(titre, site, attendu) -> None:
    assert m._marque_ajoutee(titre, site) == attendu


# ── de bout en bout ──────────────────────────────────────────────────────────────────────────

AJOUT = " | Site Demo"          # ce que le modele ajoute en fin de titre
SITE = "sitedemo.fr"            # et le site qu'il nomme ainsi


@pytest.fixture()
def modele(monkeypatch):
    etat = {"fin": AJOUT}

    def _ai(*, system, user_msg, **kw):
        langue = re.search(r"LANGUE DE L'ARTICLE : (\S+)", user_msg).group(1)
        return {"titre": "Analyser un projet crypto en %s%s" % (langue, etat["fin"]),
                "description": "Desc", "slug": "analyser-%s" % langue,
                "markdown": "## Partie\n\nTexte.\n"}

    monkeypatch.setattr(m, "_correction_ai_json", _ai)
    return etat


def _servir(pages, **kw):
    out = m._preparer_des_articles(pages=m._pages_du_crawl(pages), sujet="Le levier",
                                   section="/fr/guides", site_name=SITE, **kw)
    assert out["ok"], out
    return m._versions_servies(out["versions"])


def test_un_site_QUI_signe_ses_titles_garde_la_marque_en_title_pas_en_H1(modele) -> None:
    pages = [dict(p, title=p["title"] + AJOUT) for p in _articles.CRAWL]
    for v in _servir(pages, langues=["de"]):
        assert v["titre"].endswith(AJOUT), "la balise title GARDE la marque"
        assert v["markdown"].startswith("# Analyser un projet crypto en %s\n" % v["code"]), v["markdown"][:80]
        assert v["html"].startswith("<h1>Analyser un projet crypto en %s</h1>" % v["code"]), v["html"][:80]


def test_un_site_qui_NE_signe_PAS_perd_la_marque_ajoutee_partout(modele) -> None:
    """Le cas de production : le site n'a aucun suffixe, le modele en a ajoute un."""
    v = _servir(_articles.CRAWL)[0]
    assert v["titre"] == "Analyser un projet crypto en fr"
    assert v["markdown"].startswith("# Analyser un projet crypto en fr\n")


def test_une_fin_de_titre_qui_ne_nomme_pas_le_site_reste(modele) -> None:
    modele["fin"] = " - guide complet"
    v = _servir(_articles.CRAWL)[0]
    assert v["titre"] == "Analyser un projet crypto en fr - guide complet"
    assert v["markdown"].startswith("# Analyser un projet crypto en fr - guide complet\n")


def test_un_article_redige_AVANT_garde_son_titre() -> None:
    """Le journal relit des versions stockees sans `h1` : elles restent telles quelles."""
    v = m._versions_servies([{"titre": "Ancien | Marque", "markdown": "## A\n\nB\n"}])[0]
    assert v["markdown"].startswith("# Ancien | Marque\n")


def test_le_JOURNAL_rend_le_meme_H1_que_la_redaction(customer, plan, modele, monkeypatch) -> None:
    """« Articles rediges » relit la version stockee : elle doit porter le H1 mesure."""
    client, slug, _pid, _uid = customer
    pages = [dict(p, title=p["title"] + AJOUT) for p in _articles.CRAWL]
    monkeypatch.setattr(m, "_own_pages_for_project", lambda runs_dir, slug: (pages, "ts"))
    client.get(f"/projects/{slug}")
    entete = {m._CSRF_HEADER_NAME: client.cookies.get(m._CSRF_COOKIE_NAME, "")}
    r = client.post(f"/api/projects/{slug}/content/article", headers=entete,
                    json={"sujet": "Le levier", "section": "/fr/guides"})
    assert r.status_code == 200, r.text
    relu = client.get(f"/api/projects/{slug}/content/article/{r.json()['id']}").json()
    assert relu["versions"][0]["markdown"].startswith("# Analyser un projet crypto en fr\n"), relu
    assert relu["versions"][0]["titre"].endswith(AJOUT)
