# -*- coding: utf-8 -*-
"""La route et l'écran du mode « contenu seul » : mêmes portes que le mode GitHub, autre sortie.

Ce que ces tests défendent : aucun des deux modes n'est la porte dérobée de l'autre (plan,
quota compté en VERSIONS, cadence), un article sans crawl ne coûte rien, et un article ne se
relit que depuis son propre projet.
"""
from __future__ import annotations

import json
import re
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_une_page_neuve_part_en_brouillon as _brouillon  # noqa: E402
from test_un_article_sans_depot import CRAWL, LIENS  # noqa: E402

# Les fixtures de compte et de plan de la suite brouillon, REEXPOSEES par affectation : un
# import direct suivi de leur usage en parametre est une redefinition pour ruff (F811), et la
# barriere CI la refuse.
app_module = _brouillon.app_module
Project = _brouillon.Project
customer = _brouillon.customer
plan = _brouillon.plan


@pytest.fixture()
def crawl(monkeypatch):
    etat = {"pages": list(CRAWL)}
    monkeypatch.setattr(app_module, "_own_pages_for_project",
                        lambda runs_dir, slug: (etat["pages"], "20260928-0600"))
    return etat


@pytest.fixture()
def modele(monkeypatch):
    vues: dict[str, str] = {}

    def _ai(*, system, user_msg, **kw):
        langue = re.search(r"LANGUE DE L'ARTICLE : (\S+)", user_msg).group(1)
        vues[langue] = user_msg
        return {"titre": "Titre %s" % langue, "description": "Desc %s" % langue,
                "slug": "levier-%s" % langue, "markdown": "## Partie\n\n%s\n" % LIENS[langue]}

    monkeypatch.setattr(app_module, "_correction_ai_json", _ai)
    return vues


def _post(client, slug, **body):
    client.get(f"/projects/{slug}")
    token = client.cookies.get(app_module._CSRF_COOKIE_NAME, "")
    return client.post(f"/api/projects/{slug}/content/article", json=body,
                       headers={app_module._CSRF_HEADER_NAME: token})


def test_sans_CRAWL_rien_n_est_redige_ni_debite(customer, plan, modele, monkeypatch) -> None:
    client, slug, _pid, _uid = customer
    monkeypatch.setattr(app_module, "_own_pages_for_project", lambda runs_dir, slug: ([], ""))
    r = _post(client, slug, sujet="Le levier", section="/fr/guides")
    assert r.status_code == 400 and "crawl" in r.json()["error"], r.text
    assert modele == {} and plan["debits"] == []


def test_un_article_en_trois_langues_rend_markdown_ET_html_et_debite_TROIS(
        customer, plan, crawl, modele) -> None:
    client, slug, _pid, _uid = customer
    r = _post(client, slug, sujet="Le levier", section="/fr/guides", langues=["de", ""])
    assert r.status_code == 200, r.text
    versions = r.json()["versions"]
    assert [v["code"] for v in versions] == ["fr", "de", "en"]
    fr = versions[0]
    assert fr["markdown"].startswith("# Titre fr\n") and fr["html"].startswith("<h1>Titre fr</h1>")
    assert fr["adresse"] == "/fr/guides/levier-fr/"
    assert [d.get("amount") for d in plan["debits"]] == [3], plan["debits"]


def test_les_ETAPES_d_un_article_se_suivent(customer, plan, crawl, modele) -> None:
    client, slug, _pid, _uid = customer
    _post(client, slug, sujet="Le levier", section="/fr/guides", langues=["de"],
          suivi="r-article-0001")
    etapes = client.get(f"/api/projects/{slug}/content/suivi/r-article-0001").json()["etapes"]
    assert etapes == ["Rédaction de la version principale", "Traduction en 1 langue, en parallèle",
                      "Vérification des liens internes"], etapes


def test_le_quota_est_compte_en_VERSIONS_avant_le_modele(customer, plan, crawl, modele) -> None:
    client, slug, _pid, _uid = customer
    plan["restant"] = 1
    r = _post(client, slug, sujet="Le levier", section="/fr/guides", langues=["de"])
    assert r.status_code == 402 and "Il te reste 1" in r.json()["error"], r.text
    assert modele == {}


def test_un_plan_sous_PRO_n_atteint_pas_le_modele(customer, plan, crawl, modele) -> None:
    client, slug, _pid, _uid = customer
    plan["plan"] = "solo"
    assert _post(client, slug, sujet="Le levier", section="/fr/guides").status_code == 402
    assert modele == {}


def test_l_article_se_RELIT_depuis_le_journal_sans_rien_couter(customer, plan, crawl, modele) -> None:
    client, slug, _pid, _uid = customer
    ecrit = _post(client, slug, sujet="Le levier", section="/fr/guides").json()
    modele.clear()
    debits = list(plan["debits"])
    lu = client.get(f"/api/projects/{slug}/content/article/{ecrit['id']}").json()
    assert lu["ok"] and lu["versions"] == ecrit["versions"] and lu["sujet"] == "Le levier"
    assert modele == {} and plan["debits"] == debits
    page = client.get(f"/projects/{slug}/content").text
    assert 'data-ouvrir-article="%s"' % ecrit["id"] in page


def test_l_article_d_un_AUTRE_projet_ne_se_lit_pas(customer, plan, crawl, modele) -> None:
    client, slug, _pid, uid = customer
    ecrit = _post(client, slug, sujet="Le levier", section="/fr/guides").json()
    autre = f"autre-{uuid.uuid4().hex[:8]}"
    with app_module.DB.session() as db:
        db.add(Project(owner_user_id=uid, slug=autre, site_name="autre", base_url="https://a.fr/",
                       settings={}))
        db.commit()
    r = client.get(f"/api/projects/{autre}/content/article/{ecrit['id']}")
    assert r.status_code == 404, r.text


def test_l_ecran_propose_les_SECTIONS_du_crawl_et_NOMME_la_langue_racine(
        customer, plan, crawl) -> None:
    """« /blog/mon-sujet » était proposé sur un site dont les pages vivent sous /guides ; et la
    langue de la racine s'affichait « racine (sans préfixe) » alors que ses pages la déclarent."""
    client, slug, _pid, _uid = customer
    page = client.get(f"/projects/{slug}/content").text
    # « 8 pages » ne disait pas ce qu'il comptait (capture du 28/09/2026).
    assert '<option value="/fr/guides">/fr/guides/ · 3 pages déjà publiées</option>' in page
    assert "EN (racine)" in page and 'data-iso="en"' in page


def test_la_note_stockee_garde_le_MARKDOWN_pas_le_html(customer, plan, crawl, modele) -> None:
    """Le HTML est rendu a la lecture : une correction du rendu vaut pour les articles deja
    ecrits, sans migration."""
    client, slug, pid, _uid = customer
    ecrit = _post(client, slug, sujet="Le levier", section="/fr/guides").json()
    with app_module.DB.session() as db:
        note = json.loads(db.get(app_module.IssueTask, ecrit["id"]).note)
    assert "markdown" in note["versions"][0] and "html" not in note["versions"][0]
