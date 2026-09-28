# -*- coding: utf-8 -*-
"""« Sujet IA » : un sujet pour la section choisie, tiré de ce que la concurrence traite.

Demande du propriétaire, 28/09/2026 : un bouton dans le champ Sujet, dans les deux modes. Le
modèle ne TROUVE pas les sujets — `_sujets_non_couverts` les mesure, le moteur de l'écran
Concurrents — il CHOISIT celui qui colle à la section et le formule dans sa langue. Ce que ces
tests défendent : un sujet sans source dans la concurrence n'est jamais présenté, un sujet déjà
traité par la section non plus, et le bouton ne coûte aucun article.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_une_page_neuve_part_en_brouillon as _brouillon  # noqa: E402
from test_un_article_sans_depot import CRAWL  # noqa: E402

app_module = _brouillon.app_module
customer = _brouillon.customer
plan = _brouillon.plan
m = app_module

CONCURRENTS = ["Effet de levier expliqué", "Calcul du rendement d'un ETF", "DCA crypto"]


@pytest.fixture()
def modele(monkeypatch):
    etat = {"vues": [], "reponse": {"sujet": "Comment fonctionne l'effet de levier",
                                    "inspire_de": "effet de levier expliqué",
                                    "pourquoi": "Aucun guide ne l'explique."}}

    def _ai(*, system, user_msg, **kw):
        etat["vues"].append(user_msg)
        return etat["reponse"]

    monkeypatch.setattr(m, "_correction_ai_json", _ai)
    return etat


# ── la fonction ───────────────────────────────────────────────────────────────────────────────

def _proposer(**kw):
    args = dict(section="/fr/guides", langue="fr", titres_section=["DCA crypto : guide"],
                sujets_concurrents=CONCURRENTS)
    args.update(kw)
    return m.proposer_un_sujet(**args)


def test_la_source_est_rendue_A_LA_LETTRE_de_la_liste(modele) -> None:
    """Le modele a recopie la source en minuscules : on rend celle de la liste."""
    proposition, refus = _proposer()
    assert refus == "" and proposition["inspire_de"] == "Effet de levier expliqué", proposition


def test_un_sujet_SANS_SOURCE_chez_les_concurrents_n_est_pas_propose(modele) -> None:
    modele["reponse"] = {"sujet": "Les 10 meilleurs brokers", "inspire_de": "Top brokers 2026"}
    proposition, refus = _proposer()
    assert proposition == {} and "invention" in refus


def test_un_sujet_DEJA_traite_par_la_section_est_refuse(modele) -> None:
    modele["reponse"] = {"sujet": "DCA crypto : guide", "inspire_de": "DCA crypto"}
    proposition, refus = _proposer()
    assert proposition == {} and "déjà une page" in refus


def test_les_sujets_deja_proposes_ne_sont_plus_offerts(modele) -> None:
    _proposer(deja=["Effet de levier expliqué"])
    assert "Effet de levier expliqué" not in modele["vues"][0]
    assert "Calcul du rendement d'un ETF" in modele["vues"][0]


def test_quand_TOUT_a_ete_propose_le_modele_n_est_pas_appele(modele) -> None:
    proposition, refus = _proposer(deja=CONCURRENTS)
    assert proposition == {} and modele["vues"] == [] and "déjà été proposés" in refus


def test_la_consigne_porte_la_section_sa_langue_et_ses_pages(modele) -> None:
    _proposer()
    demande = modele["vues"][0]
    assert "SECTION OU L'ARTICLE SERA PUBLIE : /fr/guides" in demande
    assert "LANGUE DU SUJET : fr" in demande and "- DCA crypto : guide" in demande


# ── la route ──────────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def site(monkeypatch):
    etat = {"sujets": list(CONCURRENTS)}
    monkeypatch.setattr(m, "_own_pages_for_project", lambda runs_dir, slug: (CRAWL, "ts"))
    monkeypatch.setattr(m, "_sujets_non_couverts", lambda db, **kw: list(etat["sujets"]))
    return etat


def _post(client, slug, **body):
    client.get(f"/projects/{slug}")
    token = client.cookies.get(m._CSRF_COOKIE_NAME, "")
    return client.post(f"/api/projects/{slug}/content/sujet", json=body,
                       headers={m._CSRF_HEADER_NAME: token})


def test_sans_CONCURRENTS_rien_n_est_invente(customer, plan, site, modele) -> None:
    client, slug, _pid, _uid = customer
    site["sujets"] = []
    r = _post(client, slug, section="/fr/guides")
    assert r.status_code == 400 and r.json()["concurrents_url"].endswith("/competitors"), r.text
    assert modele["vues"] == []


def test_le_sujet_IA_ne_coute_AUCUN_article(customer, plan, site, modele) -> None:
    client, slug, _pid, _uid = customer
    r = _post(client, slug, section="/fr/guides")
    assert r.status_code == 200 and r.json()["sujet"].startswith("Comment fonctionne"), r.text
    assert plan["debits"] == []


def test_la_langue_du_sujet_est_celle_de_la_SECTION_racine_comprise(customer, plan, site, modele) -> None:
    client, slug, _pid, _uid = customer
    _post(client, slug, section="/de/guides")
    _post(client, slug, section="/guides")
    langues = [re.search(r"LANGUE DU SUJET : (\S+)", v).group(1) for v in modele["vues"]]
    assert langues == ["de", "en"], langues


def test_un_plan_sous_PRO_n_atteint_pas_le_modele(customer, plan, site, modele) -> None:
    client, slug, _pid, _uid = customer
    plan["plan"] = "solo"
    assert _post(client, slug, section="/fr/guides").status_code == 402
    assert modele["vues"] == []


# ── pourquoi aucun sujet : la CAUSE, pas un message pour toutes ──────────────────────────────

def _raison(monkeypatch, rows, pages=CRAWL):
    from types import SimpleNamespace
    monkeypatch.setattr(m, "_own_pages_for_project", lambda runs_dir, slug: (pages, "ts"))
    monkeypatch.setattr(m, "_competitor_rows", lambda db, pid: [SimpleNamespace(**r) for r in rows])
    return m._pourquoi_aucun_sujet(None, project_id="p", owner_user_id="u", slug="s")


def test_des_concurrents_JAMAIS_ANALYSES_se_disent_par_leur_nom(monkeypatch) -> None:
    """Releve le 28/09/2026 : deux concurrents ajoutes, jamais analyses ; on lui disait
    « ajoute des concurrents » — geste deja fait. Le geste qui manque est « Analyser »."""
    r = _raison(monkeypatch, [{"status": "new", "pages": None, "domain": "bitdegree.org"},
                              {"status": "new", "pages": None, "domain": "ebc.com"}])
    assert "Jamais analysés : bitdegree.org, ebc.com" in r and "« Analyser »" in r, r
    assert "ajoute" not in r, r


def test_chaque_etat_a_SA_phrase(monkeypatch) -> None:
    r = _raison(monkeypatch, [{"status": "crawling", "pages": None, "domain": "a.fr"},
                              {"status": "failed", "pages": None, "domain": "b.fr"},
                              {"status": "ready", "pages": [], "domain": "c.fr"},
                              {"status": "ready", "pages": [{"url": "x"}], "domain": "d.fr"}])
    assert "Tes concurrents analysés (d.fr) ne traitent aucun sujet" in r, r
    assert "analyse en cours : a.fr" in r and "illisibles : b.fr, c.fr" in r, r


def test_sans_concurrent_ou_sans_crawl_le_geste_est_autre(monkeypatch) -> None:
    assert "aucun concurrent" in _raison(monkeypatch, [])
    assert "Aucun crawl de ton site" in _raison(monkeypatch, [{"status": "new", "pages": None,
                                                                "domain": "a.fr"}], pages=[])


def test_la_route_rend_la_CAUSE(customer, plan, site, modele, monkeypatch) -> None:
    client, slug, _pid, _uid = customer
    site["sujets"] = []
    monkeypatch.setattr(m, "_pourquoi_aucun_sujet", lambda db, **kw: "CAUSE MESUREE")
    r = _post(client, slug, section="/fr/guides")
    assert r.status_code == 400 and r.json()["error"] == "CAUSE MESUREE", r.text
