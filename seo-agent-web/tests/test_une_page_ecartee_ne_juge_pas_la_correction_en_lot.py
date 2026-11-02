"""Une page que la PR a déclaré ne pas corriger ne juge pas la correction en lot.

19/10/2026, creativeai-tools PR #3 : huit méta-descriptions corrigées, `/fr/tools/` refusée et
annoncée « non corrigée » dans la PR elle-même. Après la fusion, le crawl ne signalait plus que
`/fr/tools/` — et la tâche est restée « en cours », sans aucun reçu : une correction en lot vise
toute la famille, et cette page-là la gardait vivante. La PR enregistre désormais les pages dont
tous les fichiers ont été écartés, et la vérification ne les compte plus.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from backend import app as m
from backend.models import IssueTask
from test_corrector_operational import HTML, URL as SITE_URL, harness as harness
from test_first_verified_correction import BASELINE, OTHER, URL, journey as journey

CLE = "meta_description_too_short_indexable"
ECARTEE = "https://atelier.example/fr/tools/"
CARTE = {URL: ["content/service.mdx"], OTHER: ["content/autre.mdx"], ECARTEE: ["content/fr/tools.mdx"]}
FICHIERS = sorted(f for fichiers in CARTE.values() for f in fichiers)


@pytest.fixture()
def carte(monkeypatch):
    monkeypatch.setattr(m.repo_index, "route_files", lambda index, url: CARTE.get(url, []))


def test_seules_les_pages_dont_tous_les_fichiers_sont_ecartes_sortent(carte, monkeypatch):
    pages = [URL, OTHER, ECARTEE, "https://atelier.example/sans-fichier/"]
    assert m._pages_non_traitees(pages, {}, FICHIERS, ["content/fr/tools.mdx"]) == [ECARTEE]
    assert m._pages_non_traitees(pages, {}, FICHIERS, []) == []
    # Un fichier partagé écarté ne suffit pas : la page a aussi un fichier corrigé.
    monkeypatch.setattr(m.repo_index, "route_files", lambda index, url: ["content/fr/tools.mdx", "content/service.mdx"])
    assert m._pages_non_traitees([ECARTEE], {}, FICHIERS, ["content/fr/tools.mdx"]) == []


@pytest.mark.parametrize("mode", ["individual", "bulk"])
def test_la_tache_retient_les_pages_que_la_pr_n_a_pas_touchees(harness, monkeypatch, mode):
    state, run = harness
    autre = "https://site.test/autre"
    state["key"] = "missing_h1_indexable"
    state["sources"] = {"index.html": HTML, "autre.html": HTML}
    state["report"] = {"issues": {state["key"]: {"count": 2, "examples": [SITE_URL, autre]}}}
    monkeypatch.setattr(m.repo_index, "route_files",
                        lambda index, url: {SITE_URL: ["index.html"], autre: ["autre.html"]}.get(url, []))
    monkeypatch.setattr(m, "_deep_patch_issue_files", lambda **k: (
        ["index.html"], ["autre.html"], ["index.html", "autre.html"], []))
    intentions, original = [], m.correction_journal.pr_intent

    def espion(**k):
        intentions.append(k["tasks"])
        return original(**k)

    monkeypatch.setattr(m.correction_journal, "pr_intent", espion)
    assert run(mode)["status"] == 200
    with m.DB.session() as db:
        [tache] = list(db.scalars(select(IssueTask).where(IssueTask.project_id == state["project"].id)))
    note = json.loads(tache.note)
    assert note["verification_urls"] == [SITE_URL, autre] and note["verification_excluded"] == [autre]
    # Le journal de reprise recrée la tâche si le serveur tombe après l'ouverture de la PR :
    # il doit porter la même liste, sinon la reprise la perd.
    [[reprise]] = intentions
    assert reprise["note"]["verification_excluded"] == [autre]


def _tache(data, *, exclues):
    with m.DB.session() as db:
        db.add(IssueTask(project_id=data["project_id"], user_id=data["owner"], issue_key=CLE, issue_label="Meta",
                         url=URL, crawl_ts=BASELINE, status="in_progress", severity="warning",
                         note=json.dumps({"bulk": True, "deep": True, "verification_urls": [URL, OTHER, ECARTEE],
                                          "verification_expected_count": 3,
                                          **({"verification_excluded": [ECARTEE]} if exclues else {})})))
        db.commit()


def _controle(data, issues, urls=(URL, OTHER, ECARTEE)):
    data["report"](BASELINE, issues={CLE: {"count": 3, "examples": [URL, OTHER, ECARTEE]}}, urls=(URL, OTHER, ECARTEE))
    ts = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y%m%d-%H%M%S")
    rapport = data["report"](ts, issues=issues, urls=urls)
    m._verify_corrections_after_crawl(data["slug"], rapport, data["runs_dir"], user_id=data["owner"], crawl_ts=ts)
    with m.DB.session() as db:
        tache = db.scalar(select(IssueTask).where(IssueTask.project_id == data["project_id"]))
    return tache.status, json.loads(tache.note).get("verify", {}).get("result")


def test_la_page_ecartee_seule_restante_laisse_la_correction_se_valider(journey):
    _tache(journey, exclues=True)
    assert _controle(journey, {CLE: {"count": 1, "examples": [ECARTEE]}}) == ("done", "resolved")


def test_sans_la_liste_la_regle_d_avant_tient(journey):
    # Les tâches créées avant ce correctif n'ont pas la liste : toute la famille compte encore.
    _tache(journey, exclues=False)
    assert _controle(journey, {CLE: {"count": 1, "examples": [ECARTEE]}}) == ("in_progress", None)


def test_une_page_corrigee_encore_signalee_garde_la_tache(journey):
    # Encore présent sur une page que la PR a corrigée : la tâche reste en cours, comme avant.
    _tache(journey, exclues=True)
    assert _controle(journey, {CLE: {"count": 2, "examples": [OTHER, ECARTEE]}}) == ("in_progress", None)


def test_des_exemples_tronques_disent_non_verifiable(journey):
    # Deux occurrences, une seule listée : l'autre peut être une page corrigée. Le reçu le dit,
    # au lieu de laisser la tâche « en cours » sans un mot.
    _tache(journey, exclues=True)
    assert _controle(journey, {CLE: {"count": 2, "examples": [ECARTEE]}}) == ("in_progress", "unverified")


def test_la_comparaison_saute_un_crawl_sans_rapport(journey):
    """Même soirée : un crawl coupé par un redéploiement (dossier, pas de rapport) est devenu la
    référence du suivant, et toute la colonne Évolution affichait « - »."""
    journey["report"](BASELINE)
    (journey["runs_dir"] / journey["slug"] / "20261019-182650" / "audit").mkdir(parents=True)
    journey["report"]("20261019-185846", issues={})
    vue = m.dash.project_overview(journey["runs_dir"], journey["slug"], timestamp=None, compare_to=None)
    assert vue["current"]["timestamp"] == "20261019-185846"
    assert vue["compare"]["timestamp"] == BASELINE and vue["compare"]["summary"]


def test_une_page_ecartee_non_revisitee_ne_bloque_pas(journey):
    _tache(journey, exclues=True)
    assert _controle(journey, {}, urls=(URL, OTHER)) == ("done", "resolved")
