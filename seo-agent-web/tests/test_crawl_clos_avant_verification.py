# -*- coding: utf-8 -*-
"""Un crawl reussi est clos et son rapport envoye AVANT la verification des anciennes corrections.

14/10/2026, avis-invest.com : deux crawls de 84 pages reussis — rapport ecrit sur le disque du
worker — puis le job n'a jamais ete clos ni le rapport envoye au stockage objet. Le controle des
jobs perimes les a declares en echec quinze minutes plus tard : « crawl termine mais aucun
report.json ». Entre les deux tournait la verification des 13 anciennes corrections du projet,
qui chargeait ses 10 rapports de reference en entier — et, pour un fichier absent, remontait
jusqu'a telecharger TOUS les crawls du projet. Trois reponses, et ces tests tiennent chacune :
l'ordre de fin du job, un extrait au lieu du rapport entier, et la remontee supprimee.
"""
from __future__ import annotations

import json
import time
import uuid

import pytest

from backend import app as m
from backend import auth
from backend.models import Project, User


def _projet() -> tuple[str, str]:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="crawl-%s@exemple.fr" % uuid.uuid4().hex[:8],
                 password_hash=auth.hash_password("x" * 12), is_admin=True)
        db.add(u)
        db.commit()
        db.refresh(u)
        slug = "site-%s" % uuid.uuid4().hex[:8]
        db.add(Project(owner_user_id=str(u.id), slug=slug, site_name="site", base_url="https://exemple.fr/"))
        db.commit()
        return str(u.id), slug


def _lancer(monkeypatch, *, verification_plante: bool = False) -> tuple[list, object]:
    user_id, slug = _projet()
    journal: list = []
    job_id = str(uuid.uuid4())
    m._save_job(m.Job(id=job_id, status="queued", created_at=time.time(),
                      result={"type": "crawl", "user_id": user_id, "slug": slug, "skip_billing": True}))

    def crawler(job, cmd, **kw):
        sortie = m.Path(cmd[cmd.index("--output-dir") + 1])
        sortie.mkdir(parents=True, exist_ok=True)
        (sortie / "report.json").write_text(json.dumps({"meta": {"pages_crawled": 84}, "issues": {}, "pages": []}))
        return 0

    def verifier(*a, **k):
        journal.append(("verifier", m._load_job(job_id).status))
        if verification_plante:
            raise RuntimeError("memoire epuisee")

    monkeypatch.setattr(m, "_validate_public_crawl_target", lambda url: None)
    monkeypatch.setattr(m, "_ramener_les_crawls_precedents", lambda *a, **k: [])
    monkeypatch.setattr(m, "_run_subprocess_streaming", crawler)
    monkeypatch.setattr(m, "_sync_runs_path_to_object_store",
                        lambda chemin: journal.append(("envoyer", m._load_job(job_id).status)))
    monkeypatch.setattr(m, "_verify_corrections_after_crawl", verifier)
    m._run_crawl_job(job_id, user_id, slug, None)
    return journal, m._load_job(job_id)


def test_le_job_est_clos_et_le_rapport_envoye_avant_la_verification(monkeypatch) -> None:
    journal, job = _lancer(monkeypatch)

    assert journal == [("envoyer", "done"), ("verifier", "done")]   # une seule fois chacun, dans cet ordre
    assert job.status == "done"


def test_une_verification_qui_plante_ne_fait_plus_echouer_le_crawl(monkeypatch) -> None:
    journal, job = _lancer(monkeypatch, verification_plante=True)

    assert job.status == "done" and ("envoyer", "done") in journal


def test_un_fichier_de_crawl_absent_ne_fait_pas_telecharger_tout_le_projet() -> None:
    racine = m.DEFAULT_RUNS_DIR.resolve()
    cible = racine / "compte" / "site" / "20260714-113634" / "audit" / "report.json"

    assert m._run_tree_candidates(cible) == [racine / "compte" / "site" / "20260714-113634"]


def test_la_reference_garde_un_extrait_et_rien_d_autre() -> None:
    rapport = {"meta": {"timestamp": "t"}, "issues": {"k": {"count": 2}}, "resources": [{"url": "x"}] * 50,
               "pages": [{"url": "https://e.fr/a", "final_url": "https://e.fr/a", "meta_robots": "noindex",
                          "x_robots_tag": None, "internal_links": ["l"] * 500, "pagespeed": {"gros": "x" * 1000}}]}

    extrait = m._extrait_de_reference(rapport)

    assert extrait == {"meta": {"timestamp": "t"}, "issues": {"k": {"count": 2}},
                       "pages": [{"url": "https://e.fr/a", "final_url": "https://e.fr/a",
                                  "meta_robots": "noindex", "x_robots_tag": None}]}


def test_la_verification_garde_l_extrait_et_non_le_rapport() -> None:
    import inspect
    assert "_extrait_de_reference(old)" in inspect.getsource(m._verify_corrections_after_crawl)


@pytest.mark.parametrize("rapport", [{}, {"issues": None, "pages": None}])
def test_un_rapport_maigre_donne_un_extrait_vide_mais_valide(rapport) -> None:
    assert m._extrait_de_reference(rapport) == {"issues": {}, "meta": {}, "pages": []}
