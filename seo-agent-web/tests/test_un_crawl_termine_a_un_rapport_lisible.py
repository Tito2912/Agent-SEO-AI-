# -*- coding: utf-8 -*-
"""« Terminé » veut dire « lisible » : un crawl n'est clos qu'une fois son rapport envoyé.

19/10/2026, creativeai-tools : crawl de 87 pages réussi, `report.json` écrit sur le disque du
worker, job marqué « terminé »… puis une fusion a redéployé le worker avant l'envoi au stockage
objet. Le rapport a disparu avec le disque. L'écran est retombé en silence sur le crawl
précédent : mêmes chiffres, et l'on a cru qu'une PR fusionnée n'avait rien corrigé.
"""
from __future__ import annotations

import json
import re
import time
import uuid

import pytest

from backend import app as m, object_store
from test_crawl_clos_avant_verification import _projet
from test_first_verified_correction import journey as journey

PLUS_RECENT = "20991020-090000"


def _crawler(job, cmd, **kw):
    sortie = m.Path(cmd[cmd.index("--output-dir") + 1])
    sortie.mkdir(parents=True, exist_ok=True)
    (sortie / "report.json").write_text(json.dumps({"meta": {"pages_crawled": 87}, "issues": {}, "pages": []}))
    return 0


def _lancer(monkeypatch, envoi):
    user_id, slug = _projet()
    job_id = str(uuid.uuid4())
    m._save_job(m.Job(id=job_id, status="queued", created_at=time.time(),
                      result={"type": "crawl", "user_id": user_id, "slug": slug, "skip_billing": True}))
    verifications = []
    monkeypatch.setattr(m, "_validate_public_crawl_target", lambda url: None)
    monkeypatch.setattr(m, "_ramener_les_crawls_precedents", lambda *a, **k: [])
    monkeypatch.setattr(m, "_run_subprocess_streaming", _crawler)
    monkeypatch.setattr(m, "_sync_runs_path_to_object_store", lambda chemin: None)
    monkeypatch.setattr(m, "_envoyer_le_crawl", envoi)
    monkeypatch.setattr(m, "_verify_corrections_after_crawl", lambda *a, **k: verifications.append(1))
    return job_id, user_id, slug, verifications


def test_un_envoi_qui_echoue_ne_laisse_pas_un_faux_termine(monkeypatch):
    def envoi(chemin):
        raise OSError("stockage indisponible")

    job_id, user_id, slug, verifications = _lancer(monkeypatch, envoi)
    m._run_crawl_job(job_id, user_id, slug, None)
    job = m._load_job(job_id)
    assert job.status == "failed" and "[RAPPORT]" in job.stderr and "Relance" in job.stderr
    assert not verifications  # rien à vérifier contre un rapport que personne ne lira


def test_un_worker_coupe_pendant_l_envoi_n_a_jamais_dit_termine(monkeypatch):
    def envoi(chemin):
        raise SystemExit(143)  # SIGTERM du redéploiement

    job_id, user_id, slug, _ = _lancer(monkeypatch, envoi)
    with pytest.raises(SystemExit):
        m._run_crawl_job(job_id, user_id, slug, None)
    # Resté « en cours » : le battement s'arrête et le contrôle des jobs le déclare interrompu.
    assert m._load_job(job_id).status == "running"


def test_le_rapport_part_en_dernier(tmp_path, monkeypatch):
    envoyes = []

    class Client:
        def upload_file(self, source, bucket, key):
            envoyes.append(key.rsplit("/", 2)[-2:])

    dossier = tmp_path / "compte" / "site" / "20261019-182650"
    for chemin in ("audit/report.json", "audit/report.md", "audit/crawl.jsonl", "bing/bing.json", "gsc/gsc.json"):
        (dossier / chemin).parent.mkdir(parents=True, exist_ok=True)
        (dossier / chemin).write_text("{}")
    monkeypatch.setattr(object_store, "_s3_client", lambda: Client())
    monkeypatch.setattr(object_store, "s3_bucket_name", lambda: "seau")
    assert object_store.upload_runs_path(tmp_path, dossier) == 5
    assert envoyes[-1] == ["audit", "report.json"] and len(envoyes) == 5


@pytest.fixture()
def repli(journey):
    journey["report"]()
    (journey["runs_dir"] / journey["slug"] / PLUS_RECENT / "audit").mkdir(parents=True)
    return journey


def _bandeau(data, chemin):
    page = data["client"].get(f'/projects/{data["slug"]}{chemin}')
    assert page.status_code == 200
    trouve = re.search(r'data-repli="([a-z_]+)"', page.text)
    return trouve.group(1) if trouve else ""


@pytest.mark.parametrize("chemin", ["", "/issues"])
def test_un_rapport_perdu_le_dit_et_demande_de_relancer(repli, chemin):
    assert _bandeau(repli, chemin) == "perdu"
    page = repli["client"].get(f'/projects/{repli["slug"]}{chemin}')
    assert "est introuvable" in page.text and "Relancez-la" in page.text


@pytest.mark.parametrize("chemin", ["", "/issues"])
def test_une_analyse_en_cours_annonce_son_rapport(repli, chemin):
    m._save_job(m.Job(id=str(uuid.uuid4()), status="running", created_at=time.time(),
                      result={"type": "crawl", "slug": repli["slug"], "user_id": repli["owner"]}))
    assert _bandeau(repli, chemin) == "en_cours"


def test_sans_repli_aucun_bandeau(journey):
    journey["report"]()
    assert _bandeau(journey, "/issues") == ""
