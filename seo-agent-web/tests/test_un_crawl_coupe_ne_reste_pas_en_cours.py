# -*- coding: utf-8 -*-
"""Un crawl dont le worker est mort ne reste pas « en cours » pendant 12 heures.

19/10/2026, creativeai-tools : un deploiement (fusion de la PR #19) a redemarre le worker pendant un
crawl, arrete a 50 pages sur 300. Le web et le worker sont deux conteneurs : le web ne voit ni le
processus ni le fichier du crawl, et `updated_at` n'avancait que lorsque le crawler imprimait une
ligne — une phase muette et un worker mort se ressemblaient. Le seul rattrapage etait un filet de
12 h ; le client voyait « en cours » toute la journee, sans un mot.

Deux pieces : le worker BAT pour son job toutes les 30 s, du debut a la fin ; et un crawl qui
tourne mais ne bat plus depuis 5 minutes est declare interrompu, avec un message qui dit quoi faire.
"""
from __future__ import annotations

import inspect
import threading
import time
import uuid

from sqlalchemy import update

from backend import app as m
from backend import auth
from backend.models import JobRecord, Project, User


def _proprietaire() -> tuple[str, str]:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="coupe-%s@exemple.fr" % uuid.uuid4().hex[:8],
                 password_hash=auth.hash_password("x" * 12), is_admin=True)
        db.add(u)
        db.commit()
        db.refresh(u)
        slug = "site-%s" % uuid.uuid4().hex[:8]
        db.add(Project(owner_user_id=str(u.id), slug=slug, site_name="site", base_url="https://exemple.fr/"))
        db.commit()
        return str(u.id), slug


def _crawl(statut: str = "running", *, silence_s: float, stderr: str = "") -> str:
    user_id, slug = _proprietaire()
    job_id = str(uuid.uuid4())
    debut = time.time() - 3600
    m._save_job(m.Job(
        id=job_id, status=statut, created_at=debut, started_at=debut, stderr=stderr,
        command=["python", "skills/public/seo-autopilot/scripts/seo_audit.py", "https://exemple.fr/",
                 "--output-dir", "/introuvable/%s/audit" % job_id],
        progress={"type": "crawl", "current": 50, "total": 300},
        result={"type": "crawl", "user_id": user_id, "slug": slug, "skip_billing": True}))
    with m.DB.session() as db:
        db.execute(update(JobRecord).where(JobRecord.id == job_id).values(updated_at=time.time() - silence_s))
        db.commit()
    return job_id


def _rattraper(monkeypatch, job_id: str) -> tuple[object, list]:
    factures: list = []
    monkeypatch.setattr(m, "_finalize_crawl_billing_after_stale",
                        lambda job, **k: factures.append(job.id))
    m._finalize_stale_job(m._load_job(job_id))
    return m._load_job(job_id), factures


def test_dix_minutes_de_silence_le_crawl_est_declare_interrompu(monkeypatch) -> None:
    job, factures = _rattraper(monkeypatch, _crawl(silence_s=600))

    assert job.status == "failed"
    assert "[INTERROMPU] Plus aucun signe du worker depuis 10 min" in job.stderr
    assert "Relance-la" in job.stderr
    assert factures == [job.id]


def test_quatre_minutes_et_demie_ne_suffisent_pas(monkeypatch) -> None:
    """Au-dela du garde de 240 s, en deca des 5 minutes : un crawl lent n'est pas tue."""
    job, factures = _rattraper(monkeypatch, _crawl(silence_s=270))

    assert job.status == "running" and factures == []


def test_un_crawl_en_file_n_est_jamais_declare_interrompu(monkeypatch) -> None:
    """Un job en attente ne bat pas : il n'a pas encore de worker."""
    job, factures = _rattraper(monkeypatch, _crawl("queued", silence_s=3000))

    assert job.status == "queued" and factures == []


def test_la_sortie_d_erreur_existante_est_gardee(monkeypatch) -> None:
    job, _ = _rattraper(monkeypatch, _crawl(silence_s=900, stderr="[WARN] page lente\n"))

    assert job.stderr.startswith("[WARN] page lente")
    assert "[INTERROMPU]" in job.stderr


def test_le_seuil_se_regle(monkeypatch) -> None:
    monkeypatch.setenv("SEO_AGENT_STALE_CRAWL_SILENT_SECONDS", "1200")
    job, _ = _rattraper(monkeypatch, _crawl(silence_s=600))

    assert job.status == "running"


def _updated_at(job_id: str) -> float:
    with m.DB.session() as db:
        return float(db.get(JobRecord, job_id).updated_at)


def test_le_worker_bat_pour_son_job_puis_s_arrete(monkeypatch) -> None:
    monkeypatch.setenv("SEO_AGENT_JOB_HEARTBEAT_SECONDS", "0.05")
    job_id = _crawl(silence_s=600)
    avant = _updated_at(job_id)
    arret = threading.Event()
    fil = threading.Thread(target=m._battre_pour_le_job, args=(job_id, arret), daemon=True)
    fil.start()
    time.sleep(0.3)
    pendant = _updated_at(job_id)
    arret.set()
    fil.join(timeout=2)

    assert pendant > avant + 500 and not fil.is_alive()


def test_un_job_termine_ne_bat_plus(monkeypatch) -> None:
    monkeypatch.setenv("SEO_AGENT_JOB_HEARTBEAT_SECONDS", "0.05")
    job_id = _crawl("done", silence_s=600)
    avant = _updated_at(job_id)
    arret = threading.Event()
    threading.Thread(target=m._battre_pour_le_job, args=(job_id, arret), daemon=True).start()
    time.sleep(0.3)
    arret.set()

    assert _updated_at(job_id) == avant


def test_l_execution_bat_pendant_le_job_et_plus_apres(monkeypatch) -> None:
    monkeypatch.setenv("SEO_AGENT_JOB_HEARTBEAT_SECONDS", "0.05")
    job_id = _crawl(silence_s=600)
    vu: list = []
    monkeypatch.setattr(m, "_execute_queued_job", lambda jid: (time.sleep(0.3), vu.append(_updated_at(jid))))
    avant = _updated_at(job_id)
    m._executer_avec_battement(job_id)
    juste_apres = _updated_at(job_id)
    time.sleep(0.3)

    assert vu and vu[0] > avant + 500
    assert _updated_at(job_id) == juste_apres


def test_la_boucle_du_worker_passe_par_le_battement() -> None:
    assert "_executer_avec_battement(jid)" in inspect.getsource(m._job_worker_loop)
