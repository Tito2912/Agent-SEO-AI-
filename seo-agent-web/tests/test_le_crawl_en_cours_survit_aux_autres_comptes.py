"""Le crawl en cours d'un client reste visible quel que soit le volume des AUTRES comptes.

La liste des projets, la page projet, /jobs et l'historique des durées lisaient les 100 (ou 500)
derniers travaux de TOUS les comptes, puis triaient ceux du client. Dès que d'autres comptes en
avaient lancé autant depuis, le crawl en cours disparaissait de ses propres écrans — « aucune
analyse en cours », et le client relançait par-dessus. Le filtre est maintenant posé en base.
"""
from __future__ import annotations

import time
import uuid

import pytest

from backend import app as m
from backend.models import User
from test_first_verified_correction import journey as journey

LISTE_GLOBALE = m._list_jobs  # la fixture `journey` la remplace par une liste vide
TS = "20261002-090000"


def _travail(compte, slug, *, statut="queued", cree, debut=None, fin=None):
    job = m.Job(id=str(uuid.uuid4()), status=statut, created_at=cree, started_at=debut, finished_at=fin,
                result={"type": "crawl", "slug": slug, "user_id": compte, "timestamp": TS})
    m._save_job(job)
    return job


@pytest.fixture()
def submerge(journey, monkeypatch):
    """Un crawl en cours du client, puis 520 travaux plus récents d'un autre compte (projet du
    même nom, même horodatage) et 120 du MÊME compte sur ses autres projets — une agence dont
    le cron a tout mis en file."""
    monkeypatch.setattr(m, "_list_jobs", LISTE_GLOBALE)
    journey["report"]()
    with m.DB.session() as db:
        autre = User(email=f"autre-{uuid.uuid4().hex}@example.invalid", password_hash="test", is_admin=False)
        db.add(autre)
        db.commit()
        autre_id = str(autre.id)
    t0 = time.time() - 2000
    _travail(journey["owner"], journey["slug"], statut="done", cree=t0 - 500, debut=t0 - 500, fin=t0 - 380)
    journey["live"] = _travail(journey["owner"], journey["slug"], cree=t0)
    # Actifs eux aussi : sinon la requête des travaux actifs masquerait un filtre de compte absent.
    for i in range(520):
        _travail(autre_id, journey["slug"], statut="running", cree=t0 + 1 + i, debut=t0, fin=t0 + 7)
    for i in range(120):
        _travail(journey["owner"], f"autre-projet-{i}", cree=t0 + 600 + i)
    return journey


def test_la_liste_des_projets_voit_le_crawl_en_cours(submerge):
    page = submerge["client"].get("/")
    assert page.status_code == 200
    assert page.context["live_crawls"][submerge["slug"]]["id"] == submerge["live"].id


def test_la_page_projet_voit_le_crawl_en_cours(submerge):
    page = submerge["client"].get(f'/projects/{submerge["slug"]}')
    assert page.status_code == 200
    assert page.context["live_job"]["id"] == submerge["live"].id


def test_la_page_des_travaux_montre_le_crawl_du_client(submerge):
    page = submerge["client"].get("/jobs")
    assert page.status_code == 200 and submerge["live"].id in page.text


def test_les_filtres_sont_poses_en_base(submerge):
    actifs = m._list_jobs(limit=2000, comptes=[submerge["owner"]], statuts=("queued",))
    assert len(actifs) == 121 and all(j.status == "queued" for j in actifs)
    projet = m._list_jobs(limit=2000, comptes=[submerge["owner"]], slug=submerge["slug"])
    assert len(projet) == 2 and all(j.result["slug"] == submerge["slug"] for j in projet)


def test_la_duree_d_un_ancien_crawl_est_celle_du_client(submerge):
    # 120 s pour le client ; l'autre compte a le même projet et le même horodatage en 7 s.
    assert m._crawl_timing_map(submerge["slug"], submerge["owner"])[TS]["duration_s"] == 120
