"""La page de première correction lit son rapport une seule fois et retrouve l'analyse en cours
par une requête ciblée.

Elle relisait le même report.json quand la correction venait du dernier rapport (le cas courant),
et parcourait les 100 derniers travaux de TOUS les comptes, journaux compris, à chaque affichage :
coûteux, et l'analyse en cours pouvait déjà être sortie de cette fenêtre.
"""
from __future__ import annotations

import time
import uuid

from backend import app as m
from test_first_verified_correction import BASELINE, _choose, journey as journey


def _page(data):
    page = data["client"].get(f'/projects/{data["slug"]}/first-correction')
    assert page.status_code == 200
    return page


def _travail(data, *, statut="queued", slug=None, compte=None):
    job = m.Job(id=str(uuid.uuid4()), status=statut, created_at=time.time(),
                result={"type": "crawl", "slug": slug or data["slug"], "user_id": compte or data["owner"]})
    m._save_job(job)
    return job


def test_le_rapport_de_reference_n_est_pas_relu(journey, monkeypatch):
    _choose(journey)
    lectures = []
    origine = m.dash.load_report_json

    def compter(runs_dir, slug, ts):
        lectures.append(ts)
        return origine(runs_dir, slug, ts)

    monkeypatch.setattr(m.dash, "load_report_json", compter)
    page = _page(journey)
    assert page.context["state"]["crawl_ts"] == BASELINE and page.context["instructions"]
    assert lectures.count(BASELINE) == 1


def test_l_analyse_en_cours_est_trouvee_sans_la_liste_globale(journey, monkeypatch):
    # La liste globale rendue vide : seule la requête ciblée peut encore la voir.
    monkeypatch.setattr(m, "_list_jobs", lambda **kw: [])
    journey["report"]()
    job = _travail(journey)
    assert _page(journey).context["live_job"] == {"id": job.id, "status": "queued"}


def test_les_analyses_terminees_ou_d_un_autre_projet_sont_ignorees(journey):
    journey["report"]()
    _travail(journey, statut="done")
    _travail(journey, slug="un-autre-projet")
    _travail(journey, compte=journey["member"])
    assert _page(journey).context["live_job"] is None
