# -*- coding: utf-8 -*-
"""`/cron/autopilot` fait ses balayages et ne lance plus l'audit historique.

MESURE DU 28/09/2026 : la tâche quotidienne GitHub était rouge neuf jours d'affilée, et
`/cron/autopilot` répondait 500 en production. La route enregistrait le job de l'audit
historique (300 pages de `creativeai-tools.com`, dans le conteneur web) au nom de `"cron"`, qui
n'est pas un utilisateur : la clé étrangère `jobs.owner_user_id -> users.id` refusait
l'insertion. Décision du propriétaire : l'audit est retiré, les balayages restent.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-cron-autopilot-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402


@pytest.fixture()
def cron(monkeypatch):
    m.DB.create_tables()
    monkeypatch.setenv("CRON_SECRET", "secret-cron-test")
    etat = {"pr": 0, "contenu": 0, "jobs": 0, "audit": 0, "echec": set()}

    def _pr(limit=25):
        etat["pr"] += 1
        if "pr" in etat["echec"]:
            raise RuntimeError("GitHub muet")

    def _contenu(*a, **kw):
        etat["contenu"] += 1

    monkeypatch.setattr(m, "_balayer_verifications_pr", _pr)
    monkeypatch.setattr(m, "_balayer_contenu_auto", _contenu)
    monkeypatch.setattr(m, "_save_job", lambda job: etat.__setitem__("jobs", etat["jobs"] + 1))
    monkeypatch.setattr(m, "_run_autopilot_job", lambda *a, **kw: etat.__setitem__("audit", etat["audit"] + 1))
    return etat


def _appeler(secret="secret-cron-test"):
    # SANS `with` : ouvrir le client en contexte demarre l'application, donc la boucle de
    # verification des PR qui tourne en continu — elle appelait le balayage une seconde fois
    # et faisait compter deux passages a la route.
    client = TestClient(m.app, raise_server_exceptions=True)
    return client.get("/cron/autopilot", headers={"Authorization": "Bearer " + secret})


def test_la_route_repond_200_et_fait_ses_DEUX_balayages(cron) -> None:
    r = _appeler()
    assert r.status_code == 200, r.text
    assert r.json() == {"ok": True, "balayages": {"pull_requests": "ok", "contenu_auto": "ok"}}
    assert (cron["pr"], cron["contenu"]) == (1, 1), "chaque balayage une fois, pas deux"


def test_l_audit_historique_n_est_plus_ni_enregistre_ni_lance(cron) -> None:
    _appeler()
    assert cron["jobs"] == 0 and cron["audit"] == 0


def test_un_balayage_en_echec_est_DIT_sans_faire_tomber_l_autre(cron) -> None:
    cron["echec"].add("pr")
    r = _appeler()
    assert r.status_code == 200
    assert r.json()["balayages"] == {"pull_requests": "RuntimeError", "contenu_auto": "ok"}


def test_sans_le_bon_secret_rien_ne_tourne(cron) -> None:
    assert _appeler("mauvais").status_code == 401
    assert (cron["pr"], cron["contenu"]) == (0, 0)
