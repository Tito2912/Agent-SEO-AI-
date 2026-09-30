# -*- coding: utf-8 -*-
"""La recherche automatique de backlinks respecte le plan et le quota du payeur.

Trouvé le 30/09/2026 en chiffrant les marges par plan : `/cron/auto-search-backlinks` ne
vérifiait NI le plan NI le quota. Un projet à 5 mots-clés et 2 sources en quotidien consommait
~300 recherches SerpAPI par mois, jamais décomptées — un compte Business à 30 projets, ~9 000 pour
un quota de 1 000 —, et chaque opportunité trouvée déclenchait une réponse rédigée par l'IA, elle
aussi hors quota. Un compte redescendu sous Solo gardait sa recherche automatique.
"""
from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-autobl-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import billing  # noqa: E402
from backend.models import User  # noqa: E402

PLANS: dict[str, str] = {}


class _Reponse:
    ok = True

    def __init__(self, q: str) -> None:
        self.q = q

    def json(self) -> dict:
        tag = uuid.uuid4().hex[:6]
        return {"organic_results": [{"link": "https://forum.test/%s/%d" % (tag, i), "title": "Sujet %d" % i,
                                     "snippet": self.q} for i in range(3)]}


@pytest.fixture()
def monde(monkeypatch):
    """Un faux SerpAPI et une fausse IA, qui comptent ce qu'on leur demande."""
    monkeypatch.setenv("CRON_SECRET", "secret-cron")
    monkeypatch.setenv("SERPAPI_API_KEY", "cle-serpapi")
    appels = {"serpapi": [], "ia": 0}

    def _get(url, params=None, **_kw):
        appels["serpapi"].append((params or {}).get("q", ""))
        return _Reponse((params or {}).get("q", ""))

    def _ia(**_kw):
        appels["ia"] += 1
        return "réponse rédigée"

    monkeypatch.setattr(m.requests, "get", _get)
    monkeypatch.setattr(m, "_ai_configured", lambda: True)
    monkeypatch.setattr(m, "_ai_generate_text", _ia)
    monkeypatch.setattr(billing, "effective_plan_key", lambda db, *, user_id: PLANS.get(str(user_id), "free"))
    return appels


def _projet(plan: str, *, mots: int = 1, auto_draft: bool = True) -> tuple[str, str, list[str]]:
    m.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    mots_cles = ["mot-%s-%d" % (tag, i) for i in range(mots)]
    with m.DB.session() as db:
        user = User(email="bl-%s@exemple.fr" % tag, password_hash="x", is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        db.add(m.Project(owner_user_id=uid, slug="bl-" + tag, site_name="s.fr", base_url="https://s.fr/",
                         settings={"backlinks_auto": {"enabled": True, "keywords": mots_cles, "sources": ["google"],
                                                      "frequency": "daily", "max_per_run": 50, "auto_draft": auto_draft}}))
        db.commit()
    PLANS[uid] = plan
    return uid, "bl-" + tag, mots_cles


def _lancer() -> dict:
    rep = TestClient(m.app).get("/cron/auto-search-backlinks", headers={"Authorization": "Bearer secret-cron"})
    assert rep.status_code == 200, rep.text
    return rep.json()


def _resultat(sortie: dict, slug: str) -> dict:
    return next(p for p in sortie["projects"] if p["project"] == slug)


def _mes_appels(appels: dict, mots_cles: list[str]) -> int:
    return sum(1 for q in appels["serpapi"] if q in mots_cles)


def _utilise(uid: str, metric: str) -> int:
    with m.DB.session() as db:
        return billing.usage_sum(db, user_id=uid, metric=metric)


def test_un_compte_FREE_ne_declenche_rien(monde) -> None:
    uid, slug, mots = _projet("free")
    ia_avant = monde["ia"]
    assert _resultat(_lancer(), slug)["skipped"] == "plan"
    assert _mes_appels(monde, mots) == 0 and monde["ia"] == ia_avant


def test_chaque_recherche_et_chaque_reponse_sont_DECOMPTEES(monde) -> None:
    uid, slug, mots = _projet("solo", mots=2)
    ia_avant = monde["ia"]
    res = _resultat(_lancer(), slug)
    assert _mes_appels(monde, mots) == 2
    assert _utilise(uid, "backlink_searches_month") == 2
    assert res["drafted"] == monde["ia"] - ia_avant == _utilise(uid, "backlink_replies_month") == 6


def test_quota_de_recherches_ATTEINT_plus_aucun_appel(monde) -> None:
    uid, slug, mots = _projet("solo", mots=3)
    with m.DB.session() as db:
        billing.usage_add(db, user_id=uid, metric="backlink_searches_month",
                          amount=billing.plan_catalog()["solo"]["limits"]["backlink_searches_month"])
    assert _resultat(_lancer(), slug)["skipped"] == "quota"
    assert _mes_appels(monde, mots) == 0


def test_la_recherche_s_ARRETE_en_cours_quand_le_quota_tombe(monde) -> None:
    uid, slug, mots = _projet("solo", mots=4)
    limite = billing.plan_catalog()["solo"]["limits"]["backlink_searches_month"]
    with m.DB.session() as db:
        billing.usage_add(db, user_id=uid, metric="backlink_searches_month", amount=limite - 1)
    assert _resultat(_lancer(), slug)["skipped"] == "quota"
    assert _mes_appels(monde, mots) == 1 and _utilise(uid, "backlink_searches_month") == limite


def test_quota_de_reponses_atteint_les_opportunites_arrivent_SANS_reponse(monde) -> None:
    uid, slug, mots = _projet("solo")
    with m.DB.session() as db:
        billing.usage_add(db, user_id=uid, metric="backlink_replies_month",
                          amount=billing.plan_catalog()["solo"]["limits"]["backlink_replies_month"])
    ia_avant = monde["ia"]
    res = _resultat(_lancer(), slug)
    assert res["found"] == 3 and res["drafted"] == 0 and monde["ia"] == ia_avant
