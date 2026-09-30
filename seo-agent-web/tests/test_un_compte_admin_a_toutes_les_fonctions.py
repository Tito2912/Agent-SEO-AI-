# -*- coding: utf-8 -*-
"""Un compte administrateur a toutes les fonctions, quel que soit son abonnement.

30/09/2026 : l'abonnement fantôme du compte administrateur (un « Pro » du mode test) a été clos.
Retombé en Free, il s'est vu refuser la page Opportunités (« réservé aux plans Solo+ ») : cette
porte ne lisait que le plan, et celle des concurrents ne regardait le statut admin que sur la
personne connectée — pas pour le rafraîchissement mensuel de ses rivaux.
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
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-admin-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-admin-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.models import Project, User  # noqa: E402


@pytest.fixture(autouse=True)
def tout_le_monde_en_free(monkeypatch):
    monkeypatch.setattr(billing, "effective_plan_key", lambda db, *, user_id: "free")


def _compte(*, admin: bool) -> tuple[str, str]:
    m.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        u = User(email="adm-%s@exemple.fr" % tag, password_hash="x", is_admin=admin)
        db.add(u)
        db.commit()
        db.refresh(u)
        db.add(Project(owner_user_id=str(u.id), slug="adm-" + tag, site_name="s.fr", base_url="https://s.fr/", settings={}))
        db.commit()
        return str(u.id), "adm-" + tag


@pytest.mark.parametrize("admin", [True, False])
def test_les_portes_par_plan_s_ouvrent_a_l_ADMIN_et_seulement_a_lui(admin) -> None:
    uid, _slug = _compte(admin=admin)
    with m.DB.session() as db:
        assert m._opp_has_access(db, user_id=uid) is admin
        assert m._competitor_has_access(db, user_id=uid) is admin


def test_un_compte_INEXISTANT_n_a_rien() -> None:
    with m.DB.session() as db:
        assert m._opp_has_access(db, user_id="inconnu") is False


def test_la_page_Opportunites_s_ouvre_a_l_admin_en_Free() -> None:
    uid, slug = _compte(admin=True)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    page = client.get("/projects/%s/backlinks/opportunities" % slug).text
    assert "Fonctionnalité réservée aux plans Solo" not in page


def test_temoin_un_client_en_Free_voit_toujours_la_porte() -> None:
    uid, slug = _compte(admin=False)
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    page = client.get("/projects/%s/backlinks/opportunities" % slug).text
    assert "Fonctionnalité réservée aux plans Solo" in page
