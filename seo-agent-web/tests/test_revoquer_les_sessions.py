# -*- coding: utf-8 -*-
"""Révoquer toutes les sessions ouvertes avant une date, sans toucher au chiffrement.

30/09/2026 : un cookie de session administrateur s'est retrouvé dans une capture d'écran. Une
session est un jeton signé de 30 jours dont le serveur ne garde aucune trace ; changer
`SEO_AGENT_SECRET_KEY` l'invaliderait, mais cette clé est aussi un repli de chiffrement.
`SEO_AGENT_SESSIONS_NOT_BEFORE` refuse tout jeton émis avant elle.
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-revoc-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-revoc-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth  # noqa: E402
from backend.models import User  # noqa: E402


def _client_connecte(*, emis_il_y_a: int) -> TestClient:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="revoc-%s@exemple.fr" % uuid.uuid4().hex[:8], password_hash="x", is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
    maintenant = time.time
    try:
        auth.time.time = lambda: maintenant() - emis_il_y_a
        jeton = auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"])
    finally:
        auth.time.time = maintenant
    c = TestClient(m.app)
    c.cookies.set(auth.SESSION_COOKIE_NAME, jeton)
    return c


def _connecte(c: TestClient) -> bool:
    return c.get("/billing", follow_redirects=False).status_code == 200


def test_sans_la_variable_rien_n_est_revoque(monkeypatch) -> None:
    monkeypatch.delenv("SEO_AGENT_SESSIONS_NOT_BEFORE", raising=False)
    assert _connecte(_client_connecte(emis_il_y_a=3600))


def test_une_session_ANTERIEURE_est_refusee_une_POSTERIEURE_acceptee(monkeypatch) -> None:
    ancienne = _client_connecte(emis_il_y_a=3600)
    recente = _client_connecte(emis_il_y_a=0)
    monkeypatch.setenv("SEO_AGENT_SESSIONS_NOT_BEFORE", str(int(time.time()) - 60))
    assert not _connecte(ancienne), "le jeton copié ne doit plus ouvrir le compte"
    assert _connecte(recente), "une reconnexion après la révocation fonctionne"


def test_une_valeur_ILLISIBLE_ne_revoque_rien(monkeypatch) -> None:
    monkeypatch.setenv("SEO_AGENT_SESSIONS_NOT_BEFORE", "demain")
    assert _connecte(_client_connecte(emis_il_y_a=3600))


def test_un_jeton_sans_date_d_emission_est_refuse_quand_la_revocation_est_posee() -> None:
    os.environ["SEO_AGENT_SESSIONS_NOT_BEFORE"] = "1"
    try:
        assert m._session_emise_apres_la_revocation({"uid": "x", "iat": "abc"}) is False
        assert m._session_emise_apres_la_revocation({"uid": "x", "iat": 2}) is True
    finally:
        del os.environ["SEO_AGENT_SESSIONS_NOT_BEFORE"]
