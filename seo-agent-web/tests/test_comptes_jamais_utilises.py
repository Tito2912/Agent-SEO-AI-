# -*- coding: utf-8 -*-
"""Supprimer les comptes jamais utilisés — et seulement ceux-là.

Relevé du 02/11/2026 : 39 des 44 comptes étaient des inscriptions de robots. Décision du
propriétaire : « si tu es sûr que ce ne sont pas de vrais clients, on supprime ». Ce que ces tests
défendent : UNE seule trace d'usage, quelle qu'elle soit, garde le compte ; la preuve est refaite
au moment de supprimer ; l'écran ne coche rien d'office ; seul le propriétaire du système y accède.
"""
from __future__ import annotations

import os
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-comptes-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, comptes_robots  # noqa: E402
from backend.models import (AccountInvite, AccountMember, AuditLog, BillingCustomer,  # noqa: E402
                            EmailVerificationToken, JobRecord, OAuthIdentity, Project, UsageEvent,
                            User, UserConnection)

IL_Y_A_UN_MOIS = datetime.now(timezone.utc) - timedelta(days=30)


def _compte(*, cree=IL_Y_A_UN_MOIS, admin=False) -> tuple[str, str]:
    m.DB.create_tables()
    email = "robot-%s@exemple.fr" % uuid.uuid4().hex[:10]
    with m.DB.session() as db:
        u = User(email=email, password_hash="x", is_admin=admin, created_at=cree)
        db.add(u)
        db.commit()
        db.refresh(u)
        return str(u.id), email


def _ajouter(*lignes) -> None:
    with m.DB.session() as db:
        for ligne in lignes:
            db.add(ligne)
        db.commit()


def _raisons(uid: str) -> list[str]:
    with m.DB.session() as db:
        return comptes_robots.raisons_de_garder(db, db.get(User, uid))


def test_un_compte_SANS_aucune_trace_est_candidat() -> None:
    uid, _email = _compte()
    assert _raisons(uid) == []


def _autre() -> str:
    return _compte()[0]


TRACES = {
    "possède un site": lambda uid, email: [Project(owner_user_id=uid, slug="s-" + uuid.uuid4().hex[:6],
                                                  site_name="s", base_url="https://s.fr/", settings={})],
    "a lancé une analyse": lambda uid, email: [JobRecord(id=str(uuid.uuid4()), status="done", kind="crawl",
                                                       owner_user_id=uid, slug="x")],
    "consommation enregistrée": lambda uid, email: [UsageEvent(user_id=uid, period="2026-10",
                                                               metric="pages_crawled_month", amount=1, meta={})],
    "client Stripe": lambda uid, email: [BillingCustomer(user_id=uid, stripe_customer_id="cus_" + uuid.uuid4().hex)],
    "équipe": lambda uid, email: [AccountMember(owner_user_id=_autre(), member_user_id=uid)],
    "invitation": lambda uid, email: [AccountInvite(owner_user_id=_autre(), email=email,
                                                    token_hash=uuid.uuid4().hex + uuid.uuid4().hex,
                                                    expires_at=datetime.now(timezone.utc))],
    "compte Google associé": lambda uid, email: [OAuthIdentity(user_id=uid, provider="google",
                                                              provider_user_id=uuid.uuid4().hex, email=email)],
    "service connecté": lambda uid, email: [UserConnection(user_id=uid, key="GITHUB_TOKEN", secret_value="x", meta={})],
    "s'est déjà connecté": lambda uid, email: [AuditLog(action="auth.login", status="ok", actor_email=email)],
    "email confirmé": lambda uid, email: [EmailVerificationToken(user_id=uid, token_hash=uuid.uuid4().hex,
                                                                 expires_at=datetime.now(timezone.utc),
                                                                 used_at=datetime.now(timezone.utc))],
}


@pytest.mark.parametrize("raison", sorted(TRACES))
def test_UNE_seule_trace_garde_le_compte(raison) -> None:
    uid, email = _compte()
    _ajouter(*TRACES[raison](uid, email))
    assert _raisons(uid) == [raison]


def test_un_compte_RECENT_ou_ADMINISTRATEUR_est_garde() -> None:
    recent, _ = _compte(cree=datetime.now(timezone.utc) - timedelta(days=2))
    admin, _ = _compte(admin=True)
    assert _raisons(recent) == ["inscrit il y a moins de 7 jours"]
    assert _raisons(admin) == ["administrateur"]


def test_une_connexion_ECHOUEE_n_est_pas_un_usage() -> None:
    uid, email = _compte()
    _ajouter(AuditLog(action="auth.login", status="invalid_password", actor_email=email),
             AuditLog(action="auth.signup", status="ok", actor_email=email))
    assert _raisons(uid) == []


def test_la_suppression_REFAIT_la_preuve_et_efface_l_adresse_du_journal() -> None:
    robot, email_robot = _compte()
    revenu, email_revenu = _compte()
    _ajouter(AuditLog(action="auth.signup", status="ok", actor_email=email_robot))
    # Coché sur l'écran, puis connecté avant le clic : il doit être gardé.
    _ajouter(AuditLog(action="auth.login", status="ok", actor_email=email_revenu))
    with m.DB.session() as db:
        supprimes, gardes = comptes_robots.supprimer(db, [robot, revenu])
    assert supprimes == [email_robot]
    assert gardes == [(email_revenu, ["s'est déjà connecté"])]
    with m.DB.session() as db:
        assert db.get(User, robot) is None and db.get(User, revenu) is not None
        assert db.scalar(select(AuditLog).where(AuditLog.actor_email == email_robot)) is None
        assert db.scalar(select(AuditLog.id).where(AuditLog.action == "auth.signup",
                                                   AuditLog.actor_email.is_(None))) is not None


# ── L'écran ──────────────────────────────────────────────────────────────────────────────────

def _client(uid: str) -> TestClient:
    c = TestClient(m.app)
    c.cookies.set(auth.SESSION_COOKIE_NAME,
                  auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return c


@pytest.fixture()
def proprietaire(monkeypatch):
    uid, email = _compte(admin=True)
    monkeypatch.setattr(m, "_system_settings_owner_email", lambda: email)
    return _client(uid)


def test_seul_le_PROPRIETAIRE_du_systeme_y_accede(monkeypatch) -> None:
    uid, _ = _compte()
    monkeypatch.setattr(m, "_system_settings_owner_email", lambda: "quelqu-un-d-autre@exemple.fr")
    assert _client(uid).get("/settings/activation/comptes-inutilises").status_code == 403


def test_l_ecran_liste_sans_RIEN_cocher_et_la_suppression_demande_SUPPRIMER(proprietaire) -> None:
    robot, email = _compte()
    from bs4 import BeautifulSoup
    page = proprietaire.get("/settings/activation/comptes-inutilises")
    case = BeautifulSoup(page.text, "html.parser").select_one('input[name="compte"][value="%s"]' % robot)
    assert email in page.text and case is not None
    assert not case.has_attr("checked"), "rien n'est coché d'office"
    jeton = proprietaire.cookies.get(m._CSRF_COOKIE_NAME, "")
    sans = proprietaire.post("/settings/activation/comptes-inutilises/supprimer",
                             data={"compte": robot, "confirmation": "oui", "_csrf": jeton}, follow_redirects=False)
    assert sans.status_code == 303 and "err=" in sans.headers["location"]
    with m.DB.session() as db:
        assert db.get(User, robot) is not None
    avec = proprietaire.post("/settings/activation/comptes-inutilises/supprimer",
                             data={"compte": robot, "confirmation": "SUPPRIMER", "_csrf": jeton},
                             follow_redirects=False)
    assert avec.status_code == 303 and "msg=" in avec.headers["location"]
    with m.DB.session() as db:
        assert db.get(User, robot) is None
