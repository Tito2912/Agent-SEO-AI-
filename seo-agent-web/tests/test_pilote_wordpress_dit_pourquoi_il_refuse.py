"""Le pilote WordPress dit pourquoi une connexion est refusée, sans jamais garder de secret.

Il exige WordPress 6.9 et Yoast 28.7. Seules les connexions réussies étaient journalisées : on
ignorait combien de clients essayaient, et sur quel mur ils butaient (version, droits, adresse).
Chaque refus porte maintenant un code stable, relu sur /settings/operations.
"""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from backend import app as m, auth, cms_routes, wordpress_cms as wp
from backend.models import AuditLog, User
from test_cms_integrations import cms as cms, connect, journey as journey, post

MOT_DE_PASSE = "test-application-password"


def _tentatives(data):
    with m.DB.session() as db:
        return list(db.scalars(select(AuditLog).where(AuditLog.action == "cms.wordpress.connect",
                                                      AuditLog.target_id == data["slug"])))


def _tenter(data, *, site_url="https://atelier.example", username="editor"):
    reponse = post(data, "/wordpress/connect", site_url=site_url, username=username, password=MOT_DE_PASSE)
    assert reponse.status_code == 303
    return reponse


@pytest.mark.parametrize("cas,raison", [("host", "adresse"), ("permission", "droits"),
                                        ("schema", "yoast_ancien"), ("saisie", "saisie")])
def test_chaque_refus_est_journalise_avec_sa_raison(cms, cas, raison):
    cms["remote"]["capability"] = cas != "permission"
    cms["remote"]["old_schema"] = cas == "schema"
    reponse = _tenter(cms, site_url="https://wrong.example" if cas == "host" else "https://atelier.example",
                      username="editor:x" if cas == "saisie" else "editor")
    assert "err=" in reponse.headers["location"]
    [ligne] = _tentatives(cms)
    assert ligne.status == "declined" and ligne.meta == {"reason": raison}
    assert MOT_DE_PASSE not in json.dumps(ligne.meta) and "editor" not in json.dumps(ligne.meta)


def test_une_connexion_reussie_reste_journalisee(cms):
    connect(cms)
    assert [ligne.status for ligne in _tentatives(cms)] == ["ok"]


@pytest.mark.parametrize("statut,raison", [(404, "version"), (401, "acces_refuse"), (302, "redirection"),
                                           (500, "reponse_illisible")])
def test_le_transport_nomme_la_raison(monkeypatch, statut, raison):
    monkeypatch.setattr(wp, "public_address", lambda host: "93.184.216.34")

    class Connexion:
        def __init__(self, host, address):
            pass

        def request(self, method, path, **kw):
            pass

        def getresponse(self):
            return SimpleNamespace(status=statut, read=lambda size: b"{}", getheader=lambda *args: "application/json")

        def close(self):
            pass

    monkeypatch.setattr(wp, "_PinnedHTTPS", Connexion)
    with pytest.raises(wp.CmsError) as erreur:
        wp.WordPressClient({"site_url": "https://atelier.example", "username": "editor", "password": "x"}).check()
    assert erreur.value.code == raison and raison in wp.REASONS


def test_un_refus_client_n_est_pas_une_panne_de_production(cms):
    avant = m._production_operations_snapshot()["db"]["recent_audit_problem_count"]
    cms["remote"]["capability"] = False
    _tenter(cms)
    assert m._production_operations_snapshot()["db"]["recent_audit_problem_count"] == avant


def test_la_page_des_operations_montre_le_bilan(cms, monkeypatch):
    cms["remote"]["capability"] = False
    _tenter(cms)
    _tenter(cms)
    cms["remote"]["capability"] = True
    connect(cms)
    bilan = cms_routes.connection_report()
    assert bilan["connected"] >= 1 and bilan["declined"] >= 2
    assert any(r["code"] == "droits" and r["label"] == wp.REASONS["droits"] for r in bilan["reasons"])
    with m.DB.session() as db:
        email = db.get(User, cms["owner"]).email
    monkeypatch.setenv("SYSTEM_SETTINGS_OWNER_EMAIL", email)
    proprietaire = TestClient(m.app)
    proprietaire.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(
        user_id=cms["owner"], secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    page = proprietaire.get("/settings/operations")
    assert page.status_code == 200
    assert "Pilote WordPress" in page.text and wp.REASONS["droits"] in page.text
