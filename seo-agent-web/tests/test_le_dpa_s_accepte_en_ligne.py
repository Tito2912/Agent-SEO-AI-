# -*- coding: utf-8 -*-
"""L'acceptation en ligne de l'accord de traitement des données (DPA, article 1 ; 05/11/2026).

Ce que ces tests défendent :
  * RIEN n'apparaît tant que legal.DPA_VERSION vaut None (le texte attend un juriste) ;
  * publié, l'accord s'accepte par une personne qui DÉCLARE son pouvoir, pour une organisation
    identifiée ; la preuve garde l'organisation, le compte, la personne, l'heure, la version et
    l'empreinte du texte EXACT, et la trace de l'action ;
  * une nouvelle version se réaccepte ; un membre d'équipe n'engage pas l'organisation d'un autre ;
  * la preuve SURVIT à la suppression du compte, puis s'efface cinq ans après la fin de l'accord ;
  * la table arrive en production par sa migration, et repart sans rien abîmer.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-dpa-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import func, inspect, select  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, dpa, export_compte, legal  # noqa: E402
from backend.db import Database  # noqa: E402
from backend.models import AccountMember, AuditLog, DpaAcceptance, User  # noqa: E402

FORMULAIRE = {"organisation": "Boutique Exemple SAS", "immatriculation": "934 934 308", "signataire": "Camille Martin",
              "fonction": "Gérante", "habilite": "oui"}


@pytest.fixture()
def titulaire(monkeypatch):
    m.DB.create_tables()
    monkeypatch.setattr(m, "_rate_limit_retry_after", lambda **kwargs: None)
    with m.DB.session() as db:
        user = User(email="dpa-%s@exemple.fr" % uuid.uuid4().hex[:8], password_hash="x")
        db.add(user)
        db.commit()
        return {"id": user.id, "email": user.email}


@pytest.fixture()
def publie(monkeypatch):
    monkeypatch.setattr(legal, "DPA_VERSION", "1.0")


def _client(uid, **cookies) -> TestClient:
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    for nom, valeur in cookies.items():
        client.cookies.set(nom, valeur)
    return client


def _csrf(client) -> str:
    """Le jeton anti-falsification, pris sur « Mon compte », qui a toujours des formulaires."""
    return client.get("/settings/accounts").text.split('name="_csrf" value="', 1)[1].split('"', 1)[0]


def _accepter(client, **champs):
    return client.post("/settings/dpa", data={**FORMULAIRE, **champs, "_csrf": _csrf(client)}, follow_redirects=False)


def _preuves(uid=None):
    with m.DB.session() as db:
        requete = select(DpaAcceptance).order_by(DpaAcceptance.accepte_le)
        if uid is not None:
            requete = requete.where(DpaAcceptance.user_id == uid)
        return list(db.scalars(requete))


# ── Masqué tant que le juriste n'a pas validé ────────────────────────────────────────────────

def test_NON_publie_rien_n_apparait(titulaire, monkeypatch) -> None:
    monkeypatch.setattr(legal, "DPA_VERSION", None)
    with _client(titulaire["id"]) as client:
        assert client.get("/settings/dpa").status_code == 404
        assert client.post("/settings/dpa", data=FORMULAIRE).status_code in {403, 404}
        compte = client.get("/settings/accounts").text
    assert 'id="dpa"' not in compte and "/settings/dpa" not in compte
    assert _preuves(titulaire["id"]) == []


def test_le_depot_ne_publie_PAS_le_brouillon() -> None:
    assert legal.DPA_VERSION is None, "le DPA attend la validation d'un juriste (LEGAL-VALIDATION-REQUIRED.md)"


# ── Publié ───────────────────────────────────────────────────────────────────────────────────

def test_publie_la_page_montre_le_texte_et_la_case(titulaire, publie) -> None:
    with _client(titulaire["id"]) as client:
        page = client.get("/settings/dpa").text
        compte = client.get("/settings/accounts").text
    assert "Accord de traitement des données personnelles (DPA)" in page, "le texte du DPA"
    assert 'name="habilite" type="checkbox" value="oui" required' in page
    assert 'href="/settings/dpa"' in compte.split('id="dpa"', 1)[1]


@pytest.mark.parametrize("champs, raison", [
    ({"habilite": ""}, "habilitée"),
    ({"organisation": " "}, "dénomination"),
    ({"signataire": "X"}, "votre nom"),
    ({"fonction": ""}, "fonction"),
    ({"immatriculation": "<script>"}, "immatriculation"),
], ids=["sans-declaration", "sans-organisation", "nom-trop-court", "sans-fonction", "immatriculation-invalide"])
def test_un_formulaire_INCOMPLET_n_engage_rien(titulaire, publie, champs, raison) -> None:
    with _client(titulaire["id"]) as client:
        reponse = _accepter(client, **champs)
    assert reponse.status_code == 303 and "err=" in reponse.headers["location"]
    assert _preuves(titulaire["id"]) == []


def test_l_acceptation_garde_TOUTE_sa_preuve(titulaire, publie, monkeypatch) -> None:
    # Sous TestClient, l'adresse du client (« testclient ») n'est pas une IP : on fixe celle que lit le site.
    monkeypatch.setattr(m, "_request_client_ip", lambda request: "203.0.113.9")
    with _client(titulaire["id"], noyaru_lang="fr") as client:
        reponse = _accepter(client, organisation="  Boutique   Exemple SAS ")
        apres = client.get("/settings/dpa").text
    assert reponse.status_code == 303 and "msg=" in reponse.headers["location"]
    (preuve,) = _preuves(titulaire["id"])
    attendu = hashlib.sha256((WEB_ROOT / "docs" / "legal" / "DPA-fr.md").read_bytes()).hexdigest()
    assert (preuve.organisation, preuve.immatriculation, preuve.signataire, preuve.fonction) == (
        "Boutique Exemple SAS", "934 934 308", "Camille Martin", "Gérante")
    assert preuve.email == titulaire["email"] and preuve.version == "1.0" and preuve.langue == "fr"
    assert preuve.texte_sha256 == attendu and preuve.accepte_le is not None
    assert preuve.ip_address == "203.0.113.9" and preuve.user_agent and preuve.termine_le is None
    with m.DB.session() as db:
        trace = db.scalars(select(AuditLog).where(AuditLog.action == "dpa.accept",
                                                  AuditLog.actor_user_id == titulaire["id"])).one()
    assert trace.meta == {"version": "1.0", "langue": "fr", "texte_sha256": attendu, "acceptation": preuve.id}
    assert "Accord accepté" in apres and 'name="habilite"' not in apres


def test_en_anglais_la_preuve_porte_l_empreinte_du_texte_ANGLAIS(titulaire, publie) -> None:
    with _client(titulaire["id"], noyaru_lang="en") as client:
        _accepter(client)
    (preuve,) = _preuves(titulaire["id"])
    assert preuve.langue == "en"
    assert preuve.texte_sha256 == hashlib.sha256((WEB_ROOT / "docs" / "legal" / "DPA-en.md").read_bytes()).hexdigest()


def test_une_version_deja_acceptee_ne_se_reaccepte_pas_et_une_NOUVELLE_si(titulaire, publie, monkeypatch) -> None:
    with _client(titulaire["id"]) as client:
        _accepter(client)
        deuxieme = _accepter(client)
        assert "msg=" in deuxieme.headers["location"] and len(_preuves(titulaire["id"])) == 1
        monkeypatch.setattr(legal, "DPA_VERSION", "1.1")
        page = client.get("/settings/dpa").text
        assert "Nouvelle version à accepter" in page and 'name="habilite"' in page
        _accepter(client)
    assert [p.version for p in _preuves(titulaire["id"])] == ["1.0", "1.1"]
    with m.DB.session() as db:
        assert dpa.en_vigueur(db, titulaire["id"]).version == "1.1"


def test_un_MEMBRE_d_un_autre_compte_n_engage_pas_son_organisation(titulaire, publie) -> None:
    with m.DB.session() as db:
        hote = User(email="hote-%s@exemple.fr" % uuid.uuid4().hex[:8], password_hash="x")
        db.add(hote)
        db.flush()
        db.add(AccountMember(owner_user_id=hote.id, member_user_id=titulaire["id"]))
        db.commit()
    with _client(titulaire["id"]) as client:
        page = client.get("/settings/dpa").text
        reponse = _accepter(client)
    assert "titulaire du compte" in page and 'name="habilite"' not in page
    assert "err=" in reponse.headers["location"] and _preuves(titulaire["id"]) == []


# ── Après le compte ──────────────────────────────────────────────────────────────────────────

def test_la_preuve_SURVIT_a_la_suppression_du_compte(titulaire, publie) -> None:
    with _client(titulaire["id"]) as client:
        _accepter(client)
        client.post("/settings/account/delete", data={"confirm": "DELETE", "_csrf": _csrf(client)}, follow_redirects=False)
    with m.DB.session() as db:
        assert db.get(User, titulaire["id"]) is None, "le compte est bien supprimé"
        (preuve,) = list(db.scalars(select(DpaAcceptance).where(DpaAcceptance.email == titulaire["email"])))
    assert preuve.user_id is None and preuve.termine_le is not None
    assert preuve.organisation == "Boutique Exemple SAS" and preuve.signataire == "Camille Martin"


def test_cinq_ans_apres_la_fin_la_preuve_s_EFFACE(titulaire) -> None:
    maintenant = dt.datetime(2032, 6, 1, tzinfo=dt.timezone.utc)
    with m.DB.session() as db:
        for jours, nom in ((-1, "trop-vieille"), (1, "encore-gardee")):
            db.add(DpaAcceptance(email=nom + "@exemple.fr", organisation=nom, signataire="X Y", fonction="Gérant",
                                 version="1.0", langue="fr", texte_sha256="0" * 64,
                                 termine_le=dpa.limite_de_conservation(maintenant) + dt.timedelta(days=jours)))
        db.add(DpaAcceptance(user_id=titulaire["id"], email=titulaire["email"], organisation="en-cours",
                             signataire="X Y", fonction="Gérant", version="1.0", langue="fr", texte_sha256="0" * 64))
        db.commit()
        assert dpa.purger(db, maintenant) >= 1
        restantes = {p.organisation for p in db.scalars(select(DpaAcceptance))}
    assert "trop-vieille" not in restantes and {"encore-gardee", "en-cours"} <= restantes


def test_le_29_fevrier() -> None:
    assert dpa.limite_de_conservation(dt.datetime(2032, 2, 29, tzinfo=dt.timezone.utc)) == dt.datetime(
        2027, 2, 28, tzinfo=dt.timezone.utc)


def test_la_purge_ne_leve_JAMAIS(monkeypatch) -> None:
    """Une exception dans _retention_loop arrêterait le fil de rétention pour de bon."""
    monkeypatch.setattr(m.dpa, "purger", lambda *a: (_ for _ in ()).throw(RuntimeError("base injoignable")))
    m._purger_preuves_dpa()


@pytest.mark.parametrize("version, demarre", [(None, False), ("1.0", True)])
def test_le_fil_de_retention_tourne_des_que_le_DPA_est_publie(monkeypatch, version, demarre) -> None:
    for variable in ("SEO_AGENT_JOBS_RETENTION_DAYS", "SEO_AGENT_RUNS_RETENTION_DAYS"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(legal, "DPA_VERSION", version)
    monkeypatch.setattr(m, "_RETENTION_STARTED", False)
    lances: list = []
    monkeypatch.setattr(m.threading, "Thread", lambda **kw: type("Fil", (), {"start": lambda self: lances.append(kw)})())
    m._start_retention()
    assert bool(lances) is demarre


def test_l_export_du_client_contient_ses_acceptations(titulaire, publie) -> None:
    with _client(titulaire["id"]) as client:
        _accepter(client)
    with m.DB.session() as db:
        lignes = export_compte.lignes_du_compte(db, titulaire["id"])
    assert [p["organisation"] for p in lignes["dpa_acceptances"]] == ["Boutique Exemple SAS"]


# ── La migration ─────────────────────────────────────────────────────────────────────────────

def test_la_migration_cree_la_table_et_repart_SANS_RIEN_ABIMER(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + str(tmp_path / "dpa.db"))
    base = Database(data_dir=tmp_path / "data")

    def migrer(sens, revision):
        r = subprocess.run([sys.executable, "-m", "alembic", "-c", str(WEB_ROOT / "alembic.ini"), sens, revision],
                           cwd=WEB_ROOT, env=os.environ.copy(), capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, r.stderr

    migrer("upgrade", "20261104_0025")
    with base.session() as db:
        user = User(email="migration-dpa@example.invalid", password_hash="x")
        db.add(user)
        db.commit()
        uid = user.id
    migrer("upgrade", "20261105_0026")
    migrer("upgrade", "20261105_0026")
    colonnes = {c["name"] for c in inspect(base.engine).get_columns("dpa_acceptances")}
    assert colonnes == {c.name for c in DpaAcceptance.__table__.columns}
    with base.session() as db:
        db.add(DpaAcceptance(user_id=uid, email="migration-dpa@example.invalid", organisation="O", signataire="S S",
                             fonction="F", version="1.0", langue="fr", texte_sha256="0" * 64))
        db.commit()
        assert db.scalar(select(func.count()).select_from(DpaAcceptance)) == 1
    migrer("downgrade", "20261104_0025")
    assert not inspect(base.engine).has_table("dpa_acceptances")
    with base.session() as db:
        assert db.get(User, uid).email == "migration-dpa@example.invalid"
    migrer("upgrade", "20261105_0026")
    base.engine.dispose()
