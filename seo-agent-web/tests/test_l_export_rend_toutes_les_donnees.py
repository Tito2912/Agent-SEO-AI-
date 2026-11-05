# -*- coding: utf-8 -*-
"""« Télécharger toutes mes données » (05/11/2026, backend/export_compte.py).

La restitution promise par le DPA (article 15) et la portabilité du RGPD (article 20). Ce que ces
tests défendent : l'export rend TOUT ce que la base rattache au compte, y compris ce qui n'en
dépend qu'indirectement ; il ne rend AUCUN secret ; il ne rend RIEN d'un autre compte, même quand
le titulaire a travaillé sur le projet de quelqu'un d'autre ; les fichiers d'analyse partent les
plus récents d'abord, jusqu'au plafond, le reste listé ; le titulaire seul le télécharge, au plus
trois fois par heure, et le fichier temporaire disparaît après l'envoi.
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import sys
import tempfile
import uuid
import zipfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-export-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, export_compte  # noqa: E402
from backend.models import (AuditLog, PasswordResetToken, Project, TrackedKeyword,  # noqa: E402
                            TrackedKeywordSnapshot, User, UserConnection)


def _compte(db, prefixe: str) -> User:
    user = User(email="%s-%s@exemple.fr" % (prefixe, uuid.uuid4().hex[:8]), password_hash="pbkdf2$EMPREINTE-SECRETE")
    db.add(user)
    db.flush()
    return user


def _projet(db, user: User, slug: str, **reglages) -> Project:
    projet = Project(owner_user_id=user.id, slug=slug, base_url="https://%s.exemple.fr" % slug, site_name=slug,
                     settings=reglages)
    db.add(projet)
    db.flush()
    return projet


@pytest.fixture()
def comptes():
    """A (le titulaire) et B (un autre compte, sur le projet duquel A a travaillé)."""
    m.DB.create_tables()
    with m.DB.session() as db:
        a, b = _compte(db, "titulaire"), _compte(db, "autre")
        pa = _projet(db, a, "boutique-a", wordpress={"url": "https://boutique-a.exemple.fr", "app_password": "MDP-WP"},
                     jeton_chiffre="enc:CHIFFRE-SECRET")
        pb = _projet(db, b, "site-b")
        mot_a = TrackedKeyword(project_id=pa.id, user_id=a.id, query="chaussures de randonnée")
        mot_b = TrackedKeyword(project_id=pb.id, user_id=a.id, query="MOT-DU-COMPTE-B")
        db.add_all([mot_a, mot_b])
        db.flush()
        db.add(TrackedKeywordSnapshot(keyword_id=mot_a.id, captured_on=dt.date(2026, 11, 1), clicks=42))
        db.add(UserConnection(user_id=a.id, key="github", secret_value="JETON-GITHUB-SECRET",
                              meta={"login": "titulaire", "access_token": "JETON-DANS-META"}))
        db.add(AuditLog(actor_user_id=a.id, action="project.create", status="ok", meta={"slug": "boutique-a"}))
        db.add(PasswordResetToken(user_id=a.id, token_hash="EMPREINTE-DE-JETON-" + uuid.uuid4().hex[:12],
                                  expires_at=dt.datetime.now(dt.timezone.utc)))
        db.commit()
        return {"a": a.id, "b": b.id, "email_a": a.email, "email_b": b.email, "pa": pa.id, "pb": pb.id}


def _lignes(uid):
    with m.DB.session() as db:
        return export_compte.lignes_du_compte(db, uid)


SECRETS = ["EMPREINTE-SECRETE", "MDP-WP", "CHIFFRE-SECRET", "JETON-GITHUB-SECRET", "JETON-DANS-META", "EMPREINTE-DE-JETON"]


# ── Ce qui sort, ce qui ne sort pas ─────────────────────────────────────────────────────────

def test_TOUT_ce_qui_depend_du_compte_sort(comptes) -> None:
    lignes = _lignes(comptes["a"])
    assert [u["email"] for u in lignes["users"]] == [comptes["email_a"]]
    assert [p["id"] for p in lignes["projects"]] == [comptes["pa"]]
    assert [k["query"] for k in lignes["tracked_keywords"]] == ["chaussures de randonnée"]
    assert [s["clicks"] for s in lignes["tracked_keyword_snapshots"]] == [42], "dépendance indirecte"
    assert lignes["user_connections"][0]["key"] == "github" and lignes["user_connections"][0]["meta"]["login"] == "titulaire"
    assert any(e["action"] == "project.create" for e in lignes["audit_logs"])
    assert lignes["projects"][0]["settings"]["wordpress"]["url"] == "https://boutique-a.exemple.fr"


def test_AUCUN_secret_ne_sort(comptes) -> None:
    lignes = _lignes(comptes["a"])
    tout = json.dumps(lignes, ensure_ascii=False)
    assert not [s for s in SECRETS if s in tout]
    assert "password_hash" not in lignes["users"][0] and "secret_value" not in lignes["user_connections"][0]
    assert "password_reset_tokens" not in lignes
    reglages = lignes["projects"][0]["settings"]
    assert reglages["wordpress"]["app_password"] == export_compte.MASQUE and reglages["jeton_chiffre"] == export_compte.MASQUE


def test_RIEN_d_un_autre_compte_meme_par_une_colonne_cree_par(comptes) -> None:
    """A a suivi un mot-clé dans le projet de B : il appartient au projet de B, pas à l'export de A."""
    tout = json.dumps(_lignes(comptes["a"]), ensure_ascii=False)
    assert comptes["email_b"] not in tout and comptes["pb"] not in tout and "MOT-DU-COMPTE-B" not in tout


def test_le_nettoyage_des_valeurs() -> None:
    assert export_compte.nettoyer({"niveau": {"api_key": "x", "issue_key": "title_missing", "liste": ["enc:abc", 3]},
                                   "quand": dt.datetime(2026, 11, 5, 8, 0, tzinfo=dt.timezone.utc),
                                   "brut": b"\x00\x01"}) == {
        "niveau": {"api_key": export_compte.MASQUE, "issue_key": "title_missing", "liste": [export_compte.MASQUE, 3]},
        "quand": "2026-11-05T08:00:00+00:00", "brut": export_compte.MASQUE}


# ── Le ZIP ───────────────────────────────────────────────────────────────────────────────────

def _fichier(chemin: Path, octets: int, age_s: float) -> None:
    chemin.parent.mkdir(parents=True, exist_ok=True)
    chemin.write_bytes(b"x" * octets)
    instant = dt.datetime.now().timestamp() - age_s
    os.utime(chemin, (instant, instant))


def test_le_zip_ses_tables_ses_analyses_recentes_et_son_plafond(comptes, tmp_path) -> None:
    racine = tmp_path / "runs"
    _fichier(racine / comptes["a"] / "r1" / "report.json", 400, age_s=10)
    _fichier(racine / comptes["a"] / "r2" / "report.json", 400, age_s=1000)
    _fichier(racine / comptes["a"] / "r3" / "report.json", 400, age_s=5)
    destination = tmp_path / "export.zip"
    with m.DB.session() as db:
        user = db.get(User, comptes["a"])
        manifeste = export_compte.construire(db, user, destination, dossiers_analyses=[racine / comptes["a"]],
                                             racine_analyses=racine, plafond=900)
    with zipfile.ZipFile(destination) as archive:
        noms = set(archive.namelist())
        assert {"donnees/users.json", "donnees/users.csv", "manifeste.json", "LISEZMOI.txt"} <= noms
        assert json.loads(archive.read("donnees/users.json"))[0]["email"] == comptes["email_a"]
        assert "chaussures de randonnée" in archive.read("donnees/tracked_keywords.csv").decode("utf-8")
    plus_recents = {"analyses/%s/r3/report.json" % comptes["a"], "analyses/%s/r1/report.json" % comptes["a"]}
    assert plus_recents <= noms and manifeste["analyses"]["fichiers"] == 2
    assert manifeste["analyses"]["laisses_de_cote"] == ["analyses/%s/r2/report.json" % comptes["a"]]


def test_le_zip_en_anglais_porte_un_README(comptes, tmp_path) -> None:
    with m.DB.session() as db:
        export_compte.construire(db, db.get(User, comptes["a"]), tmp_path / "e.zip", dossiers_analyses=[],
                                 racine_analyses=tmp_path, langue="en")
    with zipfile.ZipFile(tmp_path / "e.zip") as archive:
        assert "README.txt" in archive.namelist() and "LISEZMOI.txt" not in archive.namelist()


# ── La route ─────────────────────────────────────────────────────────────────────────────────

def _client(uid) -> TestClient:
    client = TestClient(m.app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client


def test_le_titulaire_telecharge_SON_export_et_le_fichier_temporaire_disparait(comptes, monkeypatch) -> None:
    crees: list[str] = []
    vrai = tempfile.mkstemp
    monkeypatch.setattr(m.tempfile, "mkstemp", lambda **kw: (lambda r: crees.append(r[1]) or r)(vrai(**kw)))
    with _client(comptes["a"]) as client:
        reponse = client.get("/settings/account/export.zip")
    assert reponse.status_code == 200 and reponse.headers["content-type"] == "application/zip"
    assert 'filename="noyaru-donnees-' in reponse.headers["content-disposition"]
    assert reponse.headers["cache-control"] == "no-store"
    with zipfile.ZipFile(io.BytesIO(reponse.content)) as archive:
        tout = "".join(archive.read(n).decode("utf-8", "replace") for n in archive.namelist())
    assert comptes["email_a"] in tout and comptes["email_b"] not in tout
    assert not [s for s in SECRETS if s in tout]
    assert crees and not any(os.path.exists(c) for c in crees), "le ZIP temporaire est effacé après l'envoi"
    with m.DB.session() as db:
        trace = db.scalars(select(AuditLog).where(AuditLog.action == "account.export",
                                                  AuditLog.actor_user_id == comptes["a"])).one()
    assert trace.status == "ok" and trace.meta["tables"] >= 5


def test_les_analyses_rangees_dans_S3_sont_RAMENEES_avant_le_zip(comptes, monkeypatch) -> None:
    """Le worker n'a pas de disque : ses analyses vivent dans S3. Une panne de S3 n'empêche pas
    l'export des données de la base."""
    ramenes: list[Path] = []

    def ramener(racine, dossier):
        ramenes.append(Path(dossier))
        raise ConnectionError("S3 injoignable")

    monkeypatch.setattr(m.object_store, "s3_enabled", lambda: True)
    monkeypatch.setattr(m.object_store, "restore_runs_tree", ramener)
    with _client(comptes["a"]) as client:
        assert client.get("/settings/account/export.zip").status_code == 200
    assert (m.DEFAULT_RUNS_DIR / comptes["a"]).resolve() in ramenes
    assert any(p.name == "boutique-a" for p in ramenes), "et les dossiers anciens, rangés par projet"


def test_sans_connexion_aucun_export(comptes) -> None:
    with TestClient(m.app) as client:
        reponse = client.get("/settings/account/export.zip", follow_redirects=False)
    assert reponse.status_code in {303, 307, 401} and reponse.headers.get("content-type") != "application/zip"


def test_au_plus_TROIS_exports_par_heure(comptes) -> None:
    with _client(comptes["a"]) as client:
        assert [client.get("/settings/account/export.zip").status_code for _ in range(3)] == [200, 200, 200]
        quatrieme = client.get("/settings/account/export.zip", follow_redirects=False)
    assert quatrieme.status_code == 303 and "mes-donnees" in quatrieme.headers["location"]
    assert "err=" in quatrieme.headers["location"]


@pytest.mark.parametrize("chemin, phrase", [("/privacy", "« Mon compte → Mes données » télécharge toutes vos"),
                                            ("/en/privacy", "“My account → My data” downloads all your data")])
def test_la_confidentialite_dit_ou_exercer_l_acces_et_la_portabilite(chemin, phrase) -> None:
    with TestClient(m.app) as client:
        page = client.get(chemin).text
    assert phrase in " ".join(page.split("10.", 1)[1].split())


def test_le_bouton_est_dans_Mon_compte(comptes) -> None:
    with _client(comptes["a"]) as client:
        page = client.get("/settings/accounts").text
    section = page.split('id="mes-donnees"', 1)[1].split("</section>", 1)[0]
    assert 'href="/settings/account/export.zip"' in section
