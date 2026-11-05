# -*- coding: utf-8 -*-
"""L'essai mensuel de restauration de la sauvegarde (05/11/2026, backend/restauration.py).

Exigé par l'accord de traitement des données (« possibilité de restauration »). Ce que ces tests
défendent : l'essai prend la sauvegarde la PLUS RÉCENTE, refuse de restaurer un fichier dont
l'empreinte diffère du manifeste, restaure dans un serveur jetable qu'il arrête toujours, juge la
restauration sur ce qu'elle rend (tables, comptes, version du schéma), et que son rapport arrive
au journal d'audit et sur Telegram. La tâche n'a pas l'adresse de la base de production.

Le serveur Postgres n'existe que dans l'image de la tâche (Dockerfile.restauration) : ici, S3 et
les commandes sont simulés ; le premier lancement sur Render est l'essai réel.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

WEB_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = WEB_ROOT.parent
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-restauration-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import app as m  # noqa: E402
from backend import restauration  # noqa: E402
from backend.models import AuditLog  # noqa: E402

PREFIXE = "backups/noyaru-prod"
SAUVEGARDE = b"PGDMP contenu de la sauvegarde"
TABLES = ["alembic_version", "users"] + ["table_%02d" % i for i in range(23)]


class FauxS3:
    """Un bucket : des objets, et une liste servie DEUX clés par page, comme une vraie pagination."""

    def __init__(self, objets: dict[str, bytes]):
        self.objets = objets

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):  # noqa: N803
        cles = sorted(k for k in self.objets if k.startswith(Prefix))
        debut = int(ContinuationToken or 0)
        page = {"Contents": [{"Key": k} for k in cles[debut:debut + 2]]}
        if debut + 2 < len(cles):
            page["NextContinuationToken"] = str(debut + 2)
        return page

    def get_object(self, Bucket, Key):  # noqa: N803
        return {"Body": io.BytesIO(self.objets[Key])}

    def download_file(self, Bucket, Key, Filename):  # noqa: N803
        Path(Filename).write_bytes(self.objets[Key])


def _bucket(*, empreinte: str | None = None) -> FauxS3:
    objets: dict[str, bytes] = {}
    for ts in ("20261101-020001", "20261103-020002", "20261105-020003"):
        objets["%s/db-%s.dump" % (PREFIXE, ts)] = SAUVEGARDE
        objets["%s/manifest-%s.json" % (PREFIXE, ts)] = json.dumps({"ts": ts, "files": [
            {"type": "postgres_dump_custom", "key": "%s/db-%s.dump" % (PREFIXE, ts), "bytes": len(SAUVEGARDE),
             "sha256": empreinte or hashlib.sha256(SAUVEGARDE).hexdigest()}]}).encode()
    return FauxS3(objets)


@pytest.fixture()
def commandes(monkeypatch):
    """Les commandes lancées ; `etat["tables"]` est ce que rendra le comptage, `etat["echec"]` le
    binaire qui échouera."""
    etat = {"lancees": [], "tables": {t: (3 if t == "users" else 1) for t in TABLES}, "echec": ""}

    def lancer(commande, **_kw):
        etat["lancees"].append(list(commande))
        binaire = Path(commande[4] if commande[0] == "runuser" else commande[0]).name
        if binaire == etat["echec"]:
            return subprocess.CompletedProcess(commande, 1, "", "ERROR: relation manquante")
        sortie = "\n".join("%s\t%d" % (t, n) for t, n in etat["tables"].items()) if binaire == "psql" else ""
        return subprocess.CompletedProcess(commande, 0, sortie, "")

    monkeypatch.setattr(restauration.subprocess, "run", lancer)
    return etat


def _binaires(etat) -> list[str]:
    return [Path(c[4] if c[0] == "runuser" else c[0]).name for c in etat["lancees"]]


def _verifier(s3, tmp_path):
    return restauration.verifier(s3, "bucket", PREFIXE, tmp_path)


# ── L'essai ──────────────────────────────────────────────────────────────────────────────────

def test_la_DERNIERE_sauvegarde_est_restauree_et_comptee(commandes, tmp_path) -> None:
    rapport = _verifier(_bucket(), tmp_path)
    assert rapport["ok"] is True, rapport
    assert rapport["sauvegarde"] == "20261105-020003", "la plus récente, même en troisième page"
    assert rapport["empreinte_ok"] and rapport["tables"] == 25 and rapport["comptes"] == 3
    assert _binaires(commandes) == ["initdb", "pg_ctl", "createdb", "pg_restore", "psql", "pg_ctl"]
    restore = next(c for c in commandes["lancees"] if Path(c[0]).name == "pg_restore")
    assert {"--exit-on-error", "--no-owner", "--no-acl"} <= set(restore)
    demarrage = commandes["lancees"][1]
    assert demarrage[-1] == "start" and "listen_addresses=''" in " ".join(demarrage), "aucune écoute réseau"


def test_une_empreinte_DIFFERENTE_n_est_jamais_restauree(commandes, tmp_path) -> None:
    rapport = _verifier(_bucket(empreinte="0" * 64), tmp_path)
    assert rapport["ok"] is False and rapport["empreinte_ok"] is False and "empreinte" in rapport["erreur"]
    assert commandes["lancees"] == []


def test_un_echec_de_pg_restore_est_RAPPORTE_et_le_serveur_ARRETE(commandes, tmp_path) -> None:
    commandes["echec"] = "pg_restore"
    rapport = _verifier(_bucket(), tmp_path)
    assert rapport["ok"] is False and "pg_restore" in rapport["erreur"] and "relation manquante" in rapport["erreur"]
    assert commandes["lancees"][-1][-1] == "stop", "le serveur jetable est arrêté même en cas d'échec"


@pytest.mark.parametrize("tables, raison", [
    ({"alembic_version": 1, "users": 3}, "tables seulement"),
    ({t: (0 if t == "users" else 1) for t in TABLES}, "aucun compte"),
    ({t: 1 for t in TABLES if t != "alembic_version"} | {"table_99": 1}, "version de schéma"),
], ids=["peu-de-tables", "aucun-compte", "sans-version"])
def test_une_restauration_INCOMPLETE_est_un_echec(commandes, tmp_path, tables, raison) -> None:
    commandes["tables"] = tables
    rapport = _verifier(_bucket(), tmp_path)
    assert rapport["ok"] is False and raison in rapport["erreur"]


def test_sans_manifeste_l_essai_echoue_et_le_dit(commandes, tmp_path) -> None:
    rapport = _verifier(FauxS3({}), tmp_path)
    assert rapport["ok"] is False and "aucun manifeste" in rapport["erreur"]


def test_en_ROOT_le_serveur_tourne_sous_postgres(commandes, tmp_path, monkeypatch) -> None:
    """Postgres refuse de démarrer en root ; un conteneur Docker l'est par défaut."""
    monkeypatch.setattr(restauration.os, "geteuid", lambda: 0, raising=False)
    proprietaires: dict[Path, str] = {}
    monkeypatch.setattr(restauration.shutil, "chown", lambda chemin, user: proprietaires.update({Path(chemin): user}))
    assert _verifier(_bucket(), tmp_path)["ok"] is True
    # Le dossier temporaire naît en 700, à root : sans lui, postgres n'atteint rien de ce qu'il contient.
    assert proprietaires.get(tmp_path) == "postgres"
    sous_postgres = [c for c in commandes["lancees"] if c[:4] == ["runuser", "-u", "postgres", "--"]]
    assert [Path(c[4]).name for c in sous_postgres] == ["initdb", "pg_ctl", "pg_ctl"], "le serveur seul"
    assert len(commandes["lancees"]) == 6, "createdb, pg_restore et psql restent des clients"
    assert set(proprietaires.values()) == {"postgres"}


# ── Le rapport ───────────────────────────────────────────────────────────────────────────────

def test_le_rapport_part_au_site_avec_le_secret(monkeypatch) -> None:
    envoye: dict = {}

    class Reponse:
        status_code = 200

    monkeypatch.setenv("PUBLIC_BASE_URL", "https://noyaru.com/")
    monkeypatch.setenv("CRON_SECRET", "secret-cron")
    monkeypatch.setattr(restauration.requests, "post",
                        lambda url, **kw: envoye.update(url=url, **kw) or Reponse())
    assert restauration.envoyer_au_site({"ok": True}) == ""
    assert envoye["url"] == "https://noyaru.com/cron/sauvegarde-verifiee"
    assert envoye["headers"] == {"Authorization": "Bearer secret-cron"} and envoye["json"] == {"ok": True}
    monkeypatch.delenv("CRON_SECRET")
    assert "CRON_SECRET" in restauration.envoyer_au_site({"ok": True})


@pytest.mark.parametrize("ok, code", [(True, 0), (False, 1)])
def test_la_tache_sort_en_ERREUR_quand_l_essai_echoue(monkeypatch, ok, code) -> None:
    monkeypatch.setenv("S3_BUCKET_NAME", "bucket")
    monkeypatch.setattr(restauration.backup, "_s3_client", lambda: object())
    monkeypatch.setattr(restauration, "verifier", lambda *a: {"ok": ok, "par_table": {}})
    monkeypatch.setattr(restauration, "envoyer_au_site", lambda rapport: "")
    assert restauration.main() == code


# ── La route du site ─────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def site(monkeypatch):
    m.DB.create_tables()
    messages: list[str] = []
    monkeypatch.setenv("CRON_SECRET", "secret-cron")
    monkeypatch.setattr(m.telegram, "notifier", messages.append)
    return messages


def _envoyer(client, rapport, secret="secret-cron"):
    return client.post("/cron/sauvegarde-verifiee", json=rapport, headers={"Authorization": "Bearer " + secret})


def _dernier_essai():
    with m.DB.session() as db:
        return db.scalars(select(AuditLog).where(AuditLog.action == "backup.restore_check")
                          .order_by(AuditLog.created_at.desc())).first()


def test_sans_le_secret_RIEN_n_est_garde(site) -> None:
    with TestClient(m.app) as client:
        assert _envoyer(client, {"ok": True}, secret="faux").status_code == 401
        assert client.post("/cron/sauvegarde-verifiee", json={"ok": True}).status_code == 401
    assert site == []


def test_un_rapport_illisible_est_refuse(site) -> None:
    with TestClient(m.app) as client:
        assert _envoyer(client, ["pas", "un", "rapport"]).status_code == 400
        assert _envoyer(client, {"ok": "oui"}).status_code == 400
    assert site == []


def test_un_essai_REUSSI_est_garde_au_journal_et_annonce(site) -> None:
    rapport = {"ok": True, "sauvegarde": "20261105-020003", "tables": 25, "lignes": 4210, "comptes": 5,
               "empreinte_ok": True, "duree_s": 12.4, "par_table": {"users": 5, "jobs": 30},
               "inattendu": "x" * 50}
    with TestClient(m.app) as client:
        assert _envoyer(client, rapport).status_code == 200
    essai = _dernier_essai()
    assert essai.status == "ok" and essai.meta["tables"] == 25 and essai.meta["par_table"] == {"users": 5, "jobs": 30}
    assert "inattendu" not in essai.meta
    (message,) = site
    assert message.startswith("✅") and "05/11/2026 02:00 UTC" in message and "25 tables" in message


def test_un_essai_ECHOUE_est_garde_et_annonce_avec_sa_raison(site) -> None:
    with TestClient(m.app) as client:
        _envoyer(client, {"ok": False, "sauvegarde": "20261105-020003", "erreur": "RuntimeError: <pg_restore> a échoué"})
    assert _dernier_essai().status == "error"
    (message,) = site
    assert message.startswith("❌") and "&lt;pg_restore&gt;" in message, "texte échappé pour Telegram"


# ── La tâche sur Render ──────────────────────────────────────────────────────────────────────

def test_la_tache_render_est_MENSUELLE_et_n_a_PAS_la_base_de_production() -> None:
    config = yaml.safe_load((REPO_ROOT / "render.yaml").read_text(encoding="utf-8"))
    tache = next(s for s in config["services"] if s["name"] == "noyaru-restauration-test")
    variables = {v["key"]: v for v in tache["envVars"]}
    assert tache["type"] == "cron" and re.fullmatch(r"\d+ \d+ 1 \* \*", tache["schedule"])
    assert "DATABASE_URL" not in variables and not any("fromDatabase" in v for v in variables.values())
    assert tache["dockerfilePath"] == "./Dockerfile.restauration" and (REPO_ROOT / "Dockerfile.restauration").exists()
    for cle in ("CRON_SECRET", "PUBLIC_BASE_URL"):
        assert variables[cle]["fromService"] == {"type": "web", "name": "seo-agent-web", "envVarKey": cle}


def test_le_serveur_de_l_essai_a_la_VERSION_du_client_qui_sauvegarde() -> None:
    """pg_restore ne lit pas une sauvegarde faite par un pg_dump plus récent que lui."""
    client = re.search(r"postgresql-client-(\d+)", (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")).group(1)
    image = (REPO_ROOT / "Dockerfile.restauration").read_text(encoding="utf-8")
    assert re.search(r"install[^\n]*\bpostgresql-(\d+)\b", image).group(1) == client
    assert restauration.PG_BIN.as_posix() == "/usr/lib/postgresql/%s/bin" % client
    assert "seo-agent-web/backend/backup.py" in image and "seo-agent-web/backend/restauration.py" in image
