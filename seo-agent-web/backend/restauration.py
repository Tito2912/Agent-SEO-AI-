"""La sauvegarde de la base se restaure-t-elle vraiment ? Un essai mensuel, sur Render.

Exigé par l'accord de traitement des données (DPA, annexe III : la sauvegarde doit pouvoir
être restaurée). Décision du propriétaire, 05/11/2026 : un essai AUTOMATIQUE, une fois
par mois, plutôt qu'une copie de la production sur un poste de travail.

La tâche `noyaru-restauration-test` (image Dockerfile.restauration) :
  1. trouve dans le bucket le manifeste le plus récent de la sauvegarde quotidienne (backup.py) ;
  2. télécharge la base qu'il décrit et compare son empreinte SHA-256 à celle du manifeste ;
  3. démarre un serveur Postgres JETABLE dans son conteneur — dossier temporaire, socket local,
     aucune écoute réseau — et y restaure la sauvegarde avec pg_restore ;
  4. compte les lignes de chaque table ;
  5. envoie le rapport au site (/cron/sauvegarde-verifiee), qui le garde dans le journal d'audit et
     le transmet sur Telegram.

La tâche ne connaît pas l'adresse de la base de production : elle ne peut rien y écrire. Le
conteneur, et la copie restaurée avec lui, disparaissent à la fin de l'essai.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterator

import requests

try:
    from . import backup
except ImportError:  # `python backend/restauration.py`
    import backup  # type: ignore

# Les binaires du serveur (paquet Debian postgresql-18) ne sont pas dans le PATH.
PG_BIN = Path(os.environ.get("RESTAURATION_PG_BIN", "/usr/lib/postgresql/18/bin"))
UTILISATEUR = "restauration"
BASE = "restauration"
# En dessous, la restauration n'a sans doute pas tout rendu : la base de Noyaru a plus de 40 tables.
TABLES_MIN = 20
COMPTER = (
    "SELECT table_name, (xpath('/row/n/text()', query_to_xml(format('SELECT count(*) AS n FROM %I.%I', "
    "table_schema, table_name), false, true, '')))[1]::text::bigint "
    "FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
    "ORDER BY table_name"
)


def _sha256(chemin: Path) -> str:
    h = hashlib.sha256()
    with chemin.open("rb") as f:
        for bloc in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloc)
    return h.hexdigest()


def dernier_manifeste(client: Any, bucket: str, prefixe: str) -> tuple[str, dict]:
    """Le manifeste le plus récent : son nom porte l'heure (manifest-AAAAMMJJ-HHMMSS.json)."""
    cles: list[str] = []
    jeton = None
    while True:
        args = {"Bucket": bucket, "Prefix": prefixe.rstrip("/") + "/manifest-"}
        if jeton:
            args["ContinuationToken"] = jeton
        page = client.list_objects_v2(**args)
        cles += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json")]
        jeton = page.get("NextContinuationToken")
        if not jeton:
            break
    if not cles:
        raise RuntimeError("aucun manifeste de sauvegarde sous %s" % prefixe)
    cle = max(cles)
    corps = client.get_object(Bucket=bucket, Key=cle)["Body"].read()
    return cle, json.loads(corps)


def _en_root() -> bool:
    return hasattr(os, "geteuid") and os.geteuid() == 0


def _comme_postgres(commande: list[str]) -> list[str]:
    """Postgres refuse de tourner en root, et un conteneur Docker l'est par défaut."""
    if _en_root():
        return ["runuser", "-u", "postgres", "--", *commande]
    return commande


def _lancer(commande: list[str], *, en_postgres: bool = False, **kw: Any) -> subprocess.CompletedProcess:
    r = subprocess.run(_comme_postgres(commande) if en_postgres else commande, capture_output=True, text=True, **kw)
    if r.returncode != 0:
        raise RuntimeError("%s a échoué (code %d) : %s" % (Path(commande[0]).name, r.returncode,
                                                            (r.stderr or r.stdout).strip()[-600:]))
    return r


@contextlib.contextmanager
def serveur_jetable(dossier: Path) -> Iterator[Path]:
    """Un serveur Postgres sans réseau dans `dossier` ; rend le dossier de sa socket."""
    donnees, socket = dossier / "donnees", dossier / "socket"
    socket.mkdir(parents=True)
    if _en_root():
        shutil.chown(dossier, "postgres")
        shutil.chown(socket, "postgres")
    _lancer([str(PG_BIN / "initdb"), "-D", str(donnees), "-U", UTILISATEUR, "--auth=trust", "-E", "UTF8",
             "--no-instructions"], en_postgres=True)
    _lancer([str(PG_BIN / "pg_ctl"), "-D", str(donnees), "-w", "-t", "60", "-l", str(dossier / "journal"),
             "-o", "-c listen_addresses='' -k %s" % socket, "start"], en_postgres=True)
    try:
        yield socket
    finally:
        subprocess.run(_comme_postgres([str(PG_BIN / "pg_ctl"), "-D", str(donnees), "-m", "fast", "stop"]),
                       capture_output=True, text=True)


def restaurer_et_compter(sauvegarde: Path, socket: Path) -> dict[str, int]:
    connexion = ["-h", str(socket), "-U", UTILISATEUR]
    _lancer([str(PG_BIN / "createdb"), *connexion, BASE])
    _lancer([str(PG_BIN / "pg_restore"), *connexion, "-d", BASE, "--no-owner", "--no-acl", "--exit-on-error",
             str(sauvegarde)])
    r = _lancer([str(PG_BIN / "psql"), *connexion, "-d", BASE, "-At", "-F", "\t", "-c", COMPTER])
    lignes: dict[str, int] = {}
    for ligne in r.stdout.splitlines():
        table, _, n = ligne.partition("\t")
        if table:
            lignes[table] = int(n or 0)
    return lignes


def verifier(client: Any, bucket: str, prefixe: str, dossier: Path) -> dict[str, Any]:
    debut = time.monotonic()
    rapport: dict[str, Any] = {"ok": False}
    try:
        if _en_root():
            # Le dossier temporaire naît en 700, à root : postgres doit pouvoir y entrer.
            shutil.chown(dossier, "postgres")
        _cle_manifeste, manifeste = dernier_manifeste(client, bucket, prefixe)
        base = next(f for f in manifeste.get("files", []) if f.get("type") == "postgres_dump_custom")
        rapport.update(sauvegarde=manifeste.get("ts"), fichier=base["key"], octets=base.get("bytes"))
        chemin = dossier / "sauvegarde.dump"
        client.download_file(bucket, base["key"], str(chemin))
        empreinte = _sha256(chemin)
        rapport["empreinte_ok"] = empreinte == base.get("sha256")
        if not rapport["empreinte_ok"]:
            raise RuntimeError("empreinte différente de celle du manifeste")
        if _en_root():
            shutil.chown(chemin, "postgres")
        with serveur_jetable(dossier / "serveur") as socket:
            lignes = restaurer_et_compter(chemin, socket)
        rapport.update(tables=len(lignes), lignes=sum(lignes.values()), comptes=lignes.get("users", 0),
                       par_table=lignes)
        manques = []
        if len(lignes) < TABLES_MIN:
            manques.append("%d tables seulement" % len(lignes))
        if not lignes.get("users"):
            manques.append("aucun compte")
        if "alembic_version" not in lignes:
            manques.append("pas de version de schéma")
        if manques:
            raise RuntimeError("restauration incomplète : " + ", ".join(manques))
        rapport["ok"] = True
    except Exception as e:  # noqa: BLE001 — le rapport dit ce qui a échoué ; la tâche sort en erreur
        rapport["erreur"] = "%s: %s" % (type(e).__name__, str(e)[:600])
    rapport["duree_s"] = round(time.monotonic() - debut, 1)
    return rapport


def envoyer_au_site(rapport: dict[str, Any]) -> str:
    base = str(os.environ.get("PUBLIC_BASE_URL") or "").rstrip("/")
    secret = str(os.environ.get("CRON_SECRET") or "").strip()
    if not base or not secret:
        return "PUBLIC_BASE_URL ou CRON_SECRET absent : rapport non transmis"
    try:
        r = requests.post(base + "/cron/sauvegarde-verifiee", json=rapport, timeout=30,
                          headers={"Authorization": "Bearer " + secret})
        return "" if r.status_code == 200 else "le site répond %d" % r.status_code
    except Exception as e:  # noqa: BLE001
        return "site injoignable (%s)" % type(e).__name__


def main() -> int:
    bucket = str(os.environ.get("S3_BUCKET_NAME") or "").strip()
    if not bucket:
        print("[RESTAURATION] S3_BUCKET_NAME manquant", file=sys.stderr)
        return 2
    prefixe = "%s/%s" % (backup._backup_s3_prefix(), backup._backup_env_slug())
    with tempfile.TemporaryDirectory(prefix="restauration-") as tmp:
        rapport = verifier(backup._s3_client(), bucket, prefixe, Path(tmp))
    resume = {k: v for k, v in rapport.items() if k != "par_table"}
    print("[RESTAURATION] " + json.dumps(resume, ensure_ascii=False), flush=True)
    probleme = envoyer_au_site(rapport)
    if probleme:
        print("[RESTAURATION] " + probleme, flush=True)
    return 0 if rapport["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
