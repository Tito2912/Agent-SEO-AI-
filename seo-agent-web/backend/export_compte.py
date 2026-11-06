"""Toutes les données d'un compte, dans un ZIP que son titulaire télécharge lui-même.

Décision du propriétaire, 05/11/2026 : un bouton « Télécharger toutes mes données » dans « Mon
compte ». Il tient deux promesses d'un coup : la RESTITUTION de l'accord de traitement des données
(DPA, clause B5 : remettre au client, à son départ, ce que les rapports et exports ne couvrent
pas, dans un format structuré et couramment utilisé) et la PORTABILITÉ du RGPD (article 20).

Le contenu se DÉDUIT DU SCHÉMA : chaque table de la base, en suivant ses clés étrangères depuis le
compte (ses projets, puis ce qui dépend d'eux, et ainsi de suite). Une table ajoutée plus tard
part d'elle-même dans l'export ; aucune liste à tenir à jour.

Deux règles protègent ce qui ne doit pas sortir :
  * aucun secret — colonnes dont le nom désigne un mot de passe, un jeton, une empreinte, un
    secret ; valeurs chiffrées (« enc:… ») ; mêmes noms à l'intérieur des champs JSON ;
  * rien d'un AUTRE compte — une table rattachée à un projet ne sort que par les projets du
    compte, jamais par une colonne « créé par » qui désignerait un membre d'équipe travaillant
    sur le projet de quelqu'un d'autre.
"""
from __future__ import annotations

import csv
import datetime as dt
import decimal
import io
import json
import re
import shutil
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from sqlalchemy import or_, select

try:
    from .models import Base
except ImportError:  # `uvicorn app:app` depuis backend/
    from models import Base  # type: ignore

# Pas « key » seul : `issue_key` (l'anomalie) ou `key` (le type d'une connexion) sont des données.
SECRET = re.compile(r"(^|_)(password|passwd|secret|token|hash|credentials?|api_?key|private_?key|access_?key)(_|$)",
                    re.IGNORECASE)
CHIFFRE = "enc:"
MASQUE = "[secret retiré]"
# Des tables entières de secrets, ou sans lien avec un compte.
TABLES_EXCLUES = {"password_reset_tokens", "email_verification_tokens", "rate_limit_buckets"}
# Les fichiers d'analyse, les plus récents d'abord, jusqu'à ce plafond et dans ce délai ; le
# reste est listé dans le manifeste (et remis sur demande).
PLAFOND_ANALYSES = 300 * 1024 * 1024
DELAI_ANALYSES_S = 60.0


def nettoyer(valeur: Any) -> Any:
    """Une valeur exportable : sans secret, en types JSON simples."""
    if isinstance(valeur, dict):
        return {str(cle): (MASQUE if SECRET.search(str(cle)) else nettoyer(v)) for cle, v in valeur.items()}
    if isinstance(valeur, (list, tuple, set)):
        return [nettoyer(v) for v in valeur]
    if isinstance(valeur, str):
        return MASQUE if valeur.startswith(CHIFFRE) else valeur
    if isinstance(valeur, (dt.datetime, dt.date, dt.time)):
        return valeur.isoformat()
    if isinstance(valeur, decimal.Decimal):
        return str(valeur)
    if isinstance(valeur, (bytes, bytearray, memoryview)):
        return MASQUE
    return valeur


def lignes_du_compte(session: Any, user_id: str, *, metadata: Any = None) -> dict[str, list[dict[str, Any]]]:
    """{table: [lignes]} — tout ce que la base rattache au compte, secrets retirés."""
    metadata = metadata if metadata is not None else Base.metadata
    inclus: dict[str, set[Any]] = {}
    sortie: dict[str, list[dict[str, Any]]] = {}
    for table in metadata.sorted_tables:
        if table.name in TABLES_EXCLUES:
            continue
        if table.name == "users":
            condition = table.c.id == user_id
        else:
            liens = [fk for fk in table.foreign_keys if fk.column.name == "id" and inclus.get(fk.column.table.name)]
            par_projet = [fk for fk in liens if fk.column.table.name == "projects"]
            # Rattachée à un projet : par les projets du compte, et par eux seuls.
            liens = par_projet or liens
            if not liens:
                continue
            condition = or_(*[fk.parent.in_(inclus[fk.column.table.name]) for fk in liens])
        rangees = [dict(r) for r in session.execute(select(table).where(condition)).mappings().all()]
        if not rangees:
            continue
        if "id" in table.c:
            inclus[table.name] = {r["id"] for r in rangees}
        sortie[table.name] = [{col: nettoyer(v) for col, v in r.items() if not SECRET.search(col)} for r in rangees]
    return sortie


def _csv(lignes: list[dict[str, Any]]) -> str:
    colonnes: list[str] = []
    for ligne in lignes:
        colonnes += [c for c in ligne if c not in colonnes]
    tampon = io.StringIO()
    ecrivain = csv.DictWriter(tampon, fieldnames=colonnes, extrasaction="ignore")
    ecrivain.writeheader()
    for ligne in lignes:
        ecrivain.writerow({c: (json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
                           for c, v in ligne.items()})
    return tampon.getvalue()


@dataclass
class Fichier:
    """Un fichier d'analyse, sur le disque ou dans S3 : il ne s'ouvre que s'il entre dans l'export.

    Premier passage en production (05/11/2026) : l'export ramenait TOUTES les analyses du compte
    depuis S3 sur le disque du site avant d'en garder 300 Mo ; sur un compte chargé, la requête
    tournait encore sans fin. Désormais, seuls les fichiers retenus sont lus, en flux."""
    nom: str
    taille: int
    date: float
    ouvrir: Callable[[], Any]


def fichiers_locaux(dossiers: Iterable[Path], racine: Path) -> list[Fichier]:
    trouves: list[Fichier] = []
    for dossier in dossiers:
        if not dossier.is_dir():
            continue
        for chemin in dossier.rglob("*"):
            if chemin.is_file() and not chemin.is_symlink():
                try:
                    relatif = chemin.resolve().relative_to(racine.resolve()).as_posix()
                except ValueError:
                    continue
                etat = chemin.stat()
                trouves.append(Fichier("analyses/" + relatif, etat.st_size, etat.st_mtime,
                                       lambda chemin=chemin: chemin.open("rb")))
    return trouves


LISEZMOI = """Toutes les données de votre compte Noyaru
Export du {date} (UTC) pour {email}.

donnees/   une table par fichier, en JSON et en CSV : votre compte, vos projets et tout ce qui
           en dépend (analyses, anomalies, corrections, contenus, mots-clés, concurrents,
           connexions, abonnement, usage, journal de vos actions).
analyses/  les fichiers produits par vos analyses de sites, les plus récents d'abord.
manifeste.json  ce que contient l'export, table par table, et les fichiers d'analyse laissés de
           côté s'ils dépassaient {plafond} Mo.

Ne figurent pas dans cet export : vos mots de passe, jetons, clés et autres secrets (ils restent
chiffrés chez nous et ne vous sont pas utiles hors de Noyaru), ni les données des autres comptes.
Une question : contact@noyaru.com.
"""
README = """All the data of your Noyaru account
Export of {date} (UTC) for {email}.

donnees/   one table per file, in JSON and CSV: your account, your projects and everything that
           depends on them (analyses, issues, corrections, content, keywords, competitors,
           connections, subscription, usage, log of your actions).
analyses/  the files produced by your site analyses, most recent first.
manifeste.json  what the export contains, table by table, and the analysis files left out if
           they went beyond {plafond} MB.

Not included: your passwords, tokens, keys and other secrets (they stay encrypted with us and
are of no use outside Noyaru), nor the data of other accounts.
Questions: contact@noyaru.com.
"""


def construire(session: Any, user: Any, destination: Path, *, fichiers: Iterable[Fichier],
               plafond: int = PLAFOND_ANALYSES, delai_s: float = DELAI_ANALYSES_S, langue: str = "fr",
               maintenant: Callable[[], dt.datetime] | None = None,
               horloge: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    """Écrit le ZIP dans `destination` ; rend le manifeste."""
    instant = (maintenant or (lambda: dt.datetime.now(dt.timezone.utc)))()
    donnees = lignes_du_compte(session, str(user.id))
    manifeste: dict[str, Any] = {"genere_le": instant.isoformat(), "compte": str(user.id),
                                 "tables": {t: len(l) for t, l in donnees.items()},
                                 "analyses": {"fichiers": 0, "octets": 0, "laisses_de_cote": [],
                                              "delai_atteint": False}}
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for table, lignes in donnees.items():
            archive.writestr("donnees/%s.json" % table, json.dumps(lignes, ensure_ascii=False, indent=2))
            archive.writestr("donnees/%s.csv" % table, _csv(lignes))
        # Le plus récent d'abord ; un même nom (disque et S3) ne part qu'une fois.
        retenus: dict[str, Fichier] = {}
        for fichier in sorted(fichiers, key=lambda f: f.date, reverse=True):
            retenus.setdefault(fichier.nom, fichier)
        total, debut = 0, horloge()
        for fichier in retenus.values():
            if total + fichier.taille > plafond:
                manifeste["analyses"]["laisses_de_cote"].append(fichier.nom)
                continue
            if horloge() - debut > delai_s:
                manifeste["analyses"]["delai_atteint"] = True
                manifeste["analyses"]["laisses_de_cote"].append(fichier.nom)
                continue
            try:
                with fichier.ouvrir() as source, archive.open(fichier.nom, "w") as cible:
                    shutil.copyfileobj(source, cible, 1024 * 1024)
            except Exception:  # noqa: BLE001 — un fichier illisible n'empêche pas l'export
                manifeste["analyses"]["laisses_de_cote"].append(fichier.nom)
                continue
            total += fichier.taille
            manifeste["analyses"]["fichiers"] += 1
        manifeste["analyses"]["octets"] = total
        archive.writestr("manifeste.json", json.dumps(manifeste, ensure_ascii=False, indent=2))
        nom, texte = ("README.txt", README) if langue == "en" else ("LISEZMOI.txt", LISEZMOI)
        archive.writestr(nom, texte.format(date=instant.strftime("%d/%m/%Y %H:%M"),
                                           email=str(getattr(user, "email", "") or ""),
                                           plafond=plafond // (1024 * 1024)))
    return manifeste
