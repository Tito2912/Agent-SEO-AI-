"""L'acceptation en ligne de l'accord de traitement des données (DPA, clause complémentaire B1). 05/11/2026.

La clause B1 du DPA : accepté de façon expresse, en ligne, par une personne habilitée à engager le
Client, qui coche une case et DÉCLARE ce pouvoir ; l'acceptation identifie l'organisation
(dénomination, immatriculation quand elle existe) ; E-Com Shop garde l'organisation, le compte et
la personne, la date et l'heure, la version EXACTE du texte et la trace de l'action, pendant
l'accord puis cinq ans ; une nouvelle version s'accepte de nouveau.

Rien de tout cela n'apparaît tant que `legal.DPA_VERSION` vaut None : le texte attend la
décision de le publier (docs/legal/LEGAL-VALIDATION-REQUIRED.md).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import re
from pathlib import Path
from typing import Any

from sqlalchemy import delete, select, update

try:
    from . import legal
    from .models import DpaAcceptance
except ImportError:  # `uvicorn app:app` depuis backend/
    import legal  # type: ignore
    from models import DpaAcceptance  # type: ignore

import markdown as _markdown

TEXTES = Path(__file__).resolve().parents[1] / "docs" / "legal"
CONSERVATION_ANS = 5
# (minimum, maximum) de caractères ; l'immatriculation est facultative (une association n'en a pas).
CHAMPS = {"organisation": (2, 200), "signataire": (2, 120), "fonction": (2, 120), "immatriculation": (0, 40)}
IMMATRICULATION = re.compile(r"[A-Za-z0-9 .\-/]*")
ERREURS = {
    "organisation": "Indiquez la dénomination de votre organisation.",
    "signataire": "Indiquez votre nom.",
    "fonction": "Indiquez votre fonction dans l’organisation.",
    "immatriculation": "Le numéro d’immatriculation ne peut contenir que des lettres, des chiffres et des espaces.",
    "habilite": "Cochez la case : seule une personne habilitée à engager l’organisation peut accepter l’accord.",
}


def version() -> str | None:
    return getattr(legal, "DPA_VERSION", None)


def texte(langue: str) -> tuple[str, str]:
    """Le texte proposé, et l'empreinte SHA-256 de ses octets exacts."""
    brut = (TEXTES / ("DPA-en.md" if langue == "en" else "DPA-fr.md")).read_bytes()
    return brut.decode("utf-8"), hashlib.sha256(brut).hexdigest()


def html(langue: str) -> str:
    return _markdown.markdown(texte(langue)[0], extensions=["tables"])


def en_vigueur(db: Any, user_id: str) -> DpaAcceptance | None:
    """La dernière acceptation du compte, si l'accord n'a pas pris fin."""
    return db.scalars(select(DpaAcceptance).where(DpaAcceptance.user_id == str(user_id),
                                                  DpaAcceptance.termine_le.is_(None))
                      .order_by(DpaAcceptance.accepte_le.desc())).first()


def a_jour(acceptation: DpaAcceptance | None) -> bool:
    return acceptation is not None and acceptation.version == version()


def lire_formulaire(formulaire: dict[str, Any]) -> tuple[dict[str, str], str]:
    """(valeurs nettoyées, première erreur ou "")."""
    valeurs = {nom: " ".join(str(formulaire.get(nom) or "").split()) for nom in CHAMPS}
    for nom, (minimum, maximum) in CHAMPS.items():
        if not minimum <= len(valeurs[nom]) <= maximum:
            return valeurs, ERREURS[nom]
    if not IMMATRICULATION.fullmatch(valeurs["immatriculation"]):
        return valeurs, ERREURS["immatriculation"]
    if str(formulaire.get("habilite") or "") != "oui":
        return valeurs, ERREURS["habilite"]
    return valeurs, ""


def accepter(db: Any, user: Any, valeurs: dict[str, str], *, langue: str, ip: str = "",
             user_agent: str = "") -> DpaAcceptance:
    acceptation = DpaAcceptance(
        user_id=str(user.id), email=str(user.email), organisation=valeurs["organisation"],
        immatriculation=valeurs["immatriculation"] or None, signataire=valeurs["signataire"],
        fonction=valeurs["fonction"], version=str(version()), langue="en" if langue == "en" else "fr",
        texte_sha256=texte(langue)[1], ip_address=(ip or None) and ip[:64],
        user_agent=(user_agent or None) and user_agent[:512],
        # À la microseconde : l'heure de la base (à la seconde) ne départagerait pas deux versions
        # acceptées dans la même seconde, et la plus récente doit gagner.
        accepte_le=dt.datetime.now(dt.timezone.utc))
    db.add(acceptation)
    db.commit()
    return acceptation


def terminer(db: Any, user_id: str, quand: dt.datetime) -> int:
    """La fin de l'accord, à la suppression du compte : les preuves restent, datées de cette fin
    et détachées du compte (explicitement : SQLite n'applique pas le ON DELETE SET NULL)."""
    resultat = db.execute(update(DpaAcceptance).where(DpaAcceptance.user_id == str(user_id))
                          .values(termine_le=quand, user_id=None))
    return int(resultat.rowcount or 0)


def limite_de_conservation(maintenant: dt.datetime) -> dt.datetime:
    try:
        return maintenant.replace(year=maintenant.year - CONSERVATION_ANS)
    except ValueError:  # un 29 février
        return maintenant.replace(year=maintenant.year - CONSERVATION_ANS, day=28)


def purger(db: Any, maintenant: dt.datetime) -> int:
    """Efface les preuves dont l'accord a pris fin il y a plus de cinq ans."""
    resultat = db.execute(delete(DpaAcceptance).where(DpaAcceptance.termine_le.is_not(None),
                                                      DpaAcceptance.termine_le < limite_de_conservation(maintenant)))
    db.commit()
    return int(resultat.rowcount or 0)
