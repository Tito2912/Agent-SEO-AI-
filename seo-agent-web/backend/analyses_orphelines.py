"""Les analyses orphelines — des dossiers d'analyse dont le projet ou le compte n'existe plus — et
leur effacement.

Jusqu'au 05/11/2026, supprimer un projet laissait ses analyses sur le disque et dans S3, et
supprimer un compte ne vidait S3 que si le disque du web en avait une copie (corrigé le même jour,
voir `_effacer_les_analyses` dans app.py). Cet écran rattrape ce qui est resté avant. Constat du
propriétaire : son export « Mes données » rendait 11 dossiers d'analyse pour 10 projets.

Seul est candidat un dossier rangé comme l'application les range : `<compte>/` dont le nom est un
identifiant de compte (minuscules) absent de la base, ou `<compte>/<projet>/` qu'aucun projet de ce
compte — ni d'un compte dont il est MEMBRE — ne porte. Les dossiers hérités (sans compte), les
dossiers techniques (« _… », « .… ») et les fichiers ne sont jamais candidats.

Comme pour les comptes jamais utilisés : l'écran ne coche rien d'office, et l'inventaire est REFAIT
au moment d'effacer, jamais repris de la page affichée plus tôt. Un dossier redevenu vivant entre-temps
(projet recréé sous le même nom) est gardé.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Callable, Iterable

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

try:
    from .models import AccountMember, Project, User
except ImportError:  # pragma: no cover - execution depuis backend/
    from models import AccountMember, Project, User  # type: ignore

logger = logging.getLogger(__name__)
router = APIRouter()
PAGE = "/settings/operations/analyses-orphelines"
# Un identifiant de compte tel que l'application nomme le dossier : str(uuid4()), en minuscules.
IDENTIFIANT = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

ListerS3 = Callable[[Path], Iterable[str]]


def _sous_dossiers(dossier: Path) -> set[str]:
    try:
        return {p.name for p in dossier.iterdir() if p.is_dir()}
    except OSError:
        return set()


def _enfants(dossier: Path, lister_s3: ListerS3) -> dict[str, str]:
    """Les sous-dossiers de `dossier` et où ils vivent : « disque », « S3 » ou « disque + S3 »."""
    disque, s3 = _sous_dossiers(dossier), set(lister_s3(dossier))
    return {nom: " + ".join(o for o, ici in (("disque", nom in disque), ("S3", nom in s3)) if ici)
            for nom in disque | s3}


def inventaire(db: Any, racine: Path, lister_s3: ListerS3) -> dict[str, Any]:
    comptes = {str(i): str(e or "") for i, e in db.execute(select(User.id, User.email))}
    if not comptes:
        # Une base vide (ou mal lue) ferait de CHAQUE dossier un orphelin.
        raise RuntimeError("aucun compte lu en base : inventaire refusé")
    projets: dict[str, set[str]] = {}
    for proprietaire, slug in db.execute(select(Project.owner_user_id, Project.slug)):
        projets.setdefault(str(proprietaire), set()).add(str(slug))
    accessibles = {uid: set(projets.get(uid, ())) for uid in comptes}
    for proprietaire, membre in db.execute(select(AccountMember.owner_user_id, AccountMember.member_user_id)):
        accessibles.setdefault(str(membre), set()).update(projets.get(str(proprietaire), ()))

    orphelins: list[dict[str, str]] = []
    ignores = 0
    for nom, ou in sorted(_enfants(racine, lister_s3).items()):
        if not IDENTIFIANT.match(nom):
            ignores += 1
            continue
        if nom not in comptes:
            orphelins.append({"chemin": nom, "raison": "compte supprimé", "compte": "", "ou": ou})
            continue
        for enfant, ou_enfant in sorted(_enfants(racine / nom, lister_s3).items()):
            if enfant.startswith(("_", ".")) or enfant in accessibles.get(nom, ()):
                continue
            orphelins.append({"chemin": f"{nom}/{enfant}", "raison": "projet supprimé",
                              "compte": comptes[nom], "ou": ou_enfant})
    return {"orphelins": orphelins, "ignores": ignores}


def supprimer(db: Any, racine: Path, chemins: Iterable[str], lister_s3: ListerS3,
              effacer: Callable[[Path, str], bool]) -> tuple[list[str], list[str]]:
    """Efface les `chemins` qui sont ENCORE orphelins à cet instant ; rend (effacés, gardés)."""
    encore = {o["chemin"] for o in inventaire(db, racine, lister_s3)["orphelins"]}
    effaces: list[str] = []
    gardes: list[str] = []
    for chemin in dict.fromkeys(str(c) for c in chemins):
        parent, _, nom = chemin.rpartition("/")
        if chemin in encore and effacer(racine / parent if parent else racine, nom):
            effaces.append(chemin)
        else:
            gardes.append(chemin)
    return effaces, gardes


def module():
    if __package__:
        from . import app
    else:  # pragma: no cover
        import app
    return app


def _lister_s3(m: Any) -> ListerS3:
    return lambda dossier: m.object_store.list_runs_subdirs(m.DEFAULT_RUNS_DIR, dossier)


@router.get("/settings/operations/analyses-orphelines")
def page_analyses_orphelines(request: Request, msg: str = "", err: str = ""):
    m = module()
    m._require_system_owner(request)
    resultat: dict[str, Any] = {"orphelins": [], "ignores": 0}
    try:
        with m.DB.session() as db:
            resultat = inventaire(db, m.DEFAULT_RUNS_DIR, _lister_s3(m))
    except Exception as e:
        logger.error("[ORPHELINS] inventaire : %s: %s", type(e).__name__, e)
        err = err or "Inventaire impossible (%s). Rien n’est proposé à l’effacement." % type(e).__name__
    return m.templates.TemplateResponse("settings_analyses_orphelines.html", {
        "request": request, "project": None, "msg": msg, "err": err, **resultat,
    }, headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})


@router.post("/settings/operations/analyses-orphelines/supprimer")
async def supprimer_analyses_orphelines(request: Request):
    m = module()
    m._require_system_owner(request)
    formulaire = await request.form()
    if str(formulaire.get("confirmation") or "").strip().upper() != "SUPPRIMER":
        return RedirectResponse(m._path_with_flash(PAGE, err="Tapez SUPPRIMER pour confirmer."), status_code=303)
    chemins = [str(v) for v in formulaire.getlist("chemin")]
    if not chemins:
        return RedirectResponse(m._path_with_flash(PAGE, err="Aucun dossier coché."), status_code=303)
    try:
        with m.DB.session() as db:
            effaces, gardes = supprimer(db, m.DEFAULT_RUNS_DIR, chemins, _lister_s3(m), m._effacer_les_analyses)
    except Exception as e:
        logger.error("[ORPHELINS] effacement : %s: %s", type(e).__name__, e)
        return RedirectResponse(m._path_with_flash(
            PAGE, err="Inventaire impossible (%s) : rien n’a été effacé." % type(e).__name__), status_code=303)
    m._audit_log(request, action="runs.purge_orphans", status="ok", target_type="runs",
                 meta={"effaces": effaces[:200], "gardes": gardes[:50]})
    message = "%d dossier%s effacé%s, sur le disque et dans S3." % (
        len(effaces), "s" if len(effaces) > 1 else "", "s" if len(effaces) > 1 else "")
    if gardes:
        message += " %d gardé%s : plus orphelin%s à cet instant (%s)." % (
            len(gardes), "s" if len(gardes) > 1 else "", "s" if len(gardes) > 1 else "", " ; ".join(gardes[:5]))
    return RedirectResponse(m._path_with_flash(PAGE, msg=message), status_code=303)
