# -*- coding: utf-8 -*-
"""Une unite de correction debitee doit dire POURQUOI, SUR QUOI et PAR QUI.

CE QUI A MANQUE LE 19/09/2026. Huit unites ont ete debitees a tort sur une pull request —
l'absence du drapeau `deterministic` etait lue comme « le modele a ecrit », donc huit fichiers
reecrits sans un seul appel au modele ont ete factures. Le defaut d'attribution est corrige
depuis. Restait a rembourser, et la se posait une question qu'on ne pouvait pas trancher :
LESQUELLES des lignes de consommation du mois etaient ces huit-la ?

`usage_add` etait appele sans `meta`. Une ligne portait un montant et une date, rien d'autre —
ni projet, ni anomalie, ni auteur. Identifier un debit a rembourser revenait a lire des heures
et a deviner.

TROIS CHAMPS, ET LE TROISIEME N'EST PAS REDONDANT :

    slug    le projet, donc ce que le client reconnaitra sur sa facture
    motif   la famille d'anomalie, ou `bulk` / `keyword_rewrite`
    par     QUI a clique — depuis les comptes d'equipe, le compte qui PAYE est l'hote et la
            personne qui declenche peut etre un membre. Les deux se posent la question un
            jour, et la reponse n'est deductible d'aucune autre colonne.

Ce fichier ne rembourse rien : un remboursement est une ecriture en base de production, elle
se fait les yeux dessus. Il rend le remboursement POSSIBLE.
"""

from __future__ import annotations

import ast
import os
import sys
import tempfile
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-debit-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from sqlalchemy import select  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth  # noqa: E402
from backend.models import AccountMember, Project, User, UsageEvent  # noqa: E402


class _Personne:
    def __init__(self, uid: str, admin: bool = False) -> None:
        self.id = uid
        self.is_admin = admin


def _utilisateur(prefixe: str) -> str:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="%s-%s@exemple.fr" % (prefixe, uuid.uuid4().hex[:8]),
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        return str(u.id)


def _projet(owner_id: str) -> str:
    s = "site-%s" % uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        db.add(Project(owner_user_id=owner_id, slug=s, site_name=s,
                       base_url="https://%s.fr/" % s))
        db.commit()
    return s


def _evenements(user_id: str) -> list[UsageEvent]:
    with m.DB.session() as db:
        return list(db.scalars(
            select(UsageEvent).where(UsageEvent.user_id == user_id,
                                     UsageEvent.metric == "ai_corrections_month")))


# --- la provenance ----------------------------------------------------------------------------

def test_un_debit_porte_le_projet_le_motif_et_l_auteur() -> None:
    hote = _utilisateur("hote")
    slug = _projet(hote)
    m._correction_charge(_Personne(hote), 3, slug=slug, motif="open_graph_url_not_matching_canonical")

    evs = _evenements(hote)
    assert len(evs) == 1 and evs[0].amount == 3
    meta = evs[0].meta or {}
    assert meta.get("slug") == slug, meta
    assert meta.get("motif") == "open_graph_url_not_matching_canonical", meta
    assert meta.get("par") == hote, meta


def test_le_PAYEUR_et_L_AUTEUR_sont_distincts_et_tous_deux_gardes() -> None:
    """Le champ qui n'est deductible d'aucun autre.

    Un membre declenche une correction sur un projet de son hote : la ligne est portee par
    l'hote (c'est lui qui paie) mais c'est le membre qui a clique. Sans `par`, la facture de
    l'agence liste des unites que personne ne peut rattacher a une action.
    """
    hote, membre = _utilisateur("hote"), _utilisateur("membre")
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=hote, member_user_id=membre))
        db.commit()
    slug = _projet(hote)

    m._correction_charge(_Personne(membre), 2, slug=slug, motif="title_too_short")

    assert not _evenements(membre), "la ligne a été portée par le membre au lieu de l'hôte"
    evs = _evenements(hote)
    assert len(evs) == 1 and evs[0].amount == 2
    assert (evs[0].meta or {}).get("par") == membre, (
        "on ne sait plus qui a déclenché la dépense : %r" % (evs[0].meta,))


def test_un_administrateur_n_est_toujours_pas_facture() -> None:
    admin = _utilisateur("admin")
    slug = _projet(admin)
    m._correction_charge(_Personne(admin, admin=True), 5, slug=slug, motif="x")
    assert not _evenements(admin)


def test_un_compte_a_zero_n_ecrit_aucune_ligne() -> None:
    """Une ligne à zéro polluerait l'historique sans rien représenter."""
    hote = _utilisateur("hote")
    m._correction_charge(_Personne(hote), 0, slug=_projet(hote), motif="x")
    assert not _evenements(hote)


# --- l'enumeration -----------------------------------------------------------------------------

def test_TOUTE_facturation_de_correction_dit_son_motif() -> None:
    """Le garde-fou qui couvre la sixieme route, celle qu'on branchera demain.

    Les cinq appels d'aujourd'hui sont verrouilles par les tests ci-dessus. Celui-ci relit le
    source et refuse un debit anonyme, ou qu'il apparaisse — c'est la lecon repetee de la
    journee : corriger les sites qu'on connait et rater le suivant ne se voit pas.
    """
    arbre = ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    fautes: list[str] = []
    for n in ast.walk(arbre):
        if not isinstance(n, ast.Call):
            continue
        if getattr(n.func, "id", "") != "_correction_charge":
            continue
        if not any(k.arg == "motif" for k in n.keywords):
            fautes.append("ligne %d : %s" % (n.lineno, ast.unparse(n)))
    assert not fautes, (
        "ces débits ne disent pas pourquoi ils ont lieu :\n  " + "\n  ".join(fautes))


def test_l_auditeur_trouve_ENCORE_des_debits_a_surveiller() -> None:
    """Une énumération qui ne mesure plus rien passe au vert en silence."""
    arbre = ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    appels = [n for n in ast.walk(arbre)
              if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "_correction_charge"]
    assert len(appels) >= 5, "l'auditeur ne voit plus que %d débit(s)" % len(appels)
