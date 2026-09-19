# -*- coding: utf-8 -*-
"""Un membre d'equipe ouvre les projets du compte qui l'accueille, et rien d'autre.

Premiere pierre des comptes d'equipe. Aujourd'hui un projet appartient a UN utilisateur
(`Project.owner_user_id`), si bien qu'une agence de trois consultants ne peut pas travailler a
plusieurs : il n'existe aucune notion de membre dans le modele. C'est ce qui ferme le segment
des agences, alors que les plans montent jusqu'a trente projets que personne ne peut partager.

CE QUI REND LE CHANTIER ABORDABLE : les cinquante-six routes qui manipulent un projet passent
toutes par `_db_project_or_404` -> `_db_project`. La question « ce projet est-il a moi ? » n'est
donc posee qu'a UN endroit, et c'est celui-la qu'on elargit. Ces tests visent cette fonction et
une route reelle, pour verifier que l'elargissement traverse bien jusqu'a l'interface.

UNE SEULE ADHESION PAR PERSONNE, et c'est une contrainte de schema, pas une preference. Les slugs
sont uniques PAR PROPRIETAIRE (`uq_projects_owner_slug`) : un membre de deux comptes possedant
chacun `mon-site` rendrait `/projects/mon-site` ambigu. On refuse la seconde adhesion plutot que
de laisser la resolution deviner.

CE QUE CETTE ETAPE NE FAIT PAS ENCORE, et qu'il ne faut pas lire comme un oubli : les crawls sont
ranges sur disque par utilisateur (`RUNS_DIR / user_id`), donc un membre voit le projet mais pas
encore ses rapports. C'est l'etape suivante, et elle est mesuree : vingt-huit appels a rerouter.
Rien n'est expose a un client tant qu'il n'y a pas d'ecran d'invitation.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-equipe-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy.exc import IntegrityError  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import AccountMember, Project, User  # noqa: E402


def _utilisateur(prefixe: str) -> str:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="%s-%s@exemple.fr" % (prefixe, uuid.uuid4().hex[:8]),
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        return str(u.id)


def _projet(owner_id: str, slug: str | None = None) -> str:
    s = slug or "site-%s" % uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        p = Project(owner_user_id=owner_id, slug=s, site_name="site.fr",
                    base_url="https://site.fr/")
        db.add(p)
        db.commit()
    return s


def _rejoint(owner_id: str, member_id: str) -> None:
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=owner_id, member_user_id=member_id))
        db.commit()


def _client(user_id: str) -> TestClient:
    c = TestClient(app)
    c.cookies.set(auth.SESSION_COOKIE_NAME,
                  auth.make_session_token(user_id=user_id, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return c


# --- ce que l'adhesion ouvre -----------------------------------------------------------------

def test_un_membre_ouvre_les_projets_du_compte_qui_l_accueille() -> None:
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron)
    assert m._db_project(consultant, slug) is None, "avant l'adhesion, rien n'est visible"
    _rejoint(patron, consultant)
    trouve = m._db_project(consultant, slug)
    assert trouve is not None and trouve.slug == slug


def test_l_acces_traverse_jusqu_a_une_VRAIE_route() -> None:
    """La fonction seule ne prouve rien : c'est l'interface que le consultant utilise.

    La route visee est la page PROJET et non `/issues`, et la nuance a failli me faire conclure
    de travers. `/issues` rend 404 sans crawl — y compris pour le proprietaire, verifie — parce
    qu'elle lit un rapport sur disque. Un test qui l'aurait prise pour cible aurait mesure
    l'absence de crawl en croyant mesurer l'acces, et m'aurait fait « reparer » quelque chose qui
    n'etait pas casse.
    """
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron)
    avant = _client(consultant).get("/projects/%s" % slug, follow_redirects=False)
    assert avant.status_code == 404, avant.status_code
    _rejoint(patron, consultant)
    apres = _client(consultant).get("/projects/%s" % slug, follow_redirects=False)
    assert apres.status_code == 200, "le membre ne voit toujours pas le projet"


# --- ce que l'adhesion n'ouvre PAS -------------------------------------------------------------

def test_un_INCONNU_ne_voit_toujours_rien() -> None:
    """Sans ce bord, un elargissement trop large passerait pour une fonctionnalite."""
    patron, etranger = _utilisateur("agence"), _utilisateur("etranger")
    slug = _projet(patron)
    _rejoint(patron, _utilisateur("consultant"))      # quelqu'un d'AUTRE a rejoint
    assert m._db_project(etranger, slug) is None


def test_l_adhesion_ne_marche_que_dans_UN_sens() -> None:
    """Le consultant entre chez l'agence ; l'agence n'entre pas chez le consultant."""
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    _rejoint(patron, consultant)
    prive = _projet(consultant, "projet-perso-%s" % uuid.uuid4().hex[:6])
    assert m._db_project(consultant, prive) is not None, "il garde ses propres projets"
    assert m._db_project(patron, prive) is None, "le compte hote n'a pas acces aux projets du membre"


def test_quitter_le_compte_referme_l_acces() -> None:
    """Un retrait doit fermer la porte tout de suite, sans redemarrage ni cache a vider."""
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)
    assert m._db_project(consultant, slug) is not None
    with m.DB.session() as db:
        from sqlalchemy import select as _select
        ligne = db.scalar(_select(AccountMember).where(AccountMember.member_user_id == consultant))
        db.delete(ligne)
        db.commit()
    assert m._db_project(consultant, slug) is None


# --- l'ambiguite de slug, refusee en base ------------------------------------------------------

def test_une_SECONDE_adhesion_est_refusee_par_le_schema() -> None:
    """La garde qui protege la resolution des slugs.

    Les slugs sont uniques par proprietaire. Deux adhesions rendraient `/projects/mon-site`
    ambigu, et la fonction devrait DEVINER lequel montrer. La base refuse, ce qui transforme une
    ambiguite silencieuse en erreur visible le jour ou on voudra vraiment plusieurs adhesions.
    """
    a, b, consultant = _utilisateur("agence-a"), _utilisateur("agence-b"), _utilisateur("consultant")
    _rejoint(a, consultant)
    with pytest.raises(IntegrityError):
        _rejoint(b, consultant)


def test_son_PROPRE_projet_gagne_sur_un_homonyme_du_compte_hote() -> None:
    """Le seul cas d'homonymie qui subsiste, et il se tranche sans deviner : le sien d'abord."""
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    _rejoint(patron, consultant)
    nom = "meme-nom-%s" % uuid.uuid4().hex[:6]
    _projet(patron, nom)
    _projet(consultant, nom)
    trouve = m._db_project(consultant, nom)
    assert trouve is not None
    assert trouve.owner_user_id == consultant, "c'est le projet du compte hote qui est sorti"


def test_une_table_ABSENTE_ne_ferme_pas_l_acces_a_ses_propres_projets() -> None:
    """Le scenario du DEPLOIEMENT, et le seul qui pouvait tout casser d'un coup.

    Le nouveau code part en production avant que la table existe : entre le demarrage et la
    migration, `select(AccountMember)` echoue pour tout le monde. Si cette erreur remontait, plus
    personne n'ouvrirait ses propres projets — une panne totale causee par une fonctionnalite que
    personne n'utilise encore.

    Le repli rend donc la liste minimale (son propre compte). Une mutation l'a montre non teste :
    le remplacer par une liste vide ne cassait aucun test, alors qu'il aurait mis le site a plat.
    """
    from sqlalchemy import text as _text

    proprietaire = _utilisateur("proprio")
    slug = _projet(proprietaire)
    try:
        with m.DB.session() as db:
            db.execute(_text("DROP TABLE account_members"))
            db.commit()
        trouve = m._db_project(proprietaire, slug)
        assert trouve is not None, "la table absente a ferme l'acces aux projets du proprietaire"
    finally:
        m.DB.create_tables()
