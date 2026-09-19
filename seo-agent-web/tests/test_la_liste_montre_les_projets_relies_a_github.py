# -*- coding: utf-8 -*-
"""La liste des projets dit lesquels sont relies a un depot, et laisse les relier sur place.

C'est l'ecran ou l'on decide quoi corriger. Or une correction SANS DEPOT n'existe pas : elle
n'a nulle part ou aller. L'information etait pourtant invisible ici — il fallait ouvrir chaque
projet un par un pour decouvrir lequel etait relie, sur un compte qui en compte vingt-deux.

L'ETAT VIT DANS LA CELLULE DU PROJET, sous l'URL, et pas dans une septieme colonne. Un depot
lie fait partie de l'identite du projet, pas de ses mesures.

AUCUNE ROUTE NOUVELLE. L'ecran reutilise les deux memes que la page des corrections :
`/api/github/repos` pour proposer un choix plutot qu'un champ libre, et
`/api/projects/<slug>/github/connect` pour lier. Un second chemin de connexion aurait double les
regles a tenir — droits d'ecriture, branche valide, jeton du bon compte.

UN DEFAUT LATENT CORRIGE AU PASSAGE, ne du travail sur les comptes d'equipe du meme jour :
`/api/github/repos` listait les depots de la personne CONNECTEE, alors que la connexion valide
desormais avec le jeton du compte PROPRIETAIRE. Un membre se serait vu proposer SES depots puis
aurait recu « dépôt introuvable » sans comprendre. Les deux bouts regardent maintenant le meme
compte.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-ghliste-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

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


def _projet(owner_id: str, *, depot: str = "", branche: str = "") -> str:
    s = "site-%s" % uuid.uuid4().hex[:8]
    reglages: dict = {}
    if depot:
        reglages = {"github_repo": depot, "github_branch": branche or "main",
                    "github_mode": "review"}
    with m.DB.session() as db:
        db.add(Project(owner_user_id=owner_id, slug=s, site_name=s,
                       base_url="https://%s.fr/" % s, settings=reglages))
        db.commit()
    return s


def _client(user_id: str) -> TestClient:
    c = TestClient(app)
    c.cookies.set(auth.SESSION_COOKIE_NAME,
                  auth.make_session_token(user_id=user_id, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return c


# --- ce que la liste montre ---------------------------------------------------------------

def test_un_projet_RELIE_affiche_son_depot() -> None:
    uid = _utilisateur("client")
    _projet(uid, depot="client/mon-site", branche="production")
    page = _client(uid).get("/")
    assert page.status_code == 200
    assert "client/mon-site" in page.text, "le dépôt lié n'apparaît pas dans la liste"
    assert "https://github.com/client/mon-site" in page.text, "le dépôt n'est pas cliquable"
    assert "production" in page.text, "la branche n'est pas affichée"


def test_un_projet_NON_RELIE_propose_de_le_relier() -> None:
    """Sans ce bouton, l'information serait un constat et pas une action."""
    uid = _utilisateur("client")
    slug = _projet(uid)
    page = _client(uid).get("/")
    assert page.status_code == 200
    assert 'class="gh-lier" data-slug="%s"' % slug in page.text, (
        "aucun moyen de relier le projet depuis la liste")
    assert "Connecter GitHub" in page.text


def test_l_etat_s_affiche_meme_sur_un_projet_JAMAIS_CRAWLE() -> None:
    """Le piege que la structure de la page tendait.

    Une ligne de projet vient soit du resume d'un crawl, soit d'un repli quand il n'y en a
    aucun. Poser l'information sur le seul resume l'aurait fait disparaitre exactement la ou
    elle est la plus utile : un projet neuf, qu'on vient d'ajouter et qu'on veut relier.
    """
    uid = _utilisateur("client")
    _projet(uid, depot="client/tout-neuf")
    page = _client(uid).get("/")
    assert page.status_code == 200
    assert "Pas encore crawlé" in page.text, "le témoin est faux : ce projet a un crawl"
    assert "client/tout-neuf" in page.text, (
        "le dépôt disparaît sur un projet jamais crawlé — là où on veut justement le lier")


def test_le_depot_d_un_AUTRE_compte_n_apparait_pas() -> None:
    a, b = _utilisateur("a"), _utilisateur("b")
    _projet(b, depot="voisin/depot-prive")
    _projet(a, depot="a/le-mien")
    page = _client(a).get("/")
    assert "a/le-mien" in page.text
    assert "voisin/depot-prive" not in page.text


def test_un_MEMBRE_voit_le_depot_du_compte_hote() -> None:
    """Suite directe des comptes d'equipe : il travaille dessus, il doit voir ou ca va."""
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    _projet(patron, depot="agence/site-client")
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=patron, member_user_id=consultant))
        db.commit()
    page = _client(consultant).get("/")
    assert page.status_code == 200
    assert "agence/site-client" in page.text


# --- la liste des depots regarde le bon compte -----------------------------------------------

def test_la_liste_des_depots_suit_le_PROPRIETAIRE_du_projet(monkeypatch) -> None:
    """Le defaut latent, ne du travail sur les comptes d'equipe le meme jour.

    La connexion valide avec le jeton du compte proprietaire. Si la liste proposait les depots
    du membre, il choisirait un depot que la validation declare introuvable — et rien dans le
    message ne lui dirait pourquoi. Les deux bouts doivent interroger le meme compte.
    """
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron)
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=patron, member_user_id=consultant))
        db.commit()

    vus: list[str] = []

    def _faux_jeton(*, user_id: str, key: str, db=None):
        vus.append(str(user_id))
        return ("ghp_jeton", "user")

    monkeypatch.setattr(m, "_effective_user_connection_value", _faux_jeton)
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **k: [])

    rep = _client(consultant).get("/api/github/repos?slug=%s" % slug)
    assert rep.status_code == 200, rep.text
    assert vus and vus[0] == patron, (
        "les dépôts listés sont ceux du membre (%r) et non du propriétaire (%r)"
        % (vus[0] if vus else None, patron))


def test_SANS_slug_la_liste_reste_celle_de_son_propre_compte(monkeypatch) -> None:
    """L'autre moitie : la route sert aussi hors projet, et la elle ne doit rien deplacer."""
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=patron, member_user_id=consultant))
        db.commit()

    vus: list[str] = []
    monkeypatch.setattr(m, "_effective_user_connection_value",
                        lambda **kw: (vus.append(str(kw.get("user_id"))), ("ghp", "user"))[1])
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **k: [])

    rep = _client(consultant).get("/api/github/repos")
    assert rep.status_code == 200, rep.text
    assert vus and vus[0] == consultant, vus
