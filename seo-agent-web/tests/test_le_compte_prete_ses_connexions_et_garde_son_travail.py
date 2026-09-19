# -*- coding: utf-8 -*-
"""Sur un projet d'equipe, les connexions externes et les lignes creees suivent le COMPTE.

Quatrieme pierre des comptes d'equipe. Les trois precedentes ouvraient l'acces aux projets, a
leurs rapports, puis reglaient qui paie. Il restait deux familles, trouvees en enumerant tout
argument `user_id` d'une route projet plutot qu'en cherchant un motif fautif :

LES CONNEXIONS EXTERNES (22 appels). Search Console, Bing, le jeton GitHub etaient lus au nom
de la personne connectee. Un consultant ouvrant un projet de l'agence voyait donc des
graphiques vides et ne pouvait ouvrir aucune pull request — la fonctionnalite aurait eu l'air
livree et n'aurait servi a rien. Decision du proprietaire : tout suit le projet. A dire
clairement, parce que c'est un pouvoir reel et pas un detail technique : un membre POUSSE
desormais sur les depots de l'agence avec le jeton de l'agence, et peut deconnecter la Search
Console d'un projet qu'il ne possede pas.

LES LIGNES CREEES (8 constructions). `user_id` change de sens sur quatre tables : il ne dit
plus « la personne qui a fait ca » mais « le compte chez qui ca a ete fait ». Sans ce
changement, une agence ne retrouverait pas le travail de ses propres consultants et tout
disparaitrait de sa vue le jour ou l'un d'eux part. `created_by` garde qui a agi — l'information
que ce changement de sens aurait autrement detruite.

LE PIEGE DE CETTE ETAPE, et il ne se voit pas en lisant les ecritures : sur
`backlink_opportunities`, les LECTURES filtraient encore par la personne connectee. Un membre
aurait cree une opportunite chez le compte hote puis ne l'aurait pas retrouvee — ecrite chez
l'un, cherchee chez l'autre. C'est la seule des quatre tables concernee ; les trois autres
filtrent par `project_id`, qui est deja partage.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-connexions-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth, billing  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import (  # noqa: E402
    AccountMember, BacklinkOpportunity, Base, BillingSubscription, CompetitorSite, Project, User,
)


def _utilisateur(prefixe: str, plan: str = "") -> str:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="%s-%s@exemple.fr" % (prefixe, uuid.uuid4().hex[:8]),
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
    if plan:
        with m.DB.session() as db:
            db.add(BillingSubscription(
                user_id=uid, plan_key=plan, status="active",
                stripe_customer_id="cus_%s" % uuid.uuid4().hex[:10],
                stripe_subscription_id="sub_%s" % uuid.uuid4().hex[:10],
                stripe_price_id="price_%s" % uuid.uuid4().hex[:10]))
            db.commit()
        with m.DB.session() as db:
            obtenu = billing.effective_plan_key(db, user_id=uid)
        assert obtenu == plan, "le forfait pose n'est pas celui que lit l'application (%r)" % obtenu
    return uid


def _projet(owner_id: str, slug: str | None = None) -> str:
    s = slug or "site-%s" % uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        db.add(Project(owner_user_id=owner_id, slug=s, site_name=s, base_url="https://%s.fr/" % s))
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


def _jeton_csrf(client: TestClient, page: str) -> str:
    rep = client.get(page)
    assert rep.status_code == 200, "%s -> %d" % (page, rep.status_code)
    trouve = re.search(r'name="_csrf"\s+value="([^"]*)"', rep.text)
    assert trouve, "pas de jeton CSRF sur %s" % page
    return trouve.group(1)


def _connecte_github(user_id: str) -> None:
    m._upsert_user_connection(user_id=user_id, key="GITHUB_TOKEN", value="ghp_" + uuid.uuid4().hex)


# --- les connexions externes -------------------------------------------------------------------

def test_le_jeton_GITHUB_lu_est_celui_du_COMPTE_HOTE() -> None:
    """Le test qui distingue les deux lectures sans toucher au reseau.

    L'agence n'a PAS connecte GitHub, le consultant si. Si la route lisait le jeton de la
    personne connectee elle irait plus loin et echouerait sur le depot ; elle doit au contraire
    repondre « GitHub non connecte », parce que c'est vrai du compte qui possede le projet.
    """
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)
    _connecte_github(consultant)          # le MEMBRE a un jeton, l'agence non

    client = _client(consultant)
    jeton = _jeton_csrf(client, "/projects/%s" % slug)
    rep = client.post("/api/projects/%s/github/connect" % slug,
                      json={"repo": "agence/site", "branch": "main", "mode": "review"},
                      headers={m._CSRF_HEADER_NAME: jeton})
    assert rep.status_code == 400, rep.status_code
    assert "non connecté" in rep.json().get("error", ""), rep.json()


def test_sur_SON_projet_le_consultant_utilise_SON_jeton() -> None:
    """L'autre moitie : son propre projet ne bascule pas vers le compte hote."""
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    _rejoint(patron, consultant)
    sien = _projet(consultant)
    _connecte_github(consultant)

    client = _client(consultant)
    jeton = _jeton_csrf(client, "/projects/%s" % sien)
    rep = client.post("/api/projects/%s/github/connect" % sien,
                      json={"repo": "moi/site", "branch": "main", "mode": "review"},
                      headers={m._CSRF_HEADER_NAME: jeton})
    # Le depot n'existe pas, donc la route echoue — mais PAS sur l'absence de connexion.
    assert rep.status_code == 400
    assert "non connecté" not in rep.json().get("error", ""), rep.json()


# --- les lignes creees ---------------------------------------------------------------------------

def test_un_concurrent_ajoute_par_le_membre_appartient_au_COMPTE() -> None:
    patron = _utilisateur("agence", plan="business")
    consultant = _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)

    client = _client(consultant)
    page = "/projects/%s/competitors" % slug
    jeton = _jeton_csrf(client, page)
    rep = client.post("%s/add" % page, data={"url": "https://rival-%s.fr" % uuid.uuid4().hex[:6],
                                             "_csrf": jeton}, follow_redirects=False)
    assert rep.status_code == 303, rep.status_code

    with m.DB.session() as db:
        proj = db.scalar(select(Project).where(Project.owner_user_id == patron, Project.slug == slug))
        ligne = db.scalar(select(CompetitorSite).where(CompetitorSite.project_id == str(proj.id)))
    assert ligne is not None, "le concurrent n'a pas ete enregistre"
    assert ligne.user_id == patron, "la ligne appartient au membre, pas au compte"
    assert ligne.created_by == consultant, "on ne sait plus qui l'a ajoutee"


def test_le_PATRON_voit_le_concurrent_ajoute_par_son_consultant() -> None:
    """La raison d'etre du changement : sinon l'agence ne retrouve pas le travail paye."""
    patron = _utilisateur("agence", plan="business")
    consultant = _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)
    domaine = "rival-%s.fr" % uuid.uuid4().hex[:6]

    client = _client(consultant)
    page = "/projects/%s/competitors" % slug
    jeton = _jeton_csrf(client, page)
    client.post("%s/add" % page, data={"url": "https://%s" % domaine, "_csrf": jeton},
                follow_redirects=False)

    vue = _client(patron).get(page)
    assert vue.status_code == 200
    assert domaine in vue.text, "le patron ne voit pas le concurrent ajoute chez lui"


def test_une_opportunite_creee_par_le_membre_est_RELUE_par_lui() -> None:
    """Le piege de cette etape : ecrite chez l'un, cherchee chez l'autre.

    Les ecritures rebranchees sans les lectures donnent un formulaire qui repond « enregistre »
    puis une liste vide. Les trois autres tables filtrent par `project_id` et n'ont pas ce
    defaut ; celle-ci filtrait par `user_id`.
    """
    patron = _utilisateur("agence", plan="business")
    consultant = _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)
    titre = "Article invite %s" % uuid.uuid4().hex[:6]

    client = _client(consultant)
    page = "/projects/%s/backlinks/opportunities" % slug
    jeton = _jeton_csrf(client, page)
    rep = client.post("%s/save" % page,
                      data={"url": "https://blog-%s.fr/article" % uuid.uuid4().hex[:6],
                            "title": titre, "_csrf": jeton},
                      follow_redirects=False)
    assert rep.status_code == 303, rep.status_code

    with m.DB.session() as db:
        ligne = db.scalar(select(BacklinkOpportunity).where(BacklinkOpportunity.title == titre))
    assert ligne is not None, "l'opportunite n'a pas ete enregistree"
    assert ligne.user_id == patron
    assert ligne.created_by == consultant

    relu = client.get(page)
    assert relu.status_code == 200
    assert titre in relu.text, "le membre ne retrouve pas ce qu'il vient de creer"


def test_un_compte_hote_SANS_forfait_refuse_quand_meme() -> None:
    """Le sens inverse, sans lequel un plafond simplement DESACTIVE passerait pour un plafond
    correctement rebranche.

    Le meme consultant, la meme page, la seule difference etant le forfait du compte qui
    possede le projet. S'il n'en a pas, la porte reste fermee — c'est bien le plan de l'hote
    qui decide, et non l'absence de controle.
    """
    patron = _utilisateur("agence-sans-forfait")
    consultant = _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)

    client = _client(consultant)
    page = "/projects/%s/competitors" % slug
    jeton = _jeton_csrf(client, page)
    client.post("%s/add" % page, data={"url": "https://rival-%s.fr" % uuid.uuid4().hex[:6],
                                      "_csrf": jeton}, follow_redirects=False)
    with m.DB.session() as db:
        proj = db.scalar(select(Project).where(Project.owner_user_id == patron, Project.slug == slug))
        ligne = db.scalar(select(CompetitorSite).where(CompetitorSite.project_id == str(proj.id)))
    assert ligne is None, "le concurrent a ete enregistre alors que le compte hote n a pas le forfait"


# --- la garde qui aurait attrape la table oubliee -------------------------------------------------

def test_les_MIGRATIONS_produisent_le_schema_des_modeles() -> None:
    """La garde qui manquait, ecrite parce que l'omission est arrivee pour de vrai.

    `account_members` a ete ajoutee au modele puis deployee SANS migration. En local rien ne se
    voit : `DB.create_tables()` cree tout depuis les modeles. Sur Render seul Alembic tourne, la
    table n'existait donc pas, et la fonctionnalite etait inerte. Le repli l'a cachee — une
    table absente rend a chacun ses propres projets au lieu de remonter l'erreur — ce qui est la
    bonne conduite en production et exactement ce qui rend l'oubli invisible.

    Ce test part d'une base VIDE, n'applique QUE les migrations, et compare le schema obtenu aux
    modeles. Aucune table ni colonne de modele ne peut plus arriver en production sans migration.
    """
    from sqlalchemy import create_engine, inspect

    racine = Path(m.__file__).resolve().parents[1]
    ini = racine / "alembic.ini"
    assert ini.exists(), "alembic.ini introuvable : ce test ne mesure plus rien"

    with tempfile.TemporaryDirectory(prefix="alembic-vierge-") as dossier:
        base = Path(dossier) / "vierge.db"
        env = os.environ.copy()
        env["DATABASE_URL"] = "sqlite:///%s" % base.as_posix()
        proc = subprocess.run([sys.executable, "-m", "alembic", "-c", str(ini), "upgrade", "head"],
                              cwd=str(racine), env=env, capture_output=True, text=True, timeout=180)
        assert proc.returncode == 0, (proc.stderr or proc.stdout)[-2000:]

        moteur = create_engine("sqlite:///%s" % base.as_posix())
        inspecteur = inspect(moteur)
        tables_migrees = set(inspecteur.get_table_names())

        manquantes = sorted(set(Base.metadata.tables) - tables_migrees)
        assert not manquantes, (
            "ces tables existent dans les modeles mais aucune migration ne les cree :\n  "
            + "\n  ".join(manquantes))

        colonnes_manquantes = []
        for nom, table in sorted(Base.metadata.tables.items()):
            migrees = {c["name"] for c in inspecteur.get_columns(nom)}
            for colonne in table.columns:
                if colonne.name not in migrees:
                    colonnes_manquantes.append("%s.%s" % (nom, colonne.name))
        assert not colonnes_manquantes, (
            "ces colonnes existent dans les modeles mais aucune migration ne les ajoute :\n  "
            + "\n  ".join(colonnes_manquantes))
        moteur.dispose()
