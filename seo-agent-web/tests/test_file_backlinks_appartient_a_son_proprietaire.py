# -*- coding: utf-8 -*-
"""Une opportunite de backlink ne s'approuve, ne se rejette et ne s'efface que chez soi.

Suite de la couverture de la zone backlinks, mesuree sans test dedie le 18/09/2026. Apres le
maillon d'entree (l'import CSV), voici les six routes qui MODIFIENT une opportunite a partir de
son identifiant : approve, reject, mark_sent, save, status, delete.

Un identifiant dans une URL est la forme classique de la faille d'acces : rien n'empeche de
remplacer celui d'autrui par le sien. L'audit du jour a lu les six requetes SQL et les a trouvees
saines — chacune filtre sur `id` ET `project_id` ET `user_id`. Ces tests existent pour que cela
le reste : une garde lue une fois ne protege que le jour ou on l'a lue.

POURQUOI SUR CETTE ZONE PLUTOT QU'UNE AUTRE. Une opportunite de backlink porte l'adresse d'un site
tiers, un texte de reponse redige par le modele, et un statut qui dit si le client l'a deja
contacte. Un acces croise n'y donnerait pas seulement des donnees : il permettrait de marquer
« envoye » une demarche qu'un autre n'a pas faite, ou d'effacer sa file de travail.

TROIS COUCHES REFUSENT, ET LA MUTATION A SERVI A LES DEMELER. Le CSRF d'abord — sans jeton, tout
le monde recoit 403, proprietaire compris, et des tests d'intrusion passeraient sans rien prouver.
`_db_project_or_404` ensuite, sur le slug : un intrus qui reclame le projet du voisin est arrete
la, avant toute requete. Les filtres SQL enfin, et ce sont EUX que ce fichier vise, d'ou l'intrus
qui passe par son PROPRE projet avec l'identifiant d'autrui.

Deux mutations survivent a ce fichier, et c'est une bonne nouvelle, pas un trou : retirer
`project_id` seul, ou `user_id` seul, ne change rien parce que CHACUN suffit a refuser. Il faut
retirer les deux pour que la garde cede — ce que la troisieme mutation verifie. Une redondance
qu'aucun test ne peut departager est une redondance qui protege, et la noter ici evite qu'on la
prenne un jour pour du code mort a nettoyer.

Ce que ces tests NE couvrent pas : le cron de verification, qui sort vers l'exterieur, et la
generation de reponse, qui appelle le modele. Ils demandent l'un un serveur, l'autre une cle.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-backlinks-acl-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import BacklinkOpportunity, Project, User  # noqa: E402


def _client_avec_opportunite() -> tuple[TestClient, str, str, str]:
    """Un compte, son projet, et une opportunite qui lui appartient."""
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        user = User(email="proprio-%s@exemple.fr" % tag,
                    password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        proj = Project(owner_user_id=uid, slug="site-%s" % tag, site_name="site.fr",
                       base_url="https://site.fr/")
        db.add(proj)
        db.commit()
        db.refresh(proj)
        opp = BacklinkOpportunity(
            project_id=str(proj.id), user_id=uid, source="web",
            title="Un blog qui parle du sujet", url="https://blog-tiers.fr/article-%s" % tag,
            snippet="extrait", opportunity_score=50)
        db.add(opp)
        db.commit()
        db.refresh(opp)
        oid = str(opp.id)
        slug = proj.slug
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug, oid, uid


def _intrus() -> tuple[TestClient, str]:
    """Un autre compte, parfaitement legitime — et SON propre projet.

    Le slug compte autant que le compte. Un intrus qui reclame le projet du voisin est arrete par
    `_db_project_or_404` bien avant la requete SQL ; passer par LE SIEN est le seul chemin qui met
    les filtres `project_id` / `user_id` a l'epreuve.
    """
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        autre = User(email="intrus-%s@exemple.fr" % tag,
                     password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(autre)
        db.commit()
        db.refresh(autre)
        sien = Project(owner_user_id=str(autre.id), slug="chez-lui-%s" % tag,
                       site_name="chez-lui.fr", base_url="https://chez-lui.fr/")
        db.add(sien)
        db.commit()
        db.refresh(sien)
        aid, son_slug = str(autre.id), sien.slug
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=aid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, son_slug


def _poster(client: TestClient, chemin: str, **kwargs):
    """Un POST avec son jeton anti-CSRF, comme le navigateur du client en envoie un.

    Sans lui la reponse est 403 pour TOUT LE MONDE, proprietaire compris — ce qui ferait passer
    les tests d'intrusion sans rien prouver : ils mesureraient le CSRF, pas le controle d'acces.
    C'est arrive a la premiere version de ce fichier.
    """
    client.get("/")                                # la session pose son cookie anti-CSRF
    jeton = client.cookies.get(app_module._CSRF_COOKIE_NAME, "") or ""
    return client.post(chemin, headers={app_module._CSRF_HEADER_NAME: jeton},
                       follow_redirects=False, **kwargs)


def _statut(oid: str) -> str:
    from sqlalchemy import select
    with app_module.DB.session() as db:
        opp = db.scalar(select(BacklinkOpportunity).where(BacklinkOpportunity.id == oid))
        return str(getattr(opp, "queue_status", "") or "") if opp else "SUPPRIMEE"


ACTIONS = ["approve", "reject", "mark-sent"]


@pytest.mark.parametrize("action", ACTIONS)
def test_le_proprietaire_peut_agir_sur_sa_file(action: str) -> None:
    """Sans ce bord, un refus generalise passerait pour une protection."""
    client, slug, oid, _uid = _client_avec_opportunite()
    reponse = _poster(client, "/api/projects/%s/backlinks/queue/%s/%s" % (slug, oid, action))
    assert reponse.status_code == 200, (action, reponse.status_code, reponse.text[:200])
    assert _statut(oid) != "", action


@pytest.mark.parametrize("action", ACTIONS)
def test_un_AUTRE_compte_ne_peut_pas_toucher_la_file_du_voisin(action: str) -> None:
    """L'identifiant se devine ou se recopie : c'est la requete qui doit refuser, pas l'URL."""
    _client, slug, oid, _uid = _client_avec_opportunite()
    avant = _statut(oid)
    client_intrus, _son_slug = _intrus()
    reponse = _poster(client_intrus, "/api/projects/%s/backlinks/queue/%s/%s" % (slug, oid, action))
    assert reponse.status_code in (403, 404), (action, reponse.status_code)
    assert _statut(oid) == avant, (
        "%s a modifie l'opportunite d'un autre compte" % action)


def test_un_AUTRE_compte_ne_peut_pas_EFFACER_la_file_du_voisin() -> None:
    """La suppression est la seule action irreversible du lot."""
    _client, slug, oid, _uid = _client_avec_opportunite()
    client_intrus, _son_slug = _intrus()
    reponse = _poster(client_intrus, "/projects/%s/backlinks/opportunities/%s/delete" % (slug, oid))
    assert reponse.status_code in (303, 403, 404), reponse.status_code
    assert _statut(oid) != "SUPPRIMEE", "l'opportunite d'un autre compte a ete effacee"


def test_un_identifiant_INVENTE_ne_fait_rien_planter() -> None:
    """Une reference inconnue doit rendre 404, pas une trace de pile."""
    client, slug, _oid, _uid = _client_avec_opportunite()
    reponse = _poster(client, "/api/projects/%s/backlinks/queue/%s/approve" % (slug, uuid.uuid4()))
    assert reponse.status_code == 404, reponse.status_code


@pytest.mark.parametrize("action", ACTIONS)
def test_un_intrus_passant_par_SON_PROPRE_projet_est_arrete_par_la_requete(action: str) -> None:
    """Le seul chemin qui met les filtres SQL a l'epreuve, et il manquait.

    La premiere version de ce fichier faisait reclamer a l'intrus le projet du voisin. Trois
    mutations y ont survecu : retirer `user_id`, retirer `project_id`, retirer les deux — les
    tests restaient verts, parce que `_db_project_or_404` refusait le slug bien avant la requete.
    Ils mesuraient la porte d'entree en croyant mesurer la serrure.

    Ici l'intrus passe par un projet qui est REELLEMENT le sien : la porte s'ouvre, et c'est le
    filtre `project_id` / `user_id` qui doit refuser l'identifiant du voisin. La defense en
    profondeur est reelle — mais chaque couche doit etre prouvee par ce qui la vise.
    """
    _client, _slug, oid, _uid = _client_avec_opportunite()
    avant = _statut(oid)
    client_intrus, son_slug = _intrus()
    reponse = _poster(client_intrus,
                      "/api/projects/%s/backlinks/queue/%s/%s" % (son_slug, oid, action))
    assert reponse.status_code in (403, 404), (action, reponse.status_code)
    assert _statut(oid) == avant, (
        "%s a modifie l'opportunite d'un autre compte via le projet de l'intrus" % action)


def test_un_intrus_passant_par_SON_PROPRE_projet_ne_peut_pas_effacer() -> None:
    """Meme chemin, sur la seule action irreversible."""
    _client, _slug, oid, _uid = _client_avec_opportunite()
    client_intrus, son_slug = _intrus()
    _poster(client_intrus, "/projects/%s/backlinks/opportunities/%s/delete" % (son_slug, oid))
    assert _statut(oid) != "SUPPRIMEE", "l'opportunite du voisin a ete effacee"
