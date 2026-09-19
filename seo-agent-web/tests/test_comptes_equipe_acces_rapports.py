# -*- coding: utf-8 -*-
"""Un membre d'equipe lit les RAPPORTS du compte qui l'accueille, pas seulement la fiche projet.

Suite immediate de `test_comptes_equipe_acces_projets`. L'etape precedente a elargi la question
« ce projet est-il a moi ? », et une fois elle passee le membre voyait la page projet — vide.
La raison tient en une ligne : les crawls sont ranges sur disque PAR UTILISATEUR
(`RUNS_DIR / user_id`) et le dossier etait resolu d'apres la personne CONNECTEE. Tant qu'un
projet n'avait qu'un utilisateur, « le dossier de qui demande » et « le dossier du projet »
designaient la meme chose ; des qu'un membre ouvre un projet qu'il ne possede pas, les deux
divergent et chaque page lisant un rapport repond « aucun crawl » sur un projet qui en a des
dizaines. Un partage qui montre une coquille vide n'est pas un partage.

CE QUE CES TESTS VERROUILLENT, et pourquoi chacun est la :

- le dossier suit le PROPRIETAIRE du projet, jamais le lecteur ;
- l'elargissement s'arrete aux comptes accessibles : un etranger retombe sur le sien ;
- `/file` — qui est une BARRIERE, pas un affichage — s'ouvre sur le compte hote et sur lui seul.
  Cette route lit un chemin arbitraire du disque et ne se protege que par sa liste de racines
  autorisees. L'elargir sans la borner exactement aurait ouvert les rapports de tous les clients ;
- la liste d'accueil ne montre qu'UNE ligne par slug. Sans ca, deux comptes possedant `mon-site`
  produiraient deux lignes dont l'une ouvre l'autre — l'ambiguite que le schema refuse en base
  reapparaitrait a l'ecran.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import types
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-equipe-rapports-"))
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

HORODATAGE = "20260101-120000"


def _utilisateur(prefixe: str) -> str:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="%s-%s@exemple.fr" % (prefixe, uuid.uuid4().hex[:8]),
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        return str(u.id)


def _projet(owner_id: str, slug: str | None = None, nom: str | None = None) -> str:
    s = slug or "site-%s" % uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        db.add(Project(owner_user_id=owner_id, slug=s, site_name=nom or s,
                       base_url="https://%s.fr/" % s))
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


def _plante_un_crawl(owner_id: str, slug: str, nom: str = "Site du patron") -> Path:
    """Un crawl minimal mais REEL sur le disque du proprietaire : run.json + audit/report.json."""
    base = m._runs_dir_for_user(owner_id) / slug / HORODATAGE
    (base / "audit").mkdir(parents=True, exist_ok=True)
    (base / "run.json").write_text(
        json.dumps({"site_name": nom, "base_url": "https://%s.fr/" % slug}), encoding="utf-8")
    rapport = base / "audit" / "report.json"
    rapport.write_text(
        json.dumps({"meta": {"pages_crawled": 3}, "issues": {}, "pages": []}), encoding="utf-8")
    return rapport


def _requete(user_id: str | None) -> object:
    u = types.SimpleNamespace(id=user_id) if user_id else None
    return types.SimpleNamespace(state=types.SimpleNamespace(user=u))


# --- le dossier suit le projet, pas le lecteur -------------------------------------------------

def test_le_dossier_des_rapports_suit_le_PROPRIETAIRE() -> None:
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron)
    _rejoint(patron, consultant)
    attendu = m._runs_dir_for_user(patron)
    obtenu = m._runs_dir_pour_slug(_requete(consultant), slug)
    assert obtenu == attendu, "le membre lirait %s au lieu de %s" % (obtenu, attendu)


def test_un_ETRANGER_reste_sur_son_propre_dossier() -> None:
    """Le bord qui empeche l'elargissement de devenir « tout le monde voit tout »."""
    patron, etranger = _utilisateur("agence"), _utilisateur("etranger")
    slug = _projet(patron)
    obtenu = m._runs_dir_pour_slug(_requete(etranger), slug)
    assert obtenu == m._runs_dir_for_user(etranger)
    assert obtenu != m._runs_dir_for_user(patron)


def test_un_projet_INCONNU_ne_donne_le_dossier_de_personne() -> None:
    """Un slug qui n'existe pas ne doit pas servir de sonde sur les dossiers des autres."""
    seul = _utilisateur("seul")
    obtenu = m._runs_dir_pour_slug(_requete(seul), "slug-qui-n-existe-pas-%s" % uuid.uuid4().hex[:6])
    assert obtenu == m._runs_dir_for_user(seul)


# --- ce que ca change dans une VRAIE page ------------------------------------------------------

def test_le_membre_voit_le_crawl_du_compte_hote() -> None:
    """La preuve qui compte : la page des crawls, pas la fonction qui la sert.

    Avant le reroutage cette page rendait la liste vide au membre, avec le meme code HTTP 200
    qu'un succes — c'est precisement ce qui rendait le defaut facile a rater a l'oeil.
    """
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron)
    _plante_un_crawl(patron, slug)
    _rejoint(patron, consultant)

    du_patron = _client(patron).get("/projects/%s/crawls" % slug)
    assert du_patron.status_code == 200 and HORODATAGE in du_patron.text, "le temoin est faux"

    vue = _client(consultant).get("/projects/%s/crawls" % slug)
    assert vue.status_code == 200, vue.status_code
    assert HORODATAGE in vue.text, "le membre voit le projet mais pas son crawl"


def test_la_liste_d_accueil_montre_les_projets_du_compte_hote_ET_leur_crawl() -> None:
    """Deux choses distinctes, et il faut les separer pour que le test morde.

    La fiche projet vient de la BASE, le resume vient du DISQUE. Une liste qui resoudrait encore
    le dossier d'apres le lecteur afficherait quand meme la ligne — en version « jamais crawle ».
    Le nom porte par le crawl differe donc du nom porte par la fiche : seule la lecture du bon
    dossier peut faire sortir le premier.
    """
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron, nom="Fiche sans crawl")
    _plante_un_crawl(patron, slug, nom="Boutique crawlee")
    _rejoint(patron, consultant)
    page = _client(consultant).get("/")
    assert page.status_code == 200
    assert slug in page.text, "le projet du compte hote n'apparait pas dans la liste"
    assert "Boutique crawlee" in page.text, "la ligne est la, mais son rapport n'a pas ete lu"


def test_la_liste_ne_montre_pas_les_projets_des_AUTRES() -> None:
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    _rejoint(patron, consultant)
    ailleurs = _utilisateur("ailleurs")
    slug = _projet(ailleurs, nom="Projet d un tiers")
    page = _client(consultant).get("/")
    assert page.status_code == 200
    assert slug not in page.text


def test_un_slug_HOMONYME_ne_sort_qu_une_fois_et_c_est_le_sien() -> None:
    """L'ambiguite refusee en base ne doit pas revenir a l'ecran.

    Deux lignes portant `mon-site` donneraient deux liens vers la meme adresse, dont l'un
    ouvrirait l'autre projet que celui affiche. On garde celle que la resolution rendra au clic.
    """
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    _rejoint(patron, consultant)
    nom = "meme-nom-%s" % uuid.uuid4().hex[:6]
    _projet(patron, nom, nom="Version du patron")
    _projet(consultant, nom, nom="Version du consultant")
    page = _client(consultant).get("/")
    assert page.status_code == 200
    # On compte les LIGNES du tableau et non les liens : une ligne de projet en contient
    # plusieurs (ouvrir, reglages, lancer un crawl), ce qui ferait dire deux a un compte naif.
    lignes = page.text.count('<tr data-project-slug="%s">' % nom)
    assert lignes == 1, "le slug homonyme sort %d fois" % lignes
    assert "Version du consultant" in page.text
    assert "Version du patron" not in page.text


# --- la barriere de /file ----------------------------------------------------------------------

def test_le_membre_ouvre_un_fichier_du_compte_hote() -> None:
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    slug = _projet(patron)
    rapport = _plante_un_crawl(patron, slug)
    _rejoint(patron, consultant)
    rep = _client(consultant).get("/file", params={"path": str(rapport)})
    assert rep.status_code == 200, rep.status_code


def test_file_REFUSE_toujours_le_dossier_d_un_tiers() -> None:
    """Le test qui empeche l'elargissement de devenir une fuite.

    `/file` lit un chemin arbitraire et ne tient que par sa liste de racines autorisees. La
    remplacer par la racine commune des runs aurait ouvert les rapports de tous les clients d'un
    coup, sans qu'aucun autre test ne s'en apercoive.
    """
    patron, consultant = _utilisateur("agence"), _utilisateur("consultant")
    _rejoint(patron, consultant)
    tiers = _utilisateur("tiers")
    slug = _projet(tiers)
    rapport = _plante_un_crawl(tiers, slug)
    rep = _client(consultant).get("/file", params={"path": str(rapport)})
    assert rep.status_code == 403, rep.status_code
