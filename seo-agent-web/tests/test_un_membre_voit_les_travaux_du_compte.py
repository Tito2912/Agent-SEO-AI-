# -*- coding: utf-8 -*-
"""Un membre doit voir, ouvrir, sonder et annuler les TRAVAUX du compte qui l'accueille.

LE SYMPTOME, tel que le proprietaire l'a decrit : « quand je clique sur nouveau crawl sur le
compte invite, la page se recharge a l'identique ». Le crawl etait pourtant bien cree et bien
mis en file. Il etait seulement INVISIBLE a celui qui venait de le lancer.

LA CAUSE, et elle vient d'une correction precedente. Depuis que la facturation suit le compte
hote, un travail porte l'identifiant du PAYEUR (`_compte_payeur`), c'est-a-dire le proprietaire
du projet. Sept endroits comparaient encore cet identifiant a celui de la personne CONNECTEE.
Tant qu'un projet n'avait qu'un utilisateur les deux designaient la meme chose ; des qu'un
membre lance un crawl chez son hote ils divergent, et la carte « en attente » ne s'affiche
jamais. Rien n'echoue, rien ne s'inscrit dans un journal : la page revient simplement
identique.

LA LECON, qui est celle de toute la journee : elargir UNE liste et rater les autres ne se voit
pas. La premiere fois c'etait le dossier des rapports, la deuxieme la liste des routes `/cron`,
celle-ci les sept comparaisons. Le dernier test de ce fichier ENUMERE au lieu de verifier un
site : il relit le code source et refuse qu'il reste une seule comparaison de cette forme,
y compris dans une route ecrite demain.

CE QUI NE BOUGE PAS. Un etranger — ni proprietaire, ni membre — reste dehors partout. Elargir
l'acces sans le borner exactement ouvrirait les travaux de tous les clients ; c'est pourquoi la
moitie de ce fichier mesure le REFUS.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-equipe-travaux-"))
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


def _projet(owner_id: str) -> str:
    s = "site-%s" % uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        db.add(Project(owner_user_id=owner_id, slug=s, site_name=s,
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
                  auth.make_session_token(user_id=user_id,
                                          secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return c


def _jeton_csrf(client: TestClient, page: str = "/jobs") -> str:
    """Les POST passent par la meme barriere anti-CSRF que le reste du site.

    L'etranger en recoit un aussi : sans cela son 403 viendrait du jeton manquant et le test
    passerait sans jamais atteindre le controle de propriete qu'il pretend mesurer.
    """
    trouve = re.search(r'name="_csrf"\s+value="([^"]*)"', client.get(page).text)
    return trouve.group(1) if trouve else ""


def _travail(owner_id: str, slug: str, statut: str = "queued") -> str:
    """Un crawl tel que la file en contient : il porte l'identifiant du PAYEUR.

    C'est le point de depart du defaut. `started_by` garde qui a appuye sur le bouton, mais
    `user_id` est le compte qui paye — donc le proprietaire, meme quand c'est un membre qui a
    lance le crawl.
    """
    job = m.Job(id=str(uuid.uuid4()), status=statut, created_at=time.time())
    job.result = {"type": "crawl", "slug": slug, "user_id": owner_id}
    job.command = ["python", "-u", "crawl.py"]
    m._save_job(job)
    return job.id


@pytest.fixture()
def equipe():
    """Un hote, son projet, son crawl en file, et un membre accueilli chez lui."""
    hote = _utilisateur("hote")
    membre = _utilisateur("membre")
    _rejoint(hote, membre)
    slug = _projet(hote)
    return {"hote": hote, "membre": membre, "slug": slug,
            "job": _travail(hote, slug), "client": _client(membre)}


# --- ce que le membre doit VOIR ------------------------------------------------------------

def test_la_carte_du_crawl_s_affiche_sur_la_page_projet(equipe) -> None:
    """LE SYMPTOME EXACT : sans cette carte, le bouton « nouveau crawl » semble ne rien faire.

    La page se rechargeait a l'identique parce que `live_job` restait vide. Ce test mesure la
    presence de la carte, pas celle d'une fonction — c'est ce que le proprietaire voit.
    """
    r = equipe["client"].get("/projects/%s" % equipe["slug"])
    assert r.status_code == 200
    assert 'data-job-id="%s"' % equipe["job"] in r.text, (
        "le crawl en file n'apparaît pas sur la page projet du membre")


def test_le_travail_demande_explicitement_s_ouvre(equipe) -> None:
    """Le second point de la meme page : `?job=<id>`, la redirection qui suit un lancement."""
    r = equipe["client"].get("/projects/%s?job=%s" % (equipe["slug"], equipe["job"]))
    assert r.status_code == 200
    assert 'data-job-id="%s"' % equipe["job"] in r.text


def test_le_travail_figure_dans_la_liste_des_travaux(equipe) -> None:
    r = equipe["client"].get("/jobs")
    assert r.status_code == 200
    assert equipe["job"] in r.text


def test_la_fiche_du_travail_s_ouvre(equipe) -> None:
    r = equipe["client"].get("/jobs/%s" % equipe["job"])
    assert r.status_code == 200
    assert equipe["job"] in r.text


def test_l_avancement_se_sonde(equipe) -> None:
    """La route que la page interroge en boucle. Un 404 ici fige la carte sur « en attente »."""
    r = equipe["client"].get("/api/jobs/%s" % equipe["job"])
    assert r.status_code == 200
    assert r.json()["id"] == equipe["job"]


# --- ce que le membre doit POUVOIR FAIRE ---------------------------------------------------

def test_le_membre_annule_le_crawl_du_compte(equipe) -> None:
    c = equipe["client"]
    r = c.post("/jobs/%s/cancel" % equipe["job"], data={"_csrf": _jeton_csrf(c)},
               follow_redirects=False)
    assert r.status_code == 303
    assert m._load_job(equipe["job"]).status == "canceled"


def test_le_membre_franchit_la_barriere_de_relance() -> None:
    """Mesure la BARRIERE, pas la relance.

    Un travail termine ressort en 303 sans rien remettre en file : la seule facon d'obtenir un
    404 est que le controle de propriete refuse. On verifie donc le droit d'entrer sans
    declencher de crawl dans une suite de tests.
    """
    hote = _utilisateur("hote")
    membre = _utilisateur("membre")
    _rejoint(hote, membre)
    job = _travail(hote, _projet(hote), statut="done")
    c = _client(membre)
    r = c.post("/jobs/%s/retry" % job, data={"_csrf": _jeton_csrf(c)}, follow_redirects=False)
    assert r.status_code == 303, "la relance a été refusée au membre"


# --- ce qui ne bouge pas : l'etranger reste dehors -----------------------------------------

@pytest.fixture()
def etranger(equipe):
    """Quelqu'un qui n'est ni le proprietaire ni un membre de ce compte."""
    return _client(_utilisateur("etranger"))


def test_l_etranger_ne_voit_pas_le_travail_dans_la_liste(equipe, etranger) -> None:
    r = etranger.get("/jobs")
    assert r.status_code == 200
    assert equipe["job"] not in r.text


def test_un_travail_SANS_proprietaire_n_ENTRE_PAS_dans_la_file(equipe) -> None:
    """C'est le SCHEMA qui l'interdit, pas le controle d'acces — et le test doit le dire.

    MA PREMIERE VERSION DE CE TEST ETAIT CREUSE. Elle affirmait qu'un travail sans proprietaire
    reste invisible, passait, et une mutation lui a survecu : je mesurais une consequence de la
    clé étrangère `JobRecord.owner_user_id`, qui est NOT NULL. `_save_job` refuse simplement
    d'ecrire une telle ligne. La clause `not owner_id` de `/jobs` est une ceinture par-dessus
    des bretelles — on la garde, mais aucun test ne peut l'atteindre par la base, et pretendre
    le contraire donnerait une couverture imaginaire.

    Ce qui est vrai se verrouille ici : la file n'accepte pas de travail anonyme. Le jour ou
    cette garantie sauterait, les sept controles ci-dessus reposeraient sur du vide.
    """
    orphelin = m.Job(id=str(uuid.uuid4()), status="queued", created_at=time.time())
    orphelin.result = {"type": "crawl", "slug": equipe["slug"]}
    m._save_job(orphelin)
    assert m._load_job(orphelin.id) is None, (
        "un travail sans propriétaire est entré dans la file : les contrôles d'accès des "
        "sept routes n'auraient plus rien à comparer")


@pytest.mark.parametrize("methode, gabarit", [
    ("get", "/jobs/%s"),
    ("get", "/api/jobs/%s"),
    ("post", "/jobs/%s/cancel"),
    ("post", "/jobs/%s/retry"),
])
def test_l_etranger_est_refuse_partout(equipe, etranger, methode, gabarit) -> None:
    kw = {"data": {"_csrf": _jeton_csrf(etranger)}} if methode == "post" else {}
    r = getattr(etranger, methode)(gabarit % equipe["job"], follow_redirects=False, **kw)
    assert r.status_code == 404, "%s %s a laissé passer un étranger" % (methode.upper(), gabarit)


def test_l_etranger_n_annule_rien(equipe, etranger) -> None:
    """Le refus doit etre SANS EFFET, pas seulement sans reponse."""
    etranger.post("/jobs/%s/cancel" % equipe["job"], data={"_csrf": _jeton_csrf(etranger)},
                  follow_redirects=False)
    assert m._load_job(equipe["job"]).status == "queued"


# --- l'enumeration ------------------------------------------------------------------------

def test_plus_AUCUNE_comparaison_a_l_utilisateur_connecte() -> None:
    """Le garde-fou qui compte vraiment : il couvre les routes pas encore ecrites.

    Les tests ci-dessus verifient sept endroits que je connais. Celui-ci relit le source et
    refuse la FORME qui a produit le defaut — comparer la propriete d'un travail a la personne
    connectee — ou qu'elle apparaisse. C'est la seule protection contre la huitieme occurrence,
    celle qu'on ajoutera sans y penser.
    """
    arbre = ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    fautes: list[str] = []
    for n in ast.walk(arbre):
        if not isinstance(n, ast.Compare) or not n.ops:
            continue
        if not isinstance(n.ops[0], (ast.Eq, ast.NotEq)):
            continue
        cotes = [ast.unparse(n.left)] + [ast.unparse(c) for c in n.comparators]
        connecte = any("getattr(user, 'id'" in c for c in cotes)
        propriete = any("owner_id" in c or "user_id" in c for c in cotes)
        if connecte and propriete:
            fautes.append("ligne %d : %s" % (n.lineno, ast.unparse(n)))
    assert not fautes, (
        "la propriété d'un travail est comparée à l'utilisateur connecté ; "
        "elle doit passer par _comptes_visibles :\n  " + "\n  ".join(fautes))


def test_le_nombre_de_requetes_ne_SUIT_PAS_le_nombre_de_travaux(equipe, monkeypatch) -> None:
    """Trois des sept appelants sont dans une boucle sur cent travaux.

    MA PREMIERE VERSION MESURAIT DE TRAVERS : elle plafonnait le nombre absolu d'appels, sans
    voir que `_db_project` interroge la meme fonction pour une raison qui n'a rien a voir. Ce
    qui compte n'est pas le total, c'est qu'il ne GRANDISSE PAS avec la file — et ca se compare,
    ca ne se plafonne pas.
    """
    appels: list[str] = []
    vrai = m._comptes_accessibles
    monkeypatch.setattr(m, "_comptes_accessibles",
                        lambda uid: (appels.append(uid), vrai(uid))[1])

    c = equipe["client"]
    c.get("/jobs")
    file_courte = len(appels)

    for _ in range(30):
        _travail(equipe["hote"], equipe["slug"], statut="done")
    appels.clear()
    c.get("/jobs")

    assert len(appels) == file_courte, (
        "%d requêtes pour une file de 31 travaux contre %d pour une file d'un seul : "
        "le calcul est reparti dans la boucle" % (len(appels), file_courte))
