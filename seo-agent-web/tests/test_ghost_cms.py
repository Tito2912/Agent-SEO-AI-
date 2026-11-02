"""Ghost, deuxième connecteur (02/11/2026) : la clé Admin API, le jeton signé, la collision.

Le parcours (aperçu, validation, écriture relue, suivi, rétablissement) est celui de WordPress ;
ces tests prouvent ce que Ghost a de propre : jeton HS256 de quelques minutes, `updated_at`
renvoyé à chaque écriture, page retrouvée par son slug parmi les articles PUIS les pages, et une
collision (Ghost n'a rien enregistré) qui rend l'aperçu au lieu de le laisser « à confirmer »."""
from __future__ import annotations

import ast
import base64
import copy
import hashlib
import hmac
import json
from pathlib import Path

import pytest

from backend import app as m, cms_guidance, cms_routes, ghost_cms as gh, i18n, wordpress_cms as wp
from backend.cms_commun import CmsError
from backend.models import Project
from test_cms_integrations import post, row
from test_first_verified_correction import URL, _choose, journey as journey

BACKEND = Path(__file__).resolve().parents[1] / "backend"
CATALOGUE = json.loads((BACKEND.parent / "locales" / "en.json").read_text(encoding="utf-8"))
CLE = "6489c7a3e1f0b2a4c5d6e7f8:" + "ab" * 32
TITRE = "Service de réparation de vélos à Lyon — Atelier"


def _decoder(part):
    return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))


def test_le_jeton_est_celui_que_ghost_attend():
    jeton = gh.jeton(CLE, maintenant=1_800_000_000)
    entete, charge, signature = jeton.split(".")
    assert _decoder(entete) == {"alg": "HS256", "kid": CLE.split(":")[0], "typ": "JWT"}
    contenu = _decoder(charge)
    assert contenu["aud"] == "/admin/" and contenu["iat"] == 1_800_000_000
    assert 0 < contenu["exp"] - contenu["iat"] <= 300  # Ghost refuse au-delà de 5 minutes
    attendu = hmac.new(bytes.fromhex(CLE.split(":")[1]), f"{entete}.{charge}".encode(), hashlib.sha256).digest()
    assert base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4)) == attendu


@pytest.mark.parametrize("saisie, attendu", [
    ("https://atelier.example", "https://atelier.example"),
    ("https://atelier.example/blog/", "https://atelier.example/blog"),
    ("https://atelier.example/ghost/", "https://atelier.example"),
    ("https://atelier-1.ghost.io/ghost", "https://atelier-1.ghost.io"),
])
def test_l_adresse_reste_sur_le_projet_ou_son_administration_ghost_pro(saisie, attendu):
    assert gh.site_url(saisie, "https://atelier.example/") == attendu


@pytest.mark.parametrize("saisie", ["http://atelier.example", "https://autre.example", "https://atelier.example?x=1",
                                    "https://ghost.io.autre.example", "https://user:pw@atelier.example",
                                    "https://atelier.example:8443"])
def test_une_adresse_hors_du_projet_est_refusee(saisie):
    with pytest.raises(CmsError) as erreur:
        gh.site_url(saisie, "https://atelier.example/")
    assert erreur.value.code == "adresse"


@pytest.mark.parametrize("cle", ["", "abc", CLE.upper(), CLE + "0", CLE.replace(":", "")])
def test_une_cle_mal_formee_est_refusee_avant_tout_appel(cle):
    with pytest.raises(CmsError) as erreur:
        gh.identifiants({"site_url": "https://atelier.example", "cle": cle}, Project(base_url="https://atelier.example/"))
    assert erreur.value.code == "saisie"


class _Reponse:
    def __init__(self, status, corps, type_="application/json"):
        self.status, self.corps, self.type_ = status, corps, type_

    def read(self, limite):
        return self.corps[:limite]

    def getheader(self, nom, defaut=""):
        return self.type_ if nom == "Content-Type" else defaut


def _connexion(monkeypatch, status, corps=b"{}", type_="application/json"):
    envois = []

    class Connexion:
        def __init__(self, host, address):
            envois.append({"host": host, "address": address})

        def request(self, method, path, body=None, headers=None):
            envois[-1].update(method=method, path=path, body=body, headers=headers)

        def getresponse(self):
            return _Reponse(status, corps, type_)

        def close(self):
            pass

    monkeypatch.setattr(gh, "public_address", lambda host: "203.0.113.7")
    monkeypatch.setattr(gh, "_PinnedHTTPS", Connexion)
    return envois


def test_chaque_requete_porte_le_jeton_et_la_version(monkeypatch):
    envois = _connexion(monkeypatch, 200, b'{"site": {}}')
    assert gh.exchange("https://atelier.example/blog", CLE, "GET", "site/") == {"site": {}}
    envoi = envois[0]
    assert envoi["host"] == "atelier.example" and envoi["address"] == "203.0.113.7"
    assert envoi["path"] == "/blog/ghost/api/admin/site/"
    assert envoi["headers"]["Authorization"].startswith("Ghost ") and envoi["headers"]["Accept-Version"] == "v5.0"
    entete = _decoder(envoi["headers"]["Authorization"][len("Ghost "):].split(".")[0])
    assert entete["kid"] == CLE.split(":")[0]


@pytest.mark.parametrize("status, methode, code, incertain", [
    (302, "GET", "redirection", False), (401, "GET", "acces_refuse", False), (404, "GET", "introuvable", False),
    (409, "PUT", "collision", False), (500, "PUT", "reponse_illisible", True), (401, "PUT", "acces_refuse", True),
])
def test_chaque_reponse_de_ghost_a_sa_raison(monkeypatch, status, methode, code, incertain):
    _connexion(monkeypatch, status)
    with pytest.raises(CmsError) as erreur:
        gh.exchange("https://atelier.example", CLE, methode, "posts/abc/", {"posts": [{}]} if methode == "PUT" else None)
    assert erreur.value.code == code and erreur.value.uncertain is incertain


def test_une_reponse_qui_n_est_pas_du_json_est_refusee(monkeypatch):
    _connexion(monkeypatch, 200, b"<html>", "text/html")
    with pytest.raises(CmsError) as erreur:
        gh.exchange("https://atelier.example", CLE, "GET", "site/")
    assert erreur.value.code == "reponse_illisible"


# --- Le parcours, contre un Ghost simulé --------------------------------------------------------

def _article():
    return {"id": "65a1b2c3d4e5f6a7b8c9d0e1", "title": "Notre service", "slug": "service", "status": "published",
            "url": URL, "updated_at": "2026-11-02T10:00:00.000Z", "meta_title": None,
            "meta_description": "Description initiale", "custom_excerpt": None}


@pytest.fixture()
def ghost(journey, monkeypatch):
    task = _choose(journey)
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "test-cms-key")
    etat = {"article": _article(), "appels": [], "site": {"title": "Atelier", "url": "https://atelier.example/",
                                                           "version": "5.96"}, "collision": False, "tour": 0}

    def exchange(base, cle, method, path, payload=None):
        assert cle == CLE and base == "https://atelier.example"
        etat["appels"].append((method, path, copy.deepcopy(payload)))
        if path == "site/":
            return {"site": dict(etat["site"])}
        if path.startswith("posts/?limit=1"):
            return {"posts": []}
        if path == "posts/slug/service/":
            return {"posts": [copy.deepcopy(etat["article"])]}
        if path.startswith(("posts/slug/", "pages/slug/")):
            raise CmsError("Ghost ne trouve pas cette ressource.", code="introuvable")
        assert method == "PUT" and path == f'posts/{etat["article"]["id"]}/'
        (modif,) = payload["posts"]
        if etat["collision"] or modif["updated_at"] != etat["article"]["updated_at"]:
            raise CmsError("La page a changé dans Ghost depuis l’aperçu. L’opération est arrêtée pour conserver les modifications actuelles.",
                           code="collision")
        etat["tour"] += 1
        etat["article"].update({k: v for k, v in modif.items() if k != "updated_at"},
                               updated_at=f"2026-11-02T10:0{etat['tour']}:00.000Z")
        return {"posts": [copy.deepcopy(etat["article"])]}

    monkeypatch.setattr(gh, "exchange", exchange)

    def wordpress(*args, **kwargs):
        raise AssertionError("WordPress ne doit pas être appelé")

    monkeypatch.setattr(wp, "exchange", wordpress)
    journey.update(task=task, etat=etat, base=f'/projects/{journey["slug"]}/cms')
    return journey


def _connecter(data, cle=CLE):
    return post(data, "/ghost/connect", site_url="https://atelier.example", cle=cle)


def _ecritures(data):
    return [appel for appel in data["etat"]["appels"] if appel[0] == "PUT"]


def test_ghost_fait_tout_le_parcours(ghost):
    reponse = _connecter(ghost)
    assert reponse.status_code == 303 and "err=" not in reponse.headers["location"]
    # /site/ répond sans clé : la connexion prouve la clé par une lecture authentifiée.
    assert ("GET", "posts/?limit=1&fields=id", None) in ghost["etat"]["appels"]
    with m.DB.session() as db:
        assert cms_guidance.settings(db.get(Project, ghost["project_id"]).settings)["platform"] == "ghost"
    reglages = ghost["client"].get(ghost["base"]).text
    assert "Déconnecter Ghost" in reglages and "Atelier" in reglages and CLE not in reglages

    edition = ghost["client"].get(ghost["base"] + f"/ghost/edit?task_id={ghost['task'].id}")
    assert edition.status_code == 200 and "Notre service" in edition.text
    apercu = post(ghost, "/ghost/preview", task_id=ghost["task"].id, value=TITRE)
    change = apercu.headers["location"].rsplit("/", 1)[1]
    assert row(change).data["post_id"] == "posts:" + ghost["etat"]["article"]["id"]
    assert row(change).data["rendered_before"] == "Notre service"  # sans titre SEO, le thème affiche le titre
    assert not _ecritures(ghost)

    post(ghost, f"/ghost/changes/{change}/apply")
    assert _ecritures(ghost) == [("PUT", f'posts/{ghost["etat"]["article"]["id"]}/',
                                 {"posts": [{"updated_at": "2026-11-02T10:00:00.000Z", "meta_title": TITRE}]})]
    assert row(change).state == "applied"
    suivi = ghost["client"].get(ghost["base"] + "/ghost/changes/" + change).text
    assert "Ghost a confirmé l’enregistrement." in suivi

    post(ghost, f"/ghost/changes/{change}/restore")
    assert _ecritures(ghost)[-1][2] == {"posts": [{"updated_at": "2026-11-02T10:01:00.000Z", "meta_title": None}]}
    assert row(change).state == "restored" and ghost["etat"]["article"]["meta_title"] is None


def test_une_collision_rend_l_apercu_sans_rien_ecrire(ghost):
    _connecter(ghost)
    apercu = post(ghost, "/ghost/preview", task_id=ghost["task"].id, value=TITRE)
    change = apercu.headers["location"].rsplit("/", 1)[1]
    ghost["etat"]["collision"] = True
    reponse = post(ghost, f"/ghost/changes/{change}/apply")
    assert "err=" in reponse.headers["location"]
    assert row(change).state == "preview" and ghost["etat"]["article"]["meta_title"] is None
    ghost["etat"]["collision"] = False
    post(ghost, f"/ghost/changes/{change}/apply")
    assert row(change).state == "applied" and ghost["etat"]["article"]["meta_title"] == TITRE


def test_une_page_modifiee_depuis_l_apercu_n_est_pas_ecrasee(ghost):
    _connecter(ghost)
    apercu = post(ghost, "/ghost/preview", task_id=ghost["task"].id, value=TITRE)
    change = apercu.headers["location"].rsplit("/", 1)[1]
    ghost["etat"]["article"].update(meta_title="Titre changé dans Ghost", updated_at="2026-11-02T11:00:00.000Z")
    post(ghost, f"/ghost/changes/{change}/apply")
    assert not _ecritures(ghost) and ghost["etat"]["article"]["meta_title"] == "Titre changé dans Ghost"


def test_une_page_ghost_est_cherchee_apres_les_articles(ghost, monkeypatch):
    client = gh.GhostClient({"site_url": "https://atelier.example", "cle": CLE, "domaine": "atelier.example"})
    page = {**_article(), "id": "page1", "url": "https://atelier.example/a-propos/"}
    reponses = {"posts/slug/a-propos/": CmsError("absent", code="introuvable"), "pages/slug/a-propos/": {"pages": [page]}}

    def exchange(base, cle, method, path, payload=None):
        reponse = reponses[path]
        if isinstance(reponse, Exception):
            raise reponse
        return reponse

    monkeypatch.setattr(gh, "exchange", exchange)
    assert client.read("https://atelier.example/a-propos/")["post_id"] == "pages:page1"
    with pytest.raises(CmsError):
        client.read("https://atelier.example/")  # l'accueil se règle dans les paramètres de Ghost


def test_un_brouillon_ou_une_autre_adresse_n_est_pas_modifiable(ghost):
    client = gh.GhostClient({"site_url": "https://atelier.example", "cle": CLE, "domaine": "atelier.example"})
    for article in ({**_article(), "status": "draft"}, {**_article(), "url": "https://atelier.example/autre/"}):
        with pytest.raises(CmsError):
            client.validate_page(article, "posts", URL)
    assert client.contains(URL) and not client.contains("https://autre.example/service/")
    with pytest.raises(CmsError):
        client.read("https://autre.example/service/")
    assert not ghost["etat"]["appels"]


def test_les_limites_de_ghost_sont_tenues_avant_l_envoi(ghost):
    client = gh.GhostClient({"site_url": "https://atelier.example", "cle": CLE, "domaine": "atelier.example"})
    page = client.validate_page(_article(), "posts", URL)
    for champ, longueur in (("seo_title", 301), ("meta_description", 501)):
        with pytest.raises(CmsError) as erreur:
            client.update(page, {champ: "x" * longueur})
        assert not erreur.value.uncertain  # rien n'est parti : l'aperçu reste valable
    assert not ghost["etat"]["appels"]


@pytest.mark.parametrize("site, code", [
    ({"title": "A", "url": "https://autre.example/", "version": "5.96"}, "autre_site"),
    ({"title": "A", "url": "https://atelier.example/", "version": "4.48"}, "version"),
])
def test_la_connexion_refuse_un_autre_site_ou_un_vieux_ghost(ghost, site, code):
    ghost["etat"]["site"] = site
    reponse = _connecter(ghost)
    assert "err=" in reponse.headers["location"] and not m._user_connection_row(
        user_id=ghost["owner"], key="CMS_GHOST:" + ghost["project_id"])
    assert code in gh.REASONS


def test_ghost_est_une_plateforme_guidee():
    guide = cms_guidance.for_issue({"cms": {"platform": "ghost"}}, "missing_title")
    assert guide["platform"] == "ghost" and guide["source"] == "https://ghost.org/help/post-settings/"
    assert any("Meta title" in etape for etape in guide["steps"])
    assert cms_routes.CONNECTEURS["ghost"] is gh.CONNECTEUR


# --- Chaque message est traduit -----------------------------------------------------------------

def _messages_du_module(nom):
    arbre = ast.parse((BACKEND / nom).read_text(encoding="utf-8"))
    for noeud in ast.walk(arbre):
        if isinstance(noeud, ast.Call) and getattr(noeud.func, "id", "") == "CmsError" and noeud.args:
            texte = noeud.args[0]
            if isinstance(texte, ast.BinOp) and isinstance(texte.left, ast.Constant):
                yield texte.left.value
            elif isinstance(texte, ast.Constant):
                yield texte.value


def test_chaque_message_ghost_a_sa_traduction():
    manquants = [texte for texte in _messages_du_module("ghost_cms.py") if texte not in CATALOGUE]
    assert not manquants


def test_les_phrases_composees_avec_le_nom_du_cms_sont_traduites_pour_chaque_connecteur():
    """« Connectez %s… », « … dans {cx.libelle}. » : traduites quel que soit le CMS."""
    arbre = ast.parse((BACKEND / "cms_routes.py").read_text(encoding="utf-8"))
    modeles = []
    for noeud in ast.walk(arbre):
        if (isinstance(noeud, ast.BinOp) and isinstance(noeud.op, ast.Mod) and isinstance(noeud.left, ast.Constant)
                and isinstance(noeud.left.value, str) and "libelle" in ast.unparse(noeud.right)):
            modeles.append(lambda libelle, texte=noeud.left.value: texte % libelle)
        elif isinstance(noeud, ast.JoinedStr) and "cx.libelle" in ast.unparse(noeud):
            parties = [valeur.value if isinstance(valeur, ast.Constant) else ast.unparse(valeur.value)
                       for valeur in noeud.values]
            genres = [isinstance(valeur, ast.Constant) for valeur in noeud.values]
            modeles.append(lambda libelle, p=parties, g=genres: "".join(
                x if constante else (libelle if x == "cx.libelle" else "3") for x, constante in zip(p, g)))
    assert len(modeles) >= 10
    for cx in cms_routes.CONNECTEURS.values():
        for modele in modeles:
            phrase = modele(cx.libelle)
            assert i18n.translate(phrase, "en") != phrase, phrase
