"""Webflow, cinquième connecteur (04/11/2026) : écrire sans jamais publier.

Le parcours (aperçu, validation, écriture relue, suivi, rétablissement) est celui de WordPress ;
ces tests prouvent ce que Webflow a de propre : le jeton désigne son site (introspection), la
page se retrouve par son adresse publiée, les pages de collection sont refusées, seuls les champs
envoyés changent (une chaîne vide efface, `null` est refusé par Webflow), le titre de repli est
le NOM DU SITE (constaté en ligne), et le suivi dit au client de publier — Noyaru ne publie
jamais (décision du propriétaire du 04/11/2026)."""
from __future__ import annotations

import ast
import copy
import json
from pathlib import Path

import pytest

from backend import app as m, cms_guidance, cms_routes, webflow_cms as wf, wordpress_cms as wp
from backend.cms_commun import CmsError
from backend.models import Project
from test_cms_integrations import post, row
from test_first_verified_correction import URL, _choose, journey as journey

BACKEND = Path(__file__).resolve().parents[1] / "backend"
CATALOGUE = json.loads((BACKEND.parent / "locales" / "en.json").read_text(encoding="utf-8"))
JETON = "9f" * 32
TITRE = "Service de réparation de vélos à Lyon — Atelier"
CRED = {"jeton": JETON, "racine": "https://atelier.example"}


def test_le_jeton_est_verifie_avant_tout_appel():
    projet = Project(base_url="https://atelier.example/")
    assert wf.identifiants({"jeton": f" {JETON} "}, projet) == CRED
    for jeton in ("", "abc", JETON + " x", "é" * 40):
        with pytest.raises(CmsError) as erreur:
            wf.identifiants({"jeton": jeton}, projet)
        assert erreur.value.code == "saisie"
    with pytest.raises(CmsError):
        wf.racine("https://atelier.example/sous-dossier/")  # un site Webflow vit à la racine


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
        def __init__(self, host, timeout, context):
            envois.append({"host": host})

        def request(self, method, path, body=None, headers=None):
            envois[-1].update(method=method, path=path, body=body, headers=headers)

        def getresponse(self):
            return _Reponse(status, corps, type_)

        def close(self):
            pass

    monkeypatch.setattr(wf, "_HTTPS", Connexion)
    return envois


def test_chaque_requete_part_vers_l_api_de_webflow_avec_le_jeton(monkeypatch):
    envois = _connexion(monkeypatch, 200, b'{"ok": 1}')
    assert wf.exchange(JETON, "GET", "/token/introspect") == {"ok": 1}
    assert envois[0]["host"] == "api.webflow.com" and envois[0]["path"] == "/v2/token/introspect"
    assert envois[0]["headers"]["Authorization"] == "Bearer " + JETON


@pytest.mark.parametrize("status, methode, code, incertain", [
    (401, "PUT", "acces_refuse", False), (403, "GET", "acces_refuse", False), (404, "PUT", "introuvable", False),
    (400, "PUT", "refus", False), (429, "PUT", "limite", False), (500, "PUT", "reponse_illisible", True),
    (500, "GET", "reponse_illisible", False), (302, "PUT", "reponse_illisible", True),
])
def test_chaque_reponse_de_webflow_a_sa_raison(monkeypatch, status, methode, code, incertain):
    _connexion(monkeypatch, status)
    with pytest.raises(CmsError) as erreur:
        wf.exchange(JETON, methode, "/pages/p1", {} if methode != "GET" else None)
    assert erreur.value.code == code and erreur.value.uncertain is incertain


def test_une_reponse_qui_n_est_pas_du_json_est_refusee(monkeypatch):
    _connexion(monkeypatch, 200, b"<html>", "text/html")
    with pytest.raises(CmsError) as erreur:
        wf.exchange(JETON, "GET", "/sites/s1")
    assert erreur.value.code == "reponse_illisible"


# --- Le parcours, contre un Webflow simulé ---------------------------------------------------------

def _page(ident, chemin, titre, seo_titre=None, **autres):
    return {"id": ident, "title": titre, "slug": chemin.strip("/") or None, "publishedPath": chemin,
            "collectionId": None, "draft": False, "archived": False, "localeId": None,
            "seo": {"title": seo_titre, "description": None}, **autres}


@pytest.fixture()
def webflow(journey, monkeypatch):
    task = _choose(journey)
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "test-cms-key")
    etat = {"appels": [], "scope": "pages:read,pages:write,sites:read", "sites": ["s1"],
            "site": {"id": "s1", "shortName": "atelier", "displayName": "Atelier ", "customDomains": [{"url": "atelier.example"}]},
            "pages": {p["id"]: p for p in (
                _page("p0", "/", "Accueil", "Atelier de réparation"),
                _page("p1", "/service", "Notre service"),
                _page("pc", "/post", "Article", collectionId="c1"),
                _page("pd", "/brouillon", "Brouillon", draft=True),
            )}}

    def exchange(jeton, method, path, payload=None):
        assert jeton == JETON
        etat["appels"].append((method, path, copy.deepcopy(payload)))
        if path == "/token/introspect":
            return {"authorization": {"scope": etat["scope"], "authorizedTo": {"siteIds": etat["sites"]}}}
        if path == "/sites/s1":
            return copy.deepcopy(etat["site"])
        if path.startswith("/sites/s1/pages?"):
            return {"pages": copy.deepcopy(list(etat["pages"].values())), "pagination": {"total": len(etat["pages"])}}
        page = etat["pages"][path.rsplit("/", 1)[1]]
        if method == "PUT":
            # Comme le vrai Webflow (04/11/2026) : null refusé, chaîne vide = champ effacé, le reste gardé.
            if any(valeur is None for valeur in payload["seo"].values()):
                raise CmsError("Webflow a refusé cette valeur ; rien n’a été enregistré.", code="refus")
            page["seo"].update({k: v or None for k, v in payload["seo"].items()})
        return copy.deepcopy(page)

    monkeypatch.setattr(wf, "exchange", exchange)

    def wordpress(*args, **kwargs):
        raise AssertionError("WordPress ne doit pas être appelé")

    monkeypatch.setattr(wp, "exchange", wordpress)
    journey.update(task=task, etat=etat, base=f'/projects/{journey["slug"]}/cms')
    return journey


def _connecter(data):
    return post(data, "/webflow/connect", jeton=JETON)


def _ecritures(data):
    return [appel for appel in data["etat"]["appels"] if appel[0] != "GET"]


def test_webflow_fait_tout_le_parcours_sans_jamais_publier(webflow):
    reponse = _connecter(webflow)
    assert reponse.status_code == 303 and "err=" not in reponse.headers["location"]
    with m.DB.session() as db:
        assert cms_guidance.settings(db.get(Project, webflow["project_id"]).settings)["platform"] == "webflow"
    reglages = webflow["client"].get(webflow["base"]).text
    assert "Déconnecter Webflow" in reglages and JETON not in reglages

    apercu = post(webflow, "/webflow/preview", task_id=webflow["task"].id, value=TITRE)
    change = apercu.headers["location"].rsplit("/", 1)[1]
    assert row(change).data["post_id"] == "p1"
    assert row(change).data["rendered_before"] == "Atelier"  # sans titre SEO : le NOM DU SITE
    assert not _ecritures(webflow)

    post(webflow, f"/webflow/changes/{change}/apply")
    assert _ecritures(webflow) == [("PUT", "/pages/p1", {"seo": {"title": TITRE}})]
    assert row(change).state == "applied"
    suivi = webflow["client"].get(webflow["base"] + "/webflow/changes/" + change).text
    assert "Webflow a confirmé l’enregistrement." in suivi and "publiez votre site" in suivi

    post(webflow, f"/webflow/changes/{change}/restore")
    assert _ecritures(webflow)[-1] == ("PUT", "/pages/p1", {"seo": {"title": ""}})
    assert row(change).state == "restored" and webflow["etat"]["pages"]["p1"]["seo"]["title"] is None
    # Aucune publication, jamais : seuls des PUT de page sont partis.
    assert {(methode, chemin.split("/")[1]) for methode, chemin, _ in _ecritures(webflow)} == {("PUT", "pages")}


def test_une_page_modifiee_depuis_l_apercu_n_est_pas_ecrasee(webflow):
    _connecter(webflow)
    change = post(webflow, "/webflow/preview", task_id=webflow["task"].id, value=TITRE).headers["location"].rsplit("/", 1)[1]
    webflow["etat"]["pages"]["p1"]["seo"]["title"] = "Titre changé dans Webflow"
    reponse = post(webflow, f"/webflow/changes/{change}/apply")
    assert "err=" in reponse.headers["location"] and not _ecritures(webflow)
    assert row(change).state == "preview"


def test_une_valeur_changee_entre_la_relecture_et_l_ecriture_arrete_tout(webflow):
    client = wf.WebflowClient(CRED)
    page = client.read(URL)
    webflow["etat"]["pages"]["p1"]["seo"]["description"] = "Changée dans Webflow"
    with pytest.raises(CmsError) as erreur:
        client.update(page, {"seo_title": TITRE})
    assert erreur.value.code == "collision" and not erreur.value.uncertain and not _ecritures(webflow)


def test_seul_le_champ_corrige_part(webflow):
    client = wf.WebflowClient(CRED)
    ecrit = client.update(client.read("https://atelier.example/"), {"meta_description": "Atelier de vélos à Lyon."})
    assert _ecritures(webflow) == [("PUT", "/pages/p0", {"seo": {"description": "Atelier de vélos à Lyon."}})]
    assert ecrit["seo_title"] == "Atelier de réparation"  # le titre n'a pas été envoyé, il reste


def test_collection_brouillon_et_autre_site_sont_refuses(webflow):
    client = wf.WebflowClient(CRED)
    with pytest.raises(CmsError) as erreur:
        client.read("https://atelier.example/post/mon-article")
    assert "collection" in str(erreur.value)
    for adresse in ("https://atelier.example/brouillon", "https://atelier.example/inconnue",
                    "https://autre.example/service", "https://atelier.example/service?page=2"):
        with pytest.raises(CmsError):
            client.read(adresse)
    assert not _ecritures(webflow)


@pytest.mark.parametrize("reglage, code", [
    ({"site": {"id": "s1", "shortName": "autre", "displayName": "Autre", "customDomains": []}}, "autre_site"),
    ({"scope": "pages:read,sites:read"}, "droits"),
    ({"sites": ["s1", "s2"]}, "droits"),
])
def test_la_connexion_refuse_un_autre_site_ou_un_jeton_sans_droits(webflow, reglage, code):
    webflow["etat"].update(reglage)
    reponse = _connecter(webflow)
    assert "err=" in reponse.headers["location"] and not m._user_connection_row(
        user_id=webflow["owner"], key="CMS_WEBFLOW:" + webflow["project_id"])
    with pytest.raises(CmsError) as erreur:
        wf.WebflowClient(CRED).check()
    assert erreur.value.code == code and code in wf.REASONS


def test_l_adresse_webflow_io_est_acceptee_aussi(webflow):
    assert wf.WebflowClient({**CRED, "racine": "https://atelier.webflow.io"}).check() == "Atelier"


def test_webflow_reste_une_plateforme_guidee():
    guide = cms_guidance.for_issue({"cms": {"platform": "webflow"}}, "missing_title")
    assert guide["platform"] == "webflow" and guide["steps"]
    assert cms_routes.CONNECTEURS["webflow"] is wf.CONNECTEUR and wf.CONNECTEUR.publication
    # Les CMS en ligne aussitôt n'affichent pas de consigne de publication.
    assert not any(cx.publication for nom, cx in cms_routes.CONNECTEURS.items() if nom != "webflow")


def _messages_du_module(nom):
    arbre = ast.parse((BACKEND / nom).read_text(encoding="utf-8"))
    for noeud in ast.walk(arbre):
        if isinstance(noeud, ast.Call) and getattr(noeud.func, "id", "") == "CmsError" and noeud.args:
            texte = noeud.args[0]
            if isinstance(texte, ast.Constant):
                yield texte.value


def test_chaque_message_webflow_a_sa_traduction():
    manquants = [texte for texte in [*_messages_du_module("webflow_cms.py"), wf.PUBLICATION] if texte not in CATALOGUE]
    assert not manquants
