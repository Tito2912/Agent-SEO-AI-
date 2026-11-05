"""Wix, quatrième connecteur (04/11/2026) : une page désignée par son type et son identifiant.

Le parcours (aperçu, validation, écriture relue, suivi, rétablissement) est celui de WordPress ;
ces tests prouvent ce que Wix a de propre : l'adresse retrouvée par la canonique PAR DÉFAUT, un
site gratuit vécu sous un préfixe, une écriture qui REMPLACE toutes les balises propres (le reste
doit revenir intact), la version en ligne d'une page classique écrite avec `publish` — y compris
le retour au modèle, qu'un « reset-to-default » laissait en ligne (essai réel du 04/11/2026) — et
les articles de blog refusés sans appel, Wix rejetant toute écriture sur eux."""
from __future__ import annotations

import ast
import copy
import json
from pathlib import Path

import pytest

from backend import app as m, cms_guidance, cms_routes, wix_cms as wx, wordpress_cms as wp
from backend.cms_commun import CmsError
from backend.models import Project
from test_cms_integrations import post, row
from test_first_verified_correction import URL, _choose, journey as journey

BACKEND = Path(__file__).resolve().parents[1] / "backend"
CATALOGUE = json.loads((BACKEND.parent / "locales" / "en.json").read_text(encoding="utf-8"))
CLE = "IST.eyJraWQiOiJQb3pIX2FDMiIsImFsZyI6IlJTMjU2In0." + "a1B2c3D4" * 20
SITE = "9e3d818b-784a-400b-ad40-e4a33a4909d7"
TITRE = "Service de réparation de vélos à Lyon — Atelier"
PARTAGE = {"type": "meta", "props": {"property": "og:image", "content": "https://static.example/velo.png"},
           "children": "", "custom": False, "disabled": False}
CRED = {"cle": CLE, "site_id": SITE, "racine": "https://atelier.example"}


def test_l_identifiant_et_la_cle_sont_verifies_avant_tout_appel():
    projet = Project(base_url="https://atelier.example/")
    assert wx.identifiants({"cle": f"  {CLE} ", "site_id": SITE.upper()}, projet) == CRED
    for saisie in ({"cle": CLE, "site_id": "93cdc3df"}, {"cle": "abc", "site_id": SITE},
                   {"cle": CLE + " x", "site_id": SITE}, {"cle": "", "site_id": ""}):
        with pytest.raises(CmsError) as erreur:
            wx.identifiants(saisie, projet)
        assert erreur.value.code == "saisie"


def test_un_site_gratuit_vit_sous_son_prefixe():
    client = wx.WixClient({**CRED, "racine": wx.racine("https://compte.wixsite.com/mon-site/")})
    assert client.relatif("https://compte.wixsite.com/mon-site") == ""
    assert client.relatif("https://compte.wixsite.com/mon-site/post/v%C3%A9lo") == "/post/vélo"
    assert client.relatif("/post/vélo/") == "/post/vélo"  # canonique d'un article : déjà relative au site
    for ailleurs in ("https://compte.wixsite.com/autre-site", "https://compte.wixsite.com/mon-site-2/a",
                     "http://compte.wixsite.com/mon-site", "https://compte.wixsite.com/mon-site/?lang=en",
                     "https://autre.example/mon-site"):
        assert not client.contains(ailleurs), ailleurs
    assert not client.contains("/post/velo")  # une adresse explorée est toujours absolue


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

    monkeypatch.setattr(wx, "_HTTPS", Connexion)
    return envois


def test_chaque_requete_part_vers_l_api_de_wix_avec_la_cle_et_le_site(monkeypatch):
    envois = _connexion(monkeypatch, 200, b'{"ok": 1}')
    assert wx.exchange(CLE, SITE, "GET", wx.SEO + "STATIC_PAGE") == {"ok": 1}
    assert envois[0]["host"] == "www.wixapis.com" and envois[0]["path"] == "/promote/seo/v1/item-seo-tags/STATIC_PAGE"
    assert envois[0]["headers"]["Authorization"] == CLE and envois[0]["headers"]["wix-site-id"] == SITE


@pytest.mark.parametrize("status, methode, code, incertain", [
    (401, "PATCH", "acces_refuse", False), (403, "GET", "acces_refuse", False), (404, "PATCH", "introuvable", False),
    (400, "PATCH", "refus", False), (429, "PATCH", "limite", False), (428, "PATCH", "brouillon", True),
    (500, "PATCH", "reponse_illisible", True), (500, "GET", "reponse_illisible", False),
    (302, "POST", "reponse_illisible", True),
])
def test_chaque_reponse_de_wix_a_sa_raison(monkeypatch, status, methode, code, incertain):
    _connexion(monkeypatch, status)
    with pytest.raises(CmsError) as erreur:
        wx.exchange(CLE, SITE, methode, wx.SEO + "STATIC_PAGE/abc", {} if methode != "GET" else None)
    assert erreur.value.code == code and erreur.value.uncertain is incertain


def test_une_reponse_qui_n_est_pas_du_json_est_refusee(monkeypatch):
    _connexion(monkeypatch, 200, b"<html>", "text/html")
    with pytest.raises(CmsError) as erreur:
        wx.exchange(CLE, SITE, "GET", wx.SEO + "STATIC_PAGE")
    assert erreur.value.code == "reponse_illisible"


def test_la_fusion_ne_touche_que_les_deux_champs():
    titre = {"type": "title", "children": "Ancien", "custom": False, "disabled": False}
    double = {"type": "title", "children": "Doublon"}
    tags = [titre, PARTAGE, double]
    assert wx.fusion(tags, {"seo_title": "Nouveau"}) == [{**titre, "children": "Nouveau"}, PARTAGE]
    assert wx.fusion(tags, {"seo_title": None}) == [PARTAGE]
    ajout = wx.fusion([PARTAGE], {"meta_description": "Texte"})
    assert ajout == [PARTAGE, {"type": "meta", "props": {"name": "description", "content": "Texte"}, "disabled": False}]
    assert tags == [titre, PARTAGE, double]  # l'original n'est pas modifié


# --- Le parcours, contre un Wix simulé -----------------------------------------------------------

def _resolues(item, modele):
    """Ce que Wix calcule : la valeur propre, sinon celle du modèle ; la canonique du modèle."""
    propres = {("title" if t["type"] == "title" else t["props"].get("name")): t for t in item["tags"]
               if t["type"] == "title" or t.get("props", {}).get("name") == "description"}
    resolues = [{"tag": propres.get("title") or {"type": "title", "children": modele},
                 "source": "TAG_SOURCE_ITEM" if "title" in propres else "TAG_SOURCE_DEFAULT_PATTERN"}]
    if "description" in propres:
        resolues.append({"tag": propres["description"], "source": "TAG_SOURCE_ITEM"})
    resolues.append({"tag": {"type": "link", "props": {"rel": "canonical", "href": item["canonique"]}},
                     "source": "TAG_SOURCE_DEFAULT_PATTERN"})
    return resolues


def _sortie(item, modele):
    return {"itemId": item["id"], "itemType": item["type"], "tags": copy.deepcopy(item["tags"]),
            "resolvedTags": _resolues(item, modele), "publishStatus": item["statut"]}


@pytest.fixture()
def wix(journey, monkeypatch):
    task = _choose(journey)
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "test-cms-key")
    etat = {"appels": [], "pages": {
        "c1dmp": {"id": "c1dmp", "type": "STATIC_PAGE", "canonique": "https://atelier.example", "tags": [],
                  "statut": "PUBLISH_STATUS_UNSPECIFIED"},
        "srv01": {"id": "srv01", "type": "STATIC_PAGE", "canonique": "https://atelier.example/service",
                  "tags": [copy.deepcopy(PARTAGE)], "statut": "PUBLISH_STATUS_UNSPECIFIED"},
        "prd01": {"id": "prd01", "type": "STORES_PRODUCT", "canonique": "/product-page/velo-de-ville", "tags": [],
                  "statut": "PUBLISH_STATUS_PUBLISHED"},
    }}

    def exchange(cle, site_id, method, path, payload=None):
        assert cle == CLE and site_id == SITE
        etat["appels"].append((method, path, copy.deepcopy(payload)))
        pages = etat["pages"]
        sorte, _, reste = path[len(wx.SEO):].partition("/")
        if not reste:  # une liste
            sorte = sorte.split("?")[0]
            return {"itemSeoTags": [_sortie(p, "Atelier") for p in pages.values() if p["type"] == sorte],
                    "pagingMetadata": {"hasNext": False}}
        ident, _, action = reste.partition("/")
        item = pages[ident]
        if method == "PATCH":
            assert payload["fieldMask"] == "tags" and payload.get("publish") is (True if sorte == "STATIC_PAGE" else None)
            item["tags"] = copy.deepcopy(payload["itemSeoTags"]["tags"])
        elif method == "POST":
            assert action == "reset-to-default"
            item["tags"] = []
        return {"itemSeoTags": _sortie(item, "Notre service | Atelier")}

    monkeypatch.setattr(wx, "exchange", exchange)

    def wordpress(*args, **kwargs):
        raise AssertionError("WordPress ne doit pas être appelé")

    monkeypatch.setattr(wp, "exchange", wordpress)
    journey.update(task=task, etat=etat, base=f'/projects/{journey["slug"]}/cms')
    return journey


def _connecter(data, site_id=SITE):
    return post(data, "/wix/connect", site_id=site_id, cle=CLE)


def _ecritures(data):
    return [appel for appel in data["etat"]["appels"] if appel[0] != "GET"]


def test_wix_fait_tout_le_parcours(wix):
    reponse = _connecter(wix)
    assert reponse.status_code == 303 and "err=" not in reponse.headers["location"]
    with m.DB.session() as db:
        assert cms_guidance.settings(db.get(Project, wix["project_id"]).settings)["platform"] == "wix"
    reglages = wix["client"].get(wix["base"]).text
    assert "Déconnecter Wix" in reglages and CLE not in reglages

    apercu = post(wix, "/wix/preview", task_id=wix["task"].id, value=TITRE)
    change = apercu.headers["location"].rsplit("/", 1)[1]
    assert row(change).data["post_id"] == "STATIC_PAGE:srv01"
    assert row(change).data["rendered_before"] == "Notre service | Atelier"  # le modèle SEO de Wix
    assert not _ecritures(wix)

    post(wix, f"/wix/changes/{change}/apply")
    # Toutes les balises propres repartent : le partage social de la page n'est pas effacé.
    assert _ecritures(wix) == [("PATCH", wx.SEO + "STATIC_PAGE/srv01", {
        "itemSeoTags": {"tags": [PARTAGE, {"type": "title", "children": TITRE, "disabled": False}]},
        "fieldMask": "tags", "publish": True})]
    assert row(change).state == "applied"
    assert "Wix a confirmé l’enregistrement." in wix["client"].get(wix["base"] + "/wix/changes/" + change).text

    post(wix, f"/wix/changes/{change}/restore")
    assert _ecritures(wix)[-1][2]["itemSeoTags"]["tags"] == [PARTAGE]
    assert row(change).state == "restored" and wix["etat"]["pages"]["srv01"]["tags"] == [PARTAGE]


def test_sans_autre_balise_le_retablissement_rend_la_page_au_modele(wix):
    wix["etat"]["pages"]["srv01"]["tags"] = []
    _connecter(wix)
    change = post(wix, "/wix/preview", task_id=wix["task"].id, value=TITRE).headers["location"].rsplit("/", 1)[1]
    post(wix, f"/wix/changes/{change}/apply")
    post(wix, f"/wix/changes/{change}/restore")
    # Une liste vide PUBLIÉE : « reset-to-default » ne touchait que le brouillon (essai réel).
    assert _ecritures(wix)[-1] == ("PATCH", wx.SEO + "STATIC_PAGE/srv01",
                                   {"itemSeoTags": {"tags": []}, "fieldMask": "tags", "publish": True})
    assert row(change).state == "restored"


def test_une_page_modifiee_depuis_l_apercu_n_est_pas_ecrasee(wix):
    _connecter(wix)
    change = post(wix, "/wix/preview", task_id=wix["task"].id, value=TITRE).headers["location"].rsplit("/", 1)[1]
    wix["etat"]["pages"]["srv01"]["tags"].append({"type": "title", "children": "Titre changé dans Wix"})
    reponse = post(wix, f"/wix/changes/{change}/apply")
    assert "err=" in reponse.headers["location"] and not _ecritures(wix)
    assert row(change).state == "preview"


def test_une_balise_changee_entre_la_relecture_et_l_ecriture_arrete_tout(wix):
    client = wx.WixClient(CRED)
    page = client.read(URL)
    wix["etat"]["pages"]["srv01"]["tags"][0]["props"]["content"] = "https://static.example/autre.png"
    with pytest.raises(CmsError) as erreur:
        client.update(page, {"seo_title": TITRE})
    assert erreur.value.code == "collision" and not erreur.value.uncertain and not _ecritures(wix)


def test_un_produit_n_a_pas_de_brouillon_et_revient_au_modele_par_reset(wix):
    client = wx.WixClient(CRED)
    page = client.read("https://atelier.example/product-page/velo-de-ville")
    assert page["post_id"] == "STORES_PRODUCT:prd01"
    ecrit = client.update(page, {"meta_description": "Vélo de ville réparé à Lyon."})
    assert _ecritures(wix)[-1][2] == {"itemSeoTags": {"tags": [ecrit["tags"][0]]}, "fieldMask": "tags"}
    client.update(ecrit, {"meta_description": None})
    assert _ecritures(wix)[-1] == ("POST", wx.SEO + "STORES_PRODUCT/prd01/reset-to-default", None)
    wix["etat"]["pages"]["prd01"]["statut"] = "PUBLISH_STATUS_NOT_PUBLISHED"
    with pytest.raises(CmsError):
        client.read("https://atelier.example/product-page/velo-de-ville")


def test_un_article_de_blog_est_refuse_sans_appel(wix):
    # Essai réel du 04/11/2026 : Wix refuse toute écriture sur un article (400 du service des brouillons).
    with pytest.raises(CmsError) as erreur:
        wx.WixClient(CRED).read("https://atelier.example/post/velo-lyon")
    assert "article de blog" in str(erreur.value) and not wix["etat"]["appels"]


def test_une_canonique_posee_a_la_main_ne_deplace_pas_la_page(wix):
    client = wx.WixClient(CRED)
    reponse = {"itemSeoTags": [{"itemId": "srv01", "resolvedTags": [
        {"tag": {"type": "link", "props": {"rel": "canonical", "href": "https://atelier.example/ailleurs"}},
         "source": "TAG_SOURCE_ITEM",
         "inheritedTag": {"type": "link", "props": {"rel": "canonical", "href": "https://atelier.example/service"}}}]}],
        "pagingMetadata": {"hasNext": False}}
    client.request = lambda method, path, payload=None: reponse
    assert client._liste("STATIC_PAGE") == {"/service": ["srv01"]}


def test_un_champ_desactive_dans_wix_n_est_pas_modifie(wix):
    wix["etat"]["pages"]["srv01"]["tags"].append({"type": "title", "children": "Caché", "disabled": True})
    with pytest.raises(CmsError):
        wx.WixClient(CRED).read(URL)


def test_une_adresse_hors_du_site_ou_inconnue_n_est_pas_modifiable(wix):
    client = wx.WixClient(CRED)
    for adresse in ("https://autre.example/service/", "https://atelier.example/inconnue"):
        with pytest.raises(CmsError):
            client.read(adresse)


def test_la_connexion_refuse_un_autre_site(wix):
    wix["etat"]["pages"]["c1dmp"]["canonique"] = "https://autre.example"
    reponse = _connecter(wix)
    assert "err=" in reponse.headers["location"] and not m._user_connection_row(
        user_id=wix["owner"], key="CMS_WIX:" + wix["project_id"])
    assert "autre_site" in wx.REASONS


def test_wix_reste_une_plateforme_guidee():
    guide = cms_guidance.for_issue({"cms": {"platform": "wix"}}, "missing_title")
    assert guide["platform"] == "wix" and guide["steps"]
    assert cms_routes.CONNECTEURS["wix"] is wx.CONNECTEUR


def _messages_du_module(nom):
    arbre = ast.parse((BACKEND / nom).read_text(encoding="utf-8"))
    for noeud in ast.walk(arbre):
        if isinstance(noeud, ast.Call) and getattr(noeud.func, "id", "") == "CmsError" and noeud.args:
            texte = noeud.args[0]
            if isinstance(texte, ast.Constant):
                yield texte.value


def test_chaque_message_wix_a_sa_traduction():
    manquants = [texte for texte in _messages_du_module("wix_cms.py") if texte not in CATALOGUE]
    assert not manquants
