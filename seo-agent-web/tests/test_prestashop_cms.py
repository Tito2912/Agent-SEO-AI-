"""PrestaShop, troisième connecteur (02/11/2026) : produits et catégories, par le Webservice.

Le PrestaShop simulé ici se comporte comme le vrai 8.2.8 du laboratoire (ops/prestashop-lab) :
un champ vide est ABSENT du JSON, un champ multilingue lu sans `language=` est une liste
[{id, value}], et un PATCH qui n'envoie qu'une langue ÉCRIT, EFFACE les autres et répond 500."""
from __future__ import annotations

import ast
import base64
import copy
import html
import json
import re
from pathlib import Path

import pytest

from backend import app as m, cms_guidance, cms_routes, i18n, prestashop_cms as ps, wordpress_cms as wp
from backend.cms_commun import CmsError
from backend.models import Project
from test_cms_integrations import post, row
from test_first_verified_correction import _choose, journey as journey

BACKEND = Path(__file__).resolve().parents[1] / "backend"
CLE = "A1B2C3D4E5F6G7H8J9K0L1M2N3P4Q5R6"
FR, EN = 1, 3
PRODUIT = "https://atelier.example/fr/hommes/1-hummingbird-printed-t-shirt.html"
TITRE = "T-shirt imprimé colibri en coton pima — Atelier"


def _boutique():
    return {
        "langues": {FR: "fr", EN: "en"},
        "products": {1: {"name": {FR: "T-shirt imprimé colibri", EN: "Hummingbird printed T-shirt"},
                         "meta_title": {FR: "", EN: "EN title"}, "meta_description": {FR: "", EN: "EN description"},
                         "link_rewrite": {FR: "hummingbird-printed-t-shirt", EN: "hummingbird-printed-t-shirt"},
                         "description_short": {FR: "<p>Coupe classique, col rond.</p>", EN: "<p>Regular fit.</p>"},
                         "active": "1"},
                     19: {"name": {FR: "Carnet", EN: "Notebook"}, "meta_title": {FR: "", EN: ""},
                          "meta_description": {FR: "", EN: ""}, "link_rewrite": {FR: "carnet", EN: "notebook"},
                          "description_short": {FR: "", EN: ""}, "active": "0"}},
        "categories": {3: {"name": {FR: "Vêtements", EN: "Clothes"}, "meta_title": {FR: "", EN: ""},
                           "meta_description": {FR: "", EN: "EN clothes"}, "link_rewrite": {FR: "vetements", EN: "clothes"},
                           "description": {FR: "<p>Nos <b>coups de cœur</b> mode.</p>", EN: ""}, "active": "1"}},
        "version": "8.2.8", "droits": {"products": "get patch", "categories": "get patch", "languages": "get"},
        "nom": "Atelier &amp; Fils", "appels": [], "panne_patch": False,
    }


def _faux_webservice(etat):
    """L'API telle qu'observée sur PrestaShop 8.2.8 (02/11/2026)."""
    def exchange(base, cle, method, path, body=None):
        etat["appels"].append((method, path, body))
        chemin, _, requete = path.partition("?")
        params = dict(p.split("=", 1) for p in requete.split("&") if "=" in p)
        if chemin == "":
            balises = "".join(f'<{r} xlink:href="x" get="{"true" if "get" in d else "false"}" '
                              f'patch="{"true" if "patch" in d else "false"}" head="true"></{r}>'
                              for r, d in etat["droits"].items())
            return etat["version"], f'<prestashop><api shopName="{etat["nom"]}">{balises}</api></prestashop>'
        if chemin == "languages":
            return etat["version"], json.dumps({"languages": [{"id": i, "iso_code": iso, "active": "1"}
                                                               for i, iso in etat["langues"].items()]})
        ressource, ident = chemin.split("/")
        objet = etat.get(ressource, {}).get(int(ident))
        if objet is None:
            raise CmsError("PrestaShop ne trouve pas cette ressource.", code="introuvable")
        if method == "PATCH":
            if etat["panne_patch"]:
                raise CmsError("PrestaShop n’a pas renvoyé une réponse exploitable.", uncertain=True, code="reponse_illisible")
            envoyees = set()
            for champ, contenu in re.findall(r"<(meta_title|meta_description)>(.*?)</\1>", body.decode()):
                nouvelles = {int(i): html.unescape(v) for i, v in re.findall(r'<language id="(\d+)">(.*?)</language>', contenu)}
                envoyees |= set(nouvelles) ^ set(etat["langues"])
                objet[champ] = {lid: nouvelles.get(lid, "") for lid in etat["langues"]}  # les autres : effacées
            if envoyees:
                raise CmsError("PrestaShop n’a pas renvoyé une réponse exploitable.", uncertain=True, code="reponse_illisible")
            return etat["version"], "{}"
        cle_objet = {"products": "product", "categories": "category"}[ressource]
        if "language" in params:
            langue = int(params["language"])
            donnees = {"id": int(ident), "active": objet["active"]}
            donnees.update({k: v[langue] for k, v in objet.items() if isinstance(v, dict) and v[langue] != ""})
            return etat["version"], json.dumps({cle_objet: donnees})
        champs = re.search(r"display=\[(.*?)\]", requete).group(1).split(",")
        donnees = {"id": int(ident)}
        for champ in champs[1:]:
            if any(objet[champ].values()):  # vide dans TOUTES les langues : absent
                donnees[champ] = [{"id": str(i), "value": v} for i, v in objet[champ].items()]
        return etat["version"], json.dumps({ressource: [donnees]})
    return exchange


@pytest.fixture()
def boutique(monkeypatch):
    etat = _boutique()
    monkeypatch.setattr(ps, "exchange", _faux_webservice(etat))
    return etat


def _client():
    return ps.PrestaShopClient({"site_url": "https://atelier.example", "cle": CLE, "domaine": "atelier.example"})


# --- Saisie et adresse ---------------------------------------------------------------------------

@pytest.mark.parametrize("saisie, attendu", [
    ("https://atelier.example", "https://atelier.example"), ("https://atelier.example/boutique/", "https://atelier.example/boutique"),
    ("https://atelier.example/api/", "https://atelier.example"),
])
def test_l_adresse_reste_sur_le_projet(saisie, attendu):
    assert ps.site_url(saisie, "https://atelier.example/") == attendu


@pytest.mark.parametrize("saisie", ["http://atelier.example", "https://autre.example", "https://atelier.example?x=1",
                                    "https://u:p@atelier.example", "https://atelier.example:8443"])
def test_une_adresse_hors_du_projet_est_refusee(saisie):
    with pytest.raises(CmsError) as erreur:
        ps.site_url(saisie, "https://atelier.example/")
    assert erreur.value.code == "adresse"


@pytest.mark.parametrize("cle", ["", CLE.lower(), CLE[:-1], CLE + "X", CLE[:-1] + "-"])
def test_une_cle_mal_formee_est_refusee_avant_tout_appel(cle):
    with pytest.raises(CmsError) as erreur:
        ps.identifiants({"site_url": "https://atelier.example", "cle": cle}, Project(base_url="https://atelier.example/"))
    assert erreur.value.code == "saisie"


# --- Connexion -----------------------------------------------------------------------------------

def test_la_connexion_lit_le_nom_de_la_boutique(boutique):
    assert _client().check() == "Atelier & Fils"


@pytest.mark.parametrize("modif, code", [
    ({"version": "1.7.8.11"}, "version"), ({"version": ""}, "version"),
    ({"droits": {"products": "get patch", "categories": "get", "languages": "get"}}, "droits"),
    ({"droits": {"products": "get patch", "categories": "get patch"}}, "droits"),
])
def test_la_connexion_refuse_un_vieux_prestashop_ou_une_cle_trop_faible(boutique, modif, code):
    boutique.update(modif)
    with pytest.raises(CmsError) as erreur:
        _client().check()
    assert erreur.value.code == code and code in ps.REASONS


class _Reponse:
    def __init__(self, status, corps=b"{}", version="8.2.8"):
        self.status, self.corps, self.version = status, corps, version

    def read(self, limite):
        return self.corps[:limite]

    def getheader(self, nom, defaut=""):
        return self.version if nom == "PSWS-Version" else defaut


def _connexion(monkeypatch, status, corps=b"{}"):
    envois = []

    class Connexion:
        def __init__(self, host, address):
            envois.append({"host": host})

        def request(self, method, path, body=None, headers=None):
            envois[-1].update(method=method, path=path, headers=headers)

        def getresponse(self):
            return _Reponse(status, corps)

        def close(self):
            pass

    monkeypatch.setattr(ps, "public_address", lambda host: "203.0.113.9")
    monkeypatch.setattr(ps, "_PinnedHTTPS", Connexion)
    return envois


def test_chaque_requete_porte_la_cle_en_authentification_basique(monkeypatch):
    envois = _connexion(monkeypatch, 200, b'{"ok": 1}')
    assert ps.exchange("https://atelier.example/boutique", CLE, "GET", "products/1?language=1") == ("8.2.8", '{"ok": 1}')
    assert envois[0]["path"] == "/boutique/api/products/1?language=1"
    assert envois[0]["headers"]["Authorization"] == "Basic " + base64.b64encode((CLE + ":").encode()).decode()


@pytest.mark.parametrize("status, methode, code, incertain", [
    (302, "GET", "redirection", False), (401, "GET", "acces_refuse", False), (503, "GET", "desactive", False),
    (403, "GET", "droits", False), (404, "GET", "introuvable", False), (500, "GET", "reponse_illisible", False),
    (500, "PATCH", "reponse_illisible", True), (401, "PATCH", "acces_refuse", True),
])
def test_chaque_reponse_a_sa_raison_et_un_patch_en_echec_reste_incertain(monkeypatch, status, methode, code, incertain):
    _connexion(monkeypatch, status)
    with pytest.raises(CmsError) as erreur:
        ps.exchange("https://atelier.example", CLE, methode, "products/1", b"<x/>" if methode == "PATCH" else None)
    assert erreur.value.code == code and erreur.value.uncertain is incertain


# --- Lecture : de l'adresse au produit ou à la catégorie -------------------------------------------

@pytest.mark.parametrize("adresse, attendu", [
    (PRODUIT, "products:1:1"),
    ("https://atelier.example/fr/hommes/1-1-hummingbird-printed-t-shirt.html", "products:1:1"),  # déclinaison
    ("https://atelier.example/fr/hommes/1-hummingbird-printed-t-shirt-3760000000001.html", "products:1:1"),  # EAN13
    ("https://atelier.example/en/men/1-hummingbird-printed-t-shirt.html", "products:1:3"),
    ("https://atelier.example/fr/3-vetements", "categories:3:1"),
    ("https://atelier.example/en/3-clothes", "categories:3:3"),
])
def test_une_adresse_retrouve_sa_fiche_et_sa_langue(boutique, adresse, attendu):
    assert _client().read(adresse)["post_id"] == attendu


@pytest.mark.parametrize("adresse", [
    "https://atelier.example/fr/content/4-a-propos",  # page CMS : jamais modifiée
    "https://atelier.example/fr/3-vetements?page=2",  # filtre
    "https://atelier.example/fr/hommes/1-un-autre-produit.html",  # réécriture fausse
    "https://atelier.example/fr/art/19-carnet.html",  # désactivé
    "https://atelier.example/fr/hommes/99-inconnu.html",  # inexistant
    "https://atelier.example/hommes/1-hummingbird-printed-t-shirt.html",  # sans langue, boutique bilingue
    "https://autre.example/fr/3-vetements",  # autre domaine
])
def test_ce_qui_n_est_pas_un_produit_ou_une_categorie_publie_est_refuse(boutique, adresse):
    with pytest.raises(CmsError):
        _client().read(adresse)
    assert not [a for a in boutique["appels"] if a[0] == "PATCH"]


def test_une_page_cms_est_refusee_en_disant_pourquoi(boutique):
    """Constaté : PATCH réenregistre le contenu d'une page CMS. Le client doit savoir que c'est voulu."""
    with pytest.raises(CmsError) as erreur:
        _client().read("https://atelier.example/fr/content/4-a-propos")
    assert "pages CMS" in str(erreur.value) and not [a for a in boutique["appels"] if "content_management" in a[1]]


def test_une_boutique_a_une_langue_n_a_pas_de_prefixe(boutique):
    boutique["langues"] = {FR: "fr"}
    for objet in boutique["products"].values():
        for valeur in objet.values():
            if isinstance(valeur, dict):
                valeur.pop(EN, None)
    assert _client().read("https://atelier.example/hommes/1-hummingbird-printed-t-shirt.html")["post_id"] == "products:1:1"


def test_l_affichage_suppose_est_celui_du_theme(boutique):
    page = _client().read(PRODUIT)
    assert page["seo_title"] is None and page["seo_title_rendered"] == "T-shirt imprimé colibri"
    assert page["meta_description_rendered"] == "Coupe classique, col rond."
    categorie = _client().read("https://atelier.example/fr/3-vetements")
    assert categorie["meta_description_rendered"] == "Nos coups de cœur mode."


# --- Écriture ----------------------------------------------------------------------------------

def test_l_ecriture_renvoie_toutes_les_langues_et_ne_change_que_la_sienne(boutique):
    client = _client()
    ecrit = client.update(client.read(PRODUIT), {"seo_title": "Colibri & coton — l’essentiel"})
    assert ecrit["seo_title"] == "Colibri & coton — l’essentiel"
    produit = boutique["products"][1]
    assert produit["meta_title"] == {FR: "Colibri & coton — l’essentiel", EN: "EN title"}
    assert produit["meta_description"] == {FR: "", EN: "EN description"}  # champ non envoyé : intact
    (corps,) = [a[2] for a in boutique["appels"] if a[0] == "PATCH"]
    assert b"&amp;" in corps and b'<language id="3">EN title</language>' in corps


def test_un_champ_vide_partout_s_ecrit_sans_rien_effacer(boutique):
    client = _client()
    page = client.read("https://atelier.example/fr/3-vetements")
    client.update(page, {"seo_title": "Vêtements imprimés pour hommes et femmes"})
    assert boutique["categories"][3]["meta_title"] == {FR: "Vêtements imprimés pour hommes et femmes", EN: ""}
    client.update(client.read("https://atelier.example/fr/3-vetements"), {"seo_title": None})
    assert boutique["categories"][3]["meta_title"] == {FR: "", EN: ""}


def test_les_limites_de_prestashop_sont_tenues_avant_l_envoi(boutique):
    client = _client()
    page = client.read(PRODUIT)
    for champ, longueur in (("seo_title", 256), ("meta_description", 513)):
        with pytest.raises(CmsError) as erreur:
            client.update(page, {champ: "x" * longueur})
        assert not erreur.value.uncertain
    assert not [a for a in boutique["appels"] if a[0] == "PATCH"]


def test_un_patch_en_echec_laisse_le_resultat_incertain(boutique):
    client = _client()
    boutique["panne_patch"] = True
    with pytest.raises(CmsError) as erreur:
        client.update(client.read(PRODUIT), {"seo_title": TITRE})
    assert erreur.value.uncertain


# --- Le parcours, par les écrans de Noyaru ---------------------------------------------------------

@pytest.fixture()
def presta(journey, boutique, monkeypatch):
    task = _choose(journey)
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "test-cms-key")

    def wordpress(*args, **kwargs):
        raise AssertionError("WordPress ne doit pas être appelé")

    monkeypatch.setattr(wp, "exchange", wordpress)
    # La page de la fixture du parcours (« /service/ ») n'a pas la forme d'une adresse PrestaShop :
    # elle est lue comme le produit 1 ; la lecture d'une adresse est éprouvée plus haut.
    lire = ps.PrestaShopClient.read
    monkeypatch.setattr(ps.PrestaShopClient, "read", lambda self, adresse: {**lire(self, PRODUIT), "permalink": adresse}
                        if adresse == task.url else lire(self, adresse))
    journey.update(task=task, boutique=boutique, base=f'/projects/{journey["slug"]}/cms')
    return journey


def test_prestashop_fait_tout_le_parcours(presta):
    reponse = post(presta, "/prestashop/connect", site_url="https://atelier.example", cle=CLE)
    assert reponse.status_code == 303 and "err=" not in reponse.headers["location"]
    with m.DB.session() as db:
        assert cms_guidance.settings(db.get(Project, presta["project_id"]).settings)["platform"] == "prestashop"
    reglages = presta["client"].get(presta["base"]).text
    assert "Déconnecter PrestaShop" in reglages and "Atelier &amp; Fils" in reglages and CLE not in reglages

    apercu = post(presta, "/prestashop/preview", task_id=presta["task"].id, value=TITRE)
    change = apercu.headers["location"].rsplit("/", 1)[1]
    assert row(change).data["post_id"] == "products:1:1" and row(change).data["rendered_before"] == "T-shirt imprimé colibri"
    post(presta, f"/prestashop/changes/{change}/apply")
    assert row(change).state == "applied"
    assert presta["boutique"]["products"][1]["meta_title"] == {FR: TITRE, EN: "EN title"}
    assert "PrestaShop a confirmé l’enregistrement." in presta["client"].get(presta["base"] + "/prestashop/changes/" + change).text
    post(presta, f"/prestashop/changes/{change}/restore")
    assert row(change).state == "restored"
    assert presta["boutique"]["products"][1]["meta_title"] == {FR: "", EN: "EN title"}


def test_prestashop_est_une_plateforme_guidee():
    guide = cms_guidance.for_issue({"cms": {"platform": "prestashop"}}, "missing_meta_description")
    assert guide["platform"] == "prestashop" and any("Méta-description" in etape for etape in guide["steps"])
    assert cms_routes.CONNECTEURS["prestashop"] is ps.CONNECTEUR


def test_chaque_message_prestashop_a_sa_traduction():
    arbre = ast.parse((BACKEND / "prestashop_cms.py").read_text(encoding="utf-8"))
    messages = [noeud.args[0].value for noeud in ast.walk(arbre)
                if isinstance(noeud, ast.Call) and getattr(noeud.func, "id", "") == "CmsError"
                and noeud.args and isinstance(noeud.args[0], ast.Constant)]
    # Traduit comme à l'affichage : entrée exacte ou modèle {label} (« … renvoyées par {label} … »).
    assert len(messages) >= 15 and not [texte for texte in messages if i18n.translate(texte, "en") == texte]


def test_le_faux_webservice_reproduit_l_effacement_constate(boutique):
    """Garde-fou du simulateur lui-même : n'envoyer qu'une langue efface l'autre ET répond 500."""
    exchange = _faux_webservice(copy.deepcopy(boutique))
    with pytest.raises(CmsError) as erreur:
        exchange("", CLE, "PATCH", "products/1?output_format=JSON",
                 b'<prestashop><product><id>1</id><meta_title><language id="1">x</language></meta_title></product></prestashop>')
    assert erreur.value.uncertain
