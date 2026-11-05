"""Webflow (API Data v2) : titre SEO et méta-description d'une page statique.

Cinquième connecteur (04/11/2026), sur le parcours commun de `cms_routes`. Le client colle un jeton
de site Webflow (droits Pages en lecture et écriture, Sites en lecture seule) ; le jeton désigne
son site, il n'y a pas d'identifiant à saisir. Tout part vers https://api.webflow.com, jamais vers
le domaine du projet.

DÉCISION du propriétaire (04/11/2026) : Noyaru ENREGISTRE SANS JAMAIS PUBLIER. Une publication
Webflow met en ligne TOUT ce qui attend dans le Designer, y compris un travail inachevé du client ;
le jeton conseillé ne le permet d'ailleurs pas (Sites en lecture seule). L'écriture reste donc en
attente jusqu'à ce que le client publie : le suivi le lui dit (`Connecteur.publication`).

Chaque page donne son adresse publiée (`publishedPath`) : une adresse se retrouve directement. Les
pages de modèle d'une collection CMS sont exclues : leur titre et leur description valent pour
TOUS les éléments de la collection. Brouillons, pages archivées, pages utilitaires (401, 404) et
autres langues aussi.

CONSTATÉ sur un vrai site (04/11/2026) : PUT /pages/{id} ne change que les champs envoyés (le titre
seul garde la description) ; `null` est refusé (400), une chaîne vide efface le champ.
"""
from __future__ import annotations

import http.client
import json
import re
import ssl
import unicodedata
from urllib.parse import quote, unquote, urlsplit

try:
    from .cms_commun import FIELDS, CmsError, Connecteur, values_of
except ImportError:
    from cms_commun import FIELDS, CmsError, Connecteur, values_of

HOTE = "api.webflow.com"
API = "/v2"
# Un jeton de site Webflow ; sa forme exacte n'est pas documentée.
JETON = re.compile(r"[A-Za-z0-9_-]{32,256}")
TAILLE_MAX = 2 * 1024 * 1024
PAGES_MAX = 20
UTILITAIRES = {"401", "404"}
WEBFLOW = {"seo_title": "title", "meta_description": "description"}
PUBLICATION = ("Webflow garde cette modification en attente jusqu’à la prochaine publication : publiez votre site "
               "dans Webflow (bouton Publish), puis lancez le contrôle.")

REASONS = {
    "acces_refuse": "Jeton refusé ou révoqué",
    "droits": "Jeton sans les droits Pages en écriture",
    "autre_site": "Jeton d’un autre site Webflow",
    "introuvable": "Page introuvable dans Webflow",
    "refus": "Webflow a refusé la valeur",
    "limite": "Trop de requêtes vers Webflow",
    "collision": "Page modifiée dans Webflow depuis l’aperçu",
    "reponse_illisible": "Réponse Webflow inexploitable",
    "injoignable": "Webflow injoignable ou réponse interrompue",
    "saisie": "Jeton mal saisi",
    "enregistrement": "Erreur Noyaru à l’enregistrement",
}

_HTTPS = http.client.HTTPSConnection


def _norm(chemin):
    """Un chemin comparable : décodé, Unicode composé, sans barre finale ("" pour l'accueil)."""
    return unicodedata.normalize("NFC", unquote(chemin or "")).rstrip("/")


def racine(project_url):
    """L'hôte du site tel que le projet l'analyse (un site Webflow vit à la racine de son domaine)."""
    try:
        p = urlsplit(project_url.strip())
        hote = (p.hostname or "").lower()
        if p.scheme != "https" or not hote or p.username or p.password or p.port not in {None, 443} or p.path.strip("/"):
            raise ValueError()
        return "https://" + hote.encode("idna").decode("ascii")
    except (ValueError, UnicodeError, AttributeError):
        raise CmsError("L’adresse du projet doit être celle, en HTTPS, de votre site Webflow publié.", code="autre_site") from None


def exchange(jeton, method, path, payload=None):
    connection = _HTTPS(HOTE, timeout=12, context=ssl.create_default_context())
    write = method != "GET"
    try:
        connection.request(method, API + path, body=json.dumps(payload).encode() if payload is not None else None,
                           headers={"Authorization": "Bearer " + jeton, "Accept": "application/json",
                                    "Content-Type": "application/json", "User-Agent": "Noyaru-CMS/1.0"})
        response = connection.getresponse()
        data = response.read(TAILLE_MAX + 1)
        # Webflow contrôle le jeton, ses droits et la valeur AVANT d'enregistrer : refus certains.
        if response.status in {401, 403}:
            raise CmsError("Webflow refuse l’accès. Vérifiez que le jeton de site est toujours actif et qu’il a les droits Pages en écriture.",
                           code="acces_refuse")
        if response.status == 404:
            raise CmsError("Webflow ne trouve pas cette page.", code="introuvable")
        if response.status == 400:
            raise CmsError("Webflow a refusé cette valeur ; rien n’a été enregistré.", code="refus")
        if response.status == 429:
            raise CmsError("Webflow limite le nombre de requêtes. Réessayez dans une minute.", code="limite")
        if response.status >= 300 or len(data) > TAILLE_MAX or "json" not in response.getheader("Content-Type", "").lower():
            raise CmsError("Webflow n’a pas renvoyé une réponse exploitable.", uncertain=write, code="reponse_illisible")
        return json.loads(data)
    except CmsError:
        raise
    except Exception:
        raise CmsError("La réponse de Webflow n’a pas pu être confirmée. Vérifiez la connexion avant de poursuivre.",
                       uncertain=write, code="injoignable") from None
    finally:
        connection.close()


def values(page):
    return values_of(page, "Webflow")


class WebflowClient:
    def __init__(self, credentials):
        self.credentials = credentials
        self.hote = urlsplit(credentials["racine"]).hostname
        self._site_id = None
        self._site = None
        self._pages = None

    def request(self, method, path, payload=None):
        return exchange(self.credentials["jeton"], method, path, payload)

    def site_id(self):
        """Le site du jeton : un jeton de site Webflow n'en désigne qu'un."""
        if self._site_id is None:
            intro = self.request("GET", "/token/introspect")
            auth = intro.get("authorization") if isinstance(intro, dict) else None
            sites = ((auth or {}).get("authorizedTo") or {}).get("siteIds")
            if not isinstance(sites, list) or len(sites) != 1 or not isinstance(sites[0], str):
                raise CmsError("Utilisez un jeton de site Webflow, créé dans les réglages du site (Apps & integrations → API access).",
                               code="droits")
            scopes = set(str(auth.get("scope") or "").split(","))
            if not {"pages:read", "pages:write"} <= scopes:
                raise CmsError("Ce jeton n’a pas les droits Pages en lecture et en écriture. Créez-en un avec ces droits dans Webflow.",
                               code="droits")
            self._site_id = sites[0]
        return self._site_id

    def site(self):
        if self._site is None:
            site = self.request("GET", "/sites/" + quote(self.site_id(), safe=""))
            if not isinstance(site, dict) or not isinstance(site.get("shortName"), str):
                raise CmsError("Webflow n’a pas renvoyé une réponse exploitable.", code="reponse_illisible")
            self._site = site
        return self._site

    def check(self):
        site = self.site()
        hotes = {site["shortName"].lower() + ".webflow.io"}
        for domaine in site.get("customDomains") or []:
            adresse = str((domaine or {}).get("url") or "").lower()
            hotes.add(urlsplit(adresse if "://" in adresse else "https://" + adresse).hostname or "")
        if self.hote not in hotes:
            raise CmsError("Ce jeton appartient à un autre site Webflow que celui de ce projet, ou ce site est publié à une autre adresse.",
                           code="autre_site")
        return str(site.get("displayName") or "").strip() or "Site Webflow"

    def contains(self, url):
        try:
            p = urlsplit(url)
            return (p.scheme == "https" and (p.hostname or "").lower() == self.hote and p.port in {None, 443}
                    and not p.username and not p.password)
        except (ValueError, TypeError):
            return False

    def _liste(self):
        if self._pages is None:
            pages, decalage = [], 0
            for _ in range(PAGES_MAX):
                reponse = self.request("GET", f"/sites/{quote(self.site_id(), safe='')}/pages?limit=100&offset={decalage}")
                lot = reponse.get("pages") if isinstance(reponse, dict) else None
                if not isinstance(lot, list):
                    raise CmsError("Webflow n’a pas renvoyé une réponse exploitable.", code="reponse_illisible")
                pages += [page for page in lot if isinstance(page, dict)]
                decalage += len(lot)
                if not lot or decalage >= int((reponse.get("pagination") or {}).get("total") or 0):
                    break
            self._pages = pages
        return self._pages

    def read(self, url):
        p = urlsplit(url) if self.contains(url) else None
        if p is None or p.query or p.fragment:
            raise CmsError("Cette page ne se trouve pas dans le site Webflow connecté.")
        chemin = _norm(p.path)
        pages = self._liste()
        for modele in pages:
            prefixe = _norm(modele.get("publishedPath"))
            if modele.get("collectionId") and prefixe and chemin.startswith(prefixe + "/"):
                raise CmsError("Cette page est un élément d’une collection CMS Webflow : son titre et sa description viennent du modèle de la collection, commun à tous ses éléments. Corrigez-la dans Webflow, avec les instructions guidées.")
        trouves = [page for page in pages if _norm(page.get("publishedPath")) == chemin and not page.get("collectionId")
                   and not page.get("draft") and not page.get("archived") and not page.get("localeId")
                   and page.get("slug") not in UTILITAIRES and isinstance(page.get("id"), str)]
        if len(trouves) != 1:
            raise CmsError("Webflow ne trouve pas de page publiée à cette adresse dans la langue principale du site.")
        return self.page(self.request("GET", "/pages/" + quote(trouves[0]["id"], safe="")), url)

    def page(self, item, url):
        seo = item.get("seo") if isinstance(item, dict) else None
        if not isinstance(seo, dict) or not isinstance(item.get("id"), str) or not item["id"]:
            raise CmsError("Webflow n’a pas renvoyé une réponse exploitable.", code="reponse_illisible")
        # Une valeur vide est absente pour Webflow : sans titre SEO, la page publiée affiche le NOM DU
        # SITE (pas celui de la page — constaté le 04/11/2026), et aucune méta-description.
        page = {"post_id": item["id"], "permalink": url, "post_title": str(item.get("title") or ""),
                "seo_title": seo.get("title") or None, "meta_description": seo.get("description") or None}
        values(page)
        page.update(seo_title_rendered=page["seo_title"] or str(self.site().get("displayName") or "").strip(),
                    meta_description_rendered=page["meta_description"] or "")
        return page

    def update(self, page, changes):
        if not changes or not changes.keys() <= FIELDS:
            raise CmsError("Seuls le titre SEO et la méta-description sont pris en charge.")
        chemin = "/pages/" + quote(page["post_id"], safe="")
        # Pas de contrôle de version chez Webflow : relire juste avant d'écrire.
        if values(self.page(self.request("GET", chemin), page["permalink"])) != values(page):
            raise CmsError("La page a changé dans Webflow depuis l’aperçu. L’opération est arrêtée pour conserver les modifications actuelles.",
                           code="collision")
        # Seuls les champs envoyés changent ; Webflow refuse null, une chaîne vide efface.
        reponse = self.request("PUT", chemin, {"seo": {WEBFLOW[name]: value or "" for name, value in changes.items()}})
        try:
            written = self.page(reponse, page["permalink"])
            if written["post_id"] != page["post_id"] or any(written[name] != value for name, value in changes.items()):
                raise CmsError("Les valeurs enregistrées diffèrent de l’aperçu validé.")
        except CmsError as error:
            raise CmsError(str(error), uncertain=True) from None
        return written


def identifiants(form, project):
    """Le formulaire de connexion -> identifiants (jeton bien formé, adresse du projet)."""
    jeton = str(form.get("jeton") or "").strip()
    if not JETON.fullmatch(jeton):
        raise CmsError("Collez le jeton de site Webflow (Site settings → Apps & integrations → API access).", code="saisie")
    return {"jeton": jeton, "racine": racine(project.base_url)}


def recharger(value, project):
    if not JETON.fullmatch(str(value.get("jeton") or "")):
        raise ValueError()
    return {**value, "racine": racine(project.base_url)}


CONNECTEUR = Connecteur(
    nom="webflow", libelle="Webflow", libelle_complet="Webflow", provider="webflow_site_token",
    client=lambda c: WebflowClient(c), identifiants=identifiants, recharger=recharger,
    plateforme_ok=lambda site: site["platform"] == "webflow",
    message_plateforme="Choisissez Webflow pour utiliser cette connexion.",
    values=values, raisons=REASONS, champs=("jeton",), publication=PUBLICATION)
