"""Wix (API REST) : titre SEO et méta-description d'une page ou d'une fiche produit.

Quatrième connecteur (04/11/2026), sur le parcours commun de `cms_routes`. Le client colle une clé
API créée dans son compte Wix (permission « Manage SEO Settings ») et l'identifiant du site. Tout
part vers https://www.wixapis.com, jamais vers le domaine du projet : rien à épingler.

Wix désigne une page par un type et un identifiant, pas par son adresse. Une page ou un produit se
retrouve parmi les éléments de son type, par son adresse canonique PAR DÉFAUT (une canonique posée
à la main peut viser une autre page : on lit celle qu'elle remplace).

Écrire REMPLACE toutes les balises propres à la page (titre, description, partage social…) : la
page est donc relue juste avant, les deux champs fusionnés dans ce qui existe, le reste renvoyé
tel quel. Pas de contrôle de version côté Wix (le dernier écrit gagne) : cette relecture refuse
une page modifiée depuis l'aperçu. Seule la langue principale du site est modifiable.

CONSTATÉ sur un vrai site (04/11/2026) :
- une page classique a un brouillon et une version publiée ; `publish: true` écrit les deux, pour
  cette page seule, en ligne en une vingtaine de secondes. « reset-to-default » ne touche QUE le
  brouillon (la page en ligne gardait l'ancien titre) : on renvoie une liste vide, publiée ;
- un article de blog n'est PAS modifiable : Wix refuse toute écriture (400, InvalidArgument du
  service des brouillons du blog), même des seuls mots-clés, sur chaque article essayé.
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

HOTE = "www.wixapis.com"
SEO = "/promote/seo/v1/item-seo-tags/"
IDENTIFIANT = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
# Une clé API Wix est un long jeton signé (« IST.… ») ; sa forme exacte n'est pas documentée.
CLE = re.compile(r"[A-Za-z0-9._=+/-]{40,4000}")
# Au-delà, une page de liste est anormale (100 éléments, balises de partage comprises).
TAILLE_MAX = 2 * 1024 * 1024
# Pages et produits se cherchent par liste : au plus 20 pages de 100.
PAGES_MAX = 20

REASONS = {
    "acces_refuse": "Clé API refusée ou permission manquante",
    "autre_site": "Identifiant d’un autre site Wix",
    "introuvable": "Page ou article introuvable dans Wix",
    "refus": "Wix a refusé les balises",
    "limite": "Trop de requêtes vers Wix",
    "brouillon": "Enregistré dans le brouillon seulement",
    "collision": "Page modifiée dans Wix depuis l’aperçu",
    "reponse_illisible": "Réponse Wix inexploitable",
    "injoignable": "Wix injoignable ou réponse interrompue",
    "saisie": "Clé API ou identifiant du site mal saisi",
    "enregistrement": "Erreur Noyaru à l’enregistrement",
}

_HTTPS = http.client.HTTPSConnection


def _norm(chemin):
    """Un chemin comparable : décodé, Unicode composé, sans barre finale."""
    return unicodedata.normalize("NFC", unquote(chemin or "")).rstrip("/")


def racine(project_url):
    """L'adresse du site Wix telle que le projet l'analyse : hôte[/préfixe] en HTTPS (un site
    gratuit vit sous un préfixe : compte.wixsite.com/mon-site)."""
    try:
        p = urlsplit(project_url.strip())
        hote = (p.hostname or "").lower()
        if p.scheme != "https" or not hote or p.username or p.password or p.port not in {None, 443}:
            raise ValueError()
        return "https://" + hote.encode("idna").decode("ascii") + p.path.rstrip("/")
    except (ValueError, UnicodeError, AttributeError):
        raise CmsError("L’adresse du projet doit être celle, en HTTPS, de votre site Wix publié.", code="autre_site") from None


def exchange(cle, site_id, method, path, payload=None):
    connection = _HTTPS(HOTE, timeout=12, context=ssl.create_default_context())
    write = method != "GET"
    try:
        connection.request(method, path, body=json.dumps(payload).encode() if payload is not None else None,
                           headers={"Authorization": cle, "wix-site-id": site_id, "Accept": "application/json",
                                    "Content-Type": "application/json", "User-Agent": "Noyaru-CMS/1.0"})
        response = connection.getresponse()
        data = response.read(TAILLE_MAX + 1)
        # Wix contrôle la clé, la permission et les balises AVANT d'enregistrer : ces refus sont certains.
        if response.status in {401, 403}:
            raise CmsError("Wix refuse l’accès. Vérifiez que la clé API a la permission « Manage SEO Settings » et qu’elle couvre ce site.",
                           code="acces_refuse")
        if response.status == 404:
            raise CmsError("Wix ne trouve pas cette page.", code="introuvable")
        if response.status == 400:
            raise CmsError("Wix a refusé ces balises ; rien n’a été enregistré.", code="refus")
        if response.status == 429:
            raise CmsError("Wix limite le nombre de requêtes. Réessayez dans une minute.", code="limite")
        if response.status == 428:
            # DRAFT_SAVED_NOT_PUBLISHED : le brouillon EST enregistré, la version en ligne non.
            raise CmsError("Wix a enregistré la modification dans le brouillon, mais pas sur le site en ligne. Publiez le site depuis l’éditeur Wix, puis vérifiez la page.",
                           uncertain=True, code="brouillon")
        if response.status >= 300 or len(data) > TAILLE_MAX or "json" not in response.getheader("Content-Type", "").lower():
            raise CmsError("Wix n’a pas renvoyé une réponse exploitable.", uncertain=write, code="reponse_illisible")
        return json.loads(data)
    except CmsError:
        raise
    except Exception:
        raise CmsError("La réponse de Wix n’a pas pu être confirmée. Vérifiez la connexion avant de poursuivre.",
                       uncertain=write, code="injoignable") from None
    finally:
        connection.close()


def values(page):
    return values_of(page, "Wix")


def _est_titre(tag):
    return isinstance(tag, dict) and tag.get("type") == "title"


def _est_description(tag):
    props = tag.get("props") if isinstance(tag, dict) else None
    return (tag.get("type") == "meta" and isinstance(props, dict)
            and str(props.get("name") or "").lower() == "description")


def _texte(tag):
    valeur = tag.get("children") if _est_titre(tag) else (tag.get("props") or {}).get("content")
    if not isinstance(valeur, str):
        raise CmsError("Les métadonnées renvoyées par Wix ne sont pas compatibles.")
    return valeur


def _propre(tags, est):
    """La valeur propre à la page (None : elle hérite du modèle de Wix)."""
    trouves = [tag for tag in tags if est(tag)]
    if any(tag.get("disabled") for tag in trouves):
        raise CmsError("Ce champ est désactivé dans les réglages SEO de la page Wix. Réactivez-le dans Wix, puis préparez un nouvel aperçu.")
    return _texte(trouves[0]) if trouves else None


def _rendu(resolved, est):
    for entree in resolved:
        tag = entree.get("tag") if isinstance(entree, dict) else None
        if isinstance(tag, dict) and est(tag) and not tag.get("disabled"):
            return _texte(tag)
    return ""


def fusion(tags, changes):
    """Les balises propres de la page, les deux champs remplacés ; le reste intact, dans l'ordre."""
    resultat = [dict(tag) for tag in tags]
    for name, value in changes.items():
        est = _est_titre if name == "seo_title" else _est_description
        places = [i for i, tag in enumerate(resultat) if est(tag)]
        if value is None:
            resultat = [tag for i, tag in enumerate(resultat) if i not in places]
            continue
        if name == "seo_title":
            nouveau = {**(resultat[places[0]] if places else {"type": "title"}), "children": value, "disabled": False}
        else:
            ancien = resultat[places[0]] if places else {"type": "meta", "props": {"name": "description"}}
            nouveau = {**ancien, "props": {**ancien["props"], "content": value}, "disabled": False}
        if places:
            resultat[places[0]] = nouveau
            resultat = [tag for i, tag in enumerate(resultat) if i not in places[1:]]
        else:
            resultat.append(nouveau)
    return resultat


class WixClient:
    def __init__(self, credentials):
        self.credentials = credentials
        p = urlsplit(credentials["racine"])
        self.hote, self.prefixe = p.hostname, p.path
        self._index = {}

    def request(self, method, path, payload=None):
        c = self.credentials
        return exchange(c["cle"], c["site_id"], method, path, payload)

    def relatif(self, href):
        """Le chemin d'une adresse dans le site Wix ("" pour l'accueil) ; None hors du site.
        Une adresse sans hôte (canonique d'un article : /post/…) est déjà relative au site."""
        try:
            p = urlsplit(href)
        except (ValueError, TypeError):
            return None
        if p.query or p.fragment:
            return None
        if not p.scheme and not p.netloc:
            return _norm(p.path) if p.path.startswith("/") else None
        if (p.scheme != "https" or (p.hostname or "").lower() != self.hote or p.port not in {None, 443}
                or p.username or p.password):
            return None
        if not (p.path.rstrip("/") == self.prefixe or p.path.startswith(self.prefixe + "/")):
            return None
        return _norm(p.path[len(self.prefixe):])

    def contains(self, url):
        return isinstance(url, str) and self.relatif(url) is not None and "://" in url

    def _canonique(self, item):
        """L'adresse canonique PAR DÉFAUT de l'élément : posée à la main, elle peut viser ailleurs."""
        for entree in item.get("resolvedTags") or []:
            tag = entree.get("tag") if isinstance(entree, dict) else None
            if (isinstance(tag, dict) and tag.get("type") == "link"
                    and str((tag.get("props") or {}).get("rel") or "").lower() == "canonical"):
                if entree.get("source") == "TAG_SOURCE_ITEM":
                    tag = entree.get("inheritedTag") if isinstance(entree.get("inheritedTag"), dict) else {}
                href = (tag.get("props") or {}).get("href")
                return self.relatif(href) if isinstance(href, str) else None
        return None

    def _liste(self, kind):
        if kind not in self._index:
            index, curseur = {}, None
            for _ in range(PAGES_MAX):
                suite = "&paging.cursor=" + quote(curseur, safe="") if curseur else ""
                reponse = self.request("GET", f"{SEO}{kind}?paging.limit=100{suite}")
                items = reponse.get("itemSeoTags") if isinstance(reponse, dict) else None
                if not isinstance(items, list):
                    raise CmsError("Wix n’a pas renvoyé une réponse exploitable.", code="reponse_illisible")
                for item in items:
                    chemin = self._canonique(item) if isinstance(item, dict) else None
                    if chemin is not None and isinstance(item.get("itemId"), str):
                        index.setdefault(chemin, []).append(item["itemId"])
                pagination = reponse.get("pagingMetadata") or {}
                curseur = (pagination.get("cursors") or {}).get("next")
                if not pagination.get("hasNext") or not curseur:
                    break
            self._index[kind] = index
        return self._index[kind]

    def check(self):
        # Une page du site doit avoir sa canonique sous l'adresse du projet : la clé et
        # l'identifiant désignent bien CE site, et la lecture des balises prouve la permission.
        if "" not in self._liste("STATIC_PAGE"):
            raise CmsError("Cette clé et cet identifiant désignent un autre site Wix que celui de ce projet, ou ce site est publié à une autre adresse.",
                           code="autre_site")
        return "Wix — " + (self.hote + self.prefixe)

    def _element(self, kind, ident):
        reponse = self.request("GET", SEO + kind + "/" + quote(ident, safe=""))
        item = reponse.get("itemSeoTags") if isinstance(reponse, dict) else None
        if not isinstance(item, dict) or item.get("itemId") != ident or not isinstance(item.get("tags"), list):
            raise CmsError("Wix n’a pas renvoyé une réponse exploitable.", code="reponse_illisible")
        return item

    def read(self, url):
        chemin = self.relatif(url) if "://" in str(url) else None
        if chemin is None:
            raise CmsError("Cette page ne se trouve pas dans le site Wix connecté.")
        if chemin.startswith("/post/"):
            raise CmsError("Wix ne permet pas encore de modifier un article de blog depuis une autre application. Corrigez cet article dans Wix, avec les instructions guidées.")
        kind = "STORES_PRODUCT" if chemin.startswith("/product-page/") else "STATIC_PAGE"
        trouves = self._liste(kind).get(chemin) or []
        if len(trouves) != 1:
            raise CmsError("Wix ne trouve pas de page publiée à cette adresse dans la langue principale du site.")
        item = self._element(kind, trouves[0])
        if kind != "STATIC_PAGE" and item.get("publishStatus") != "PUBLISH_STATUS_PUBLISHED":
            raise CmsError("Wix ne trouve pas de page publiée à cette adresse dans la langue principale du site.")
        return self.page(item, kind, url)

    def page(self, item, kind, url, titre=""):
        tags, resolved = item.get("tags"), item.get("resolvedTags") or []
        if not isinstance(tags, list) or not isinstance(resolved, list) or not all(isinstance(t, dict) for t in tags):
            raise CmsError("Wix n’a pas renvoyé une réponse exploitable.", code="reponse_illisible")
        rendu_titre = _rendu(resolved, _est_titre)
        page = {"post_id": f"{kind}:{item['itemId']}", "permalink": url, "post_title": titre or rendu_titre,
                "seo_title": _propre(tags, _est_titre), "meta_description": _propre(tags, _est_description),
                # Ce que la page affiche sans valeur propre : le modèle SEO de Wix pour ce type de page.
                "seo_title_rendered": rendu_titre, "meta_description_rendered": _rendu(resolved, _est_description),
                "tags": tags}
        values(page)
        return page

    def update(self, page, changes):
        if not changes or not changes.keys() <= FIELDS:
            raise CmsError("Seuls le titre SEO et la méta-description sont pris en charge.")
        kind, ident = page["post_id"].split(":", 1)
        # Wix remplace toutes les balises propres et ne contrôle aucune version : relire juste avant.
        actuel = self.page(self._element(kind, ident), kind, page["permalink"], page["post_title"])
        if values(actuel) != values(page) or actuel["tags"] != page["tags"]:
            raise CmsError("La page a changé dans Wix depuis l’aperçu. L’opération est arrêtée pour conserver les modifications actuelles.",
                           code="collision")
        tags = fusion(actuel["tags"], changes)
        chemin = SEO + kind + "/" + quote(ident, safe="")
        if kind == "STATIC_PAGE":
            # Brouillon ET version en ligne ; une liste vide publiée rend la page au modèle (le
            # « reset-to-default » de Wix laissait l'ancien titre en ligne).
            reponse = self.request("PATCH", chemin, {"itemSeoTags": {"tags": tags}, "fieldMask": "tags", "publish": True})
        elif tags:
            reponse = self.request("PATCH", chemin, {"itemSeoTags": {"tags": tags}, "fieldMask": "tags"})
        else:
            # Un produit n'a pas de brouillon : Wix demande ce retour au modèle plutôt qu'une liste vide.
            reponse = self.request("POST", chemin + "/reset-to-default")
        try:
            item = reponse.get("itemSeoTags") if isinstance(reponse, dict) else None
            if not isinstance(item, dict) or item.get("itemId") != ident:
                raise CmsError("Wix n’a pas renvoyé une réponse exploitable.")
            written = self.page(item, kind, page["permalink"], page["post_title"])
            if any(written[name] != value for name, value in changes.items()):
                raise CmsError("Les valeurs publiées diffèrent de l’aperçu validé.")
        except CmsError as error:
            raise CmsError(str(error), uncertain=True) from None
        return written


def identifiants(form, project):
    """Le formulaire de connexion -> identifiants (clé et identifiant bien formés, adresse du projet)."""
    cle = str(form.get("cle") or "").strip()
    site_id = str(form.get("site_id") or "").strip().lower()
    if not CLE.fullmatch(cle) or not IDENTIFIANT.fullmatch(site_id):
        raise CmsError("Collez la clé API Wix et l’identifiant du site (de la forme 1a2b3c4d-…, dans l’adresse du tableau de bord Wix).",
                       code="saisie")
    return {"cle": cle, "site_id": site_id, "racine": racine(project.base_url)}


def recharger(value, project):
    if not CLE.fullmatch(str(value.get("cle") or "")) or not IDENTIFIANT.fullmatch(str(value.get("site_id") or "")):
        raise ValueError()
    return {**value, "racine": racine(project.base_url)}


CONNECTEUR = Connecteur(
    nom="wix", libelle="Wix", libelle_complet="Wix", provider="wix_api_key",
    client=lambda c: WixClient(c), identifiants=identifiants, recharger=recharger,
    plateforme_ok=lambda site: site["platform"] == "wix",
    message_plateforme="Choisissez Wix pour utiliser cette connexion.",
    values=values, raisons=REASONS, champs=("site_id", "cle"))
