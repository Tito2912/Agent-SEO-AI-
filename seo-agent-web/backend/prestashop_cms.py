"""PrestaShop (Webservice) : titre SEO et méta-description d'un produit ou d'une catégorie.

Troisième connecteur (02/11/2026), sur le parcours commun de `cms_routes`. Le client crée une clé
Webservice (Paramètres avancés → Webservice) avec la lecture et la modification (PATCH) des
produits et des catégories, et la lecture des langues. PATCH n'existe que depuis PrestaShop 8 :
la version 1.7 n'a que PUT, qui renvoie la fiche entière — refusé ici.

Constaté sur un vrai PrestaShop 8.2.8 (02/11/2026), et qui commande ce module :
- un champ vide est ABSENT de la réponse JSON (pas vide) : absent = None ;
- un PATCH qui n'envoie qu'UNE langue efface les autres et répond 500 en écrivant quand même :
  chaque écriture renvoie TOUTES les langues du champ, seule celle de la page change ;
- un PATCH de page CMS réenregistre son contenu (texte modifié) et peut échouer en 500 : les pages
  CMS ne sont pas modifiées par ce connecteur ;
- sur un produit ou une catégorie, seuls le champ envoyé et `date_upd` changent en base.
La réponse XML (racine de l'API) n'est jamais analysée par un parseur XML : on y lit des attributs.
"""
from __future__ import annotations

import base64
import html
import json
import re
from urllib.parse import unquote, urlsplit, urlunsplit

try:
    from .cms_commun import FIELDS, CmsError, Connecteur, HTTPSEpingle, adresse_publique, values_of
except ImportError:
    from cms_commun import FIELDS, CmsError, Connecteur, HTTPSEpingle, adresse_publique, values_of

CLE = re.compile(r"[A-Z0-9]{32}")
CHAMPS = {"seo_title": "meta_title", "meta_description": "meta_description"}
# Ressource Webservice -> élément XML de l'objet ; repli d'affichage quand le champ SEO est vide.
RESSOURCES = {"products": ("product", "description_short"), "categories": ("category", "description")}
DROITS = {"products": ("get", "patch"), "categories": ("get", "patch"), "languages": ("get",)}
# Limites de PrestaShop (ObjectModel) : au-delà, il refuse l'enregistrement.
LIMITES = {"seo_title": 255, "meta_description": 512}

REASONS = {
    "version": "PrestaShop < 8 (pas de PATCH) ou Webservice introuvable",
    "desactive": "Webservice désactivé dans la boutique",
    "droits": "Clé sans les droits produits / catégories / langues",
    "acces_refuse": "Clé Webservice refusée",
    "adresse": "Adresse hors du domaine du projet ou invalide",
    "adresse_privee": "Adresse non publique",
    "redirection": "PrestaShop redirige l’adresse",
    "reponse_illisible": "Réponse PrestaShop inexploitable",
    "injoignable": "PrestaShop injoignable ou réponse interrompue",
    "saisie": "Clé Webservice mal saisie",
    "enregistrement": "Erreur Noyaru à l’enregistrement",
}


def _hote(url):
    return (urlsplit(url).hostname or "").lower()


def site_url(value, project_url):
    try:
        parts = urlsplit(value.strip())
        host = (parts.hostname or "").lower()
        if (parts.scheme != "https" or not host or parts.username or parts.password or parts.query or parts.fragment
                or parts.port not in {None, 443} or host != _hote(project_url)
                or any(ord(ch) < 32 or ch == "\\" for ch in unquote(value))
                or any(segment in {".", ".."} for segment in unquote(parts.path).split("/"))):
            raise ValueError()
        path = parts.path.rstrip("/")
        if path.endswith("/api"):
            path = path[:-len("/api")]
        return urlunsplit(("https", host.encode("idna").decode("ascii"), path, "", ""))
    except (ValueError, UnicodeError, AttributeError):
        raise CmsError("Utilisez l’adresse HTTPS de votre boutique PrestaShop, sur le même domaine que ce projet.",
                       code="adresse") from None


def public_address(host):
    return adresse_publique(host, "PrestaShop")


_PinnedHTTPS = HTTPSEpingle


def exchange(base_url, cle, method, path, body=None):
    """Une requête au Webservice. Rend (version annoncée par PrestaShop, corps texte)."""
    parsed = urlsplit(base_url)
    address = public_address(parsed.hostname)
    connection = _PinnedHTTPS(parsed.hostname, address)
    write = method == "PATCH"
    try:
        connection.request(method, parsed.path.rstrip("/") + "/api/" + path, body=body,
                           headers={"Authorization": "Basic " + base64.b64encode((cle + ":").encode()).decode(),
                                    "Accept": "application/json, text/xml", "User-Agent": "Noyaru-CMS/1.0",
                                    **({"Content-Type": "application/xml"} if body is not None else {})})
        response = connection.getresponse()
        data = response.read(1048577)
        if 300 <= response.status < 400:
            raise CmsError("PrestaShop redirige cette adresse. Utilisez l’adresse HTTPS exacte de la boutique.",
                           uncertain=write, code="redirection")
        if response.status == 401:
            raise CmsError("PrestaShop refuse la clé Webservice. Vérifiez la clé et qu’elle est activée.",
                           uncertain=write, code="acces_refuse")
        if response.status == 503:
            raise CmsError("Le Webservice de la boutique est désactivé. Activez-le dans Paramètres avancés → Webservice.",
                           uncertain=write, code="desactive")
        if response.status in {403, 405}:
            raise CmsError("La clé Webservice n’a pas les droits nécessaires : lecture et modification des produits et des catégories, lecture des langues.",
                           uncertain=write, code="droits")
        if response.status == 404:
            raise CmsError("PrestaShop ne trouve pas cette ressource.", uncertain=write, code="introuvable")
        if response.status >= 400 or len(data) > 1048576:
            # Constaté : un PATCH peut répondre 500 en ayant écrit. Le doute est toujours dit.
            raise CmsError("PrestaShop n’a pas renvoyé une réponse exploitable.", uncertain=write, code="reponse_illisible")
        return str(response.getheader("PSWS-Version", "") or ""), data.decode("utf-8")
    except CmsError:
        raise
    except Exception:
        raise CmsError("La réponse de PrestaShop n’a pas pu être confirmée. Vérifiez la connexion avant de poursuivre.",
                       uncertain=write, code="injoignable") from None
    finally:
        connection.close()


def values(page):
    return values_of(page, "PrestaShop")


def _texte(value):
    """Un champ multilingue lu pour UNE langue : chaîne ; vide ou absent -> None."""
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise CmsError("Les métadonnées renvoyées par PrestaShop ne sont pas compatibles.")
    return value


def _sans_balises(value):
    return " ".join(html.unescape(re.sub(r"<[^>]*>", " ", str(value or ""))).split())


class PrestaShopClient:
    def __init__(self, credentials):
        self.credentials = credentials
        self._langues = None

    def request(self, method, path, body=None):
        c = self.credentials
        return exchange(c["site_url"], c["cle"], method, path, body)

    def json(self, path):
        separateur = "&" if "?" in path else "?"
        _, texte = self.request("GET", path + separateur + "output_format=JSON")
        try:
            return json.loads(texte)
        except ValueError:
            raise CmsError("PrestaShop n’a pas renvoyé une réponse exploitable.", code="reponse_illisible") from None

    def check(self):
        try:
            version, racine = self.request("GET", "")
        except CmsError as error:
            if error.code == "introuvable":
                raise CmsError("Cette adresse ne répond pas comme le Webservice de PrestaShop 8 ou supérieur.", code="version") from None
            raise
        majeure = re.match(r"\d+", version)
        if not majeure or int(majeure.group()) < 8:
            raise CmsError("Noyaru modifie les boutiques PrestaShop 8 ou supérieur : la version 1.7 ne permet pas de modifier un seul champ.",
                           code="version")
        for ressource, methodes in DROITS.items():
            balise = re.search(r"<%s\b[^>]*>" % ressource, racine)
            if not balise or any(f'{methode}="true"' not in balise.group() for methode in methodes):
                raise CmsError("La clé Webservice n’a pas les droits nécessaires : lecture et modification des produits et des catégories, lecture des langues.",
                               code="droits")
        if not self.langues():
            raise CmsError("PrestaShop n’a pas renvoyé une réponse exploitable.", code="reponse_illisible")
        nom = re.search(r'<api\b[^>]*\bshopName="([^"]*)"', racine)
        return html.unescape(nom.group(1)) if nom and nom.group(1) else "Boutique PrestaShop"

    def langues(self):
        """{iso: id} des langues actives."""
        if self._langues is None:
            liste = self.json("languages?display=[id,iso_code,active]").get("languages")
            if not isinstance(liste, list):
                raise CmsError("PrestaShop n’a pas renvoyé une réponse exploitable.", code="reponse_illisible")
            self._langues = {str(l.get("iso_code") or "").lower(): int(l["id"]) for l in liste
                             if isinstance(l, dict) and str(l.get("active")) == "1" and str(l.get("id", "")).isdigit()}
        return self._langues

    def contains(self, url):
        try:
            p = urlsplit(url)
            return (p.scheme == "https" and (p.hostname or "").lower() == self.credentials["domaine"]
                    and p.port in {None, 443} and not p.username and not p.password)
        except (ValueError, TypeError):
            return False

    def _segments(self, url):
        """(id de langue, segments du chemin sous la boutique) ; la langue vient du préfixe /fr/."""
        base = urlsplit(self.credentials["site_url"]).path.rstrip("/")
        chemin = unquote(urlsplit(url).path)
        if base and not (chemin + "/").startswith(base + "/"):
            raise CmsError("Cette page ne se trouve pas dans la boutique PrestaShop connectée.")
        segments = [s for s in chemin[len(base):].split("/") if s]
        langues = self.langues()
        if segments and segments[0].lower() in langues:
            return langues[segments[0].lower()], segments[1:]
        if len(langues) == 1:
            return next(iter(langues.values())), segments
        raise CmsError("Cette adresse ne dit pas sa langue (/fr/, /en/…) : Noyaru ne peut pas savoir quelle traduction modifier.")

    def read(self, url):
        if not self.contains(url):
            raise CmsError("Cette page ne se trouve pas dans la boutique PrestaShop connectée.")
        if urlsplit(url).query:
            raise CmsError("Cette adresse porte des paramètres : seule la page d’un produit ou d’une catégorie, sans filtre, est modifiable.")
        langue, segments = self._segments(url)
        if len(segments) >= 2 and segments[-2] == "content":
            raise CmsError("Les pages CMS de PrestaShop ne sont pas modifiées par Noyaru : PrestaShop réenregistrerait aussi leur contenu. Suivez les instructions pour les corriger vous-même.")
        candidats = []
        dernier = segments[-1] if segments else ""
        produit = re.fullmatch(r"(\d+)-(.+)\.html", dernier)
        if produit:
            ident, reste = produit.groups()
            variantes = [reste, re.sub(r"-\d{8,13}$", "", reste)]
            declinaison = re.fullmatch(r"\d+-(.+)", reste)
            if declinaison:
                variantes += [declinaison.group(1), re.sub(r"-\d{8,13}$", "", declinaison.group(1))]
            candidats = [("products", ident, dict.fromkeys(variantes))]
        elif len(segments) == 1 and re.fullmatch(r"(\d+)-(.+)", dernier):
            ident, reste = re.fullmatch(r"(\d+)-(.+)", dernier).groups()
            candidats = [("categories", ident, {reste: None})]
        for ressource, ident, reecritures in candidats:
            try:
                page = self._lire(ressource, int(ident), langue)
            except CmsError as error:
                if error.code == "introuvable":
                    continue
                raise
            if page["link_rewrite"] in reecritures:
                if page["active"] != "1":
                    raise CmsError("Cette page n’est pas publiée dans PrestaShop.")
                return {**page, "permalink": url}
        raise CmsError("PrestaShop ne trouve pas de produit ou de catégorie à cette adresse.")

    def _lire(self, ressource, ident, langue):
        cle, repli = RESSOURCES[ressource]
        objet = self.json(f"{ressource}/{ident}?language={langue}").get(cle)
        if not isinstance(objet, dict) or str(objet.get("id")) != str(ident):
            raise CmsError("La réponse ne correspond pas au produit ou à la catégorie attendu.")
        page = {"post_id": f"{ressource}:{ident}:{langue}", "post_title": str(objet.get("name") or ""),
                "seo_title": _texte(objet.get("meta_title")), "meta_description": _texte(objet.get("meta_description")),
                "link_rewrite": str(objet.get("link_rewrite") or ""), "active": str(objet.get("active") or "")}
        # Ce que le thème affiche quand le champ est vide (constaté) : le nom, la description sans balises.
        page["seo_title_rendered"] = page["seo_title"] or page["post_title"]
        page["meta_description_rendered"] = page["meta_description"] or _sans_balises(objet.get(repli))
        values(page)
        return page

    def update(self, page, changes):
        if not changes or not changes.keys() <= FIELDS:
            raise CmsError("Seuls le titre SEO et la méta-description sont pris en charge.")
        if any(value is not None and len(value) > LIMITES[name] for name, value in changes.items()):
            raise CmsError("PrestaShop limite le titre SEO à 255 caractères et la méta-description à 512.")
        ressource, ident, langue = page["post_id"].split(":")
        cle = RESSOURCES[ressource][0]
        # TOUTES les langues du champ, relues juste avant : n'en envoyer qu'une effacerait les autres.
        champs = ",".join(CHAMPS[name] for name in changes)
        objet = self.json(f"{ressource}/{ident}?display=[id,{champs}]").get(ressource)
        objet = objet[0] if isinstance(objet, list) and len(objet) == 1 else None
        if not isinstance(objet, dict) or str(objet.get("id")) != ident:
            raise CmsError("La réponse ne correspond pas au produit ou à la catégorie attendu.")
        xml = ""
        for name, value in changes.items():
            actuelles = _par_langue(objet.get(CHAMPS[name]), self.langues())
            actuelles[int(langue)] = value or ""
            noeuds = "".join(f'<language id="{lid}">{html.escape(texte or "", quote=False)}</language>'
                             for lid, texte in sorted(actuelles.items()))
            xml += f"<{CHAMPS[name]}>{noeuds}</{CHAMPS[name]}>"
        corps = (f'<prestashop xmlns:xlink="http://www.w3.org/1999/xlink"><{cle}><id>{ident}</id>{xml}</{cle}>'
                 f'</prestashop>').encode()
        self.request("PATCH", f"{ressource}/{ident}?output_format=JSON", corps)
        try:
            written = {**self._lire(ressource, int(ident), int(langue)), "permalink": page["permalink"]}
            if any(written[name] != (value or None) for name, value in changes.items()):
                raise CmsError("Les valeurs publiées diffèrent de l’aperçu validé.")
        except CmsError as error:
            raise CmsError(str(error), uncertain=True) from None
        return written


def _par_langue(valeur, langues):
    """Un champ lu sans `language=` : liste [{id, value}] (boutique multilingue), chaîne (une seule
    langue), ou ABSENT — PrestaShop omet un champ vide, ici vide dans toutes les langues."""
    if isinstance(valeur, list):
        return {int(entree["id"]): str(entree.get("value") or "") for entree in valeur
                if isinstance(entree, dict) and str(entree.get("id", "")).isdigit()}
    if valeur is None:
        return {lid: "" for lid in langues.values()}
    if isinstance(valeur, str) and len(langues) == 1:
        return {next(iter(langues.values())): valeur}
    raise CmsError("PrestaShop n’a pas renvoyé toutes les langues du champ : rien n’est écrit.")


def identifiants(form, project):
    cle = str(form.get("cle") or "").strip()
    if not CLE.fullmatch(cle):
        raise CmsError("Collez la clé Webservice de votre boutique (32 lettres majuscules et chiffres).", code="saisie")
    return {"site_url": site_url(str(form.get("site_url") or ""), project.base_url), "cle": cle,
            "domaine": _hote(project.base_url)}


def recharger(value, project):
    if not CLE.fullmatch(str(value.get("cle") or "")):
        raise ValueError()
    return {**value, "site_url": site_url(value["site_url"], project.base_url), "domaine": _hote(project.base_url)}


CONNECTEUR = Connecteur(
    nom="prestashop", libelle="PrestaShop", libelle_complet="PrestaShop", provider="prestashop_webservice",
    client=lambda c: PrestaShopClient(c), identifiants=identifiants, recharger=recharger,
    plateforme_ok=lambda site: site["platform"] == "prestashop",
    message_plateforme="Choisissez PrestaShop pour utiliser cette connexion.",
    values=values, raisons=REASONS, champs=("site_url", "cle"))
