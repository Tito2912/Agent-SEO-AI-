"""Ghost (Admin API) : titre SEO et méta-description d'un article ou d'une page publiés.

Deuxième connecteur (02/11/2026), sur le parcours commun de `cms_routes`. Le client colle la clé
Admin API d'une intégration personnalisée (« identifiant:secret ») ; chaque requête porte un jeton
HS256 de quelques minutes signé avec ce secret. Comme pour WordPress, la connexion HTTPS est
épinglée sur une adresse publique validée, et rien n'est envoyé hors du domaine du projet — sauf
l'administration Ghost(Pro), qui vit en <site>.ghost.io quand le site a son propre domaine.

Ghost refuse une modification si `updated_at` n'est plus celui de la lecture (détection de
collision) : une page modifiée depuis l'aperçu n'est jamais écrasée.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import time
from urllib.parse import quote, unquote, urlsplit, urlunsplit

try:
    from .cms_commun import FIELDS, CmsError, Connecteur, HTTPSEpingle, adresse_publique, same_page, values_of
except ImportError:
    from cms_commun import FIELDS, CmsError, Connecteur, HTTPSEpingle, adresse_publique, same_page, values_of

API = "/ghost/api/admin/"
# La version minimale de l'API ; Ghost 5 est la premiere a l'accepter.
VERSION = "v5.0"
CLE = re.compile(r"[0-9a-f]{24}:[0-9a-f]{64}")
# Limites des champs dans Ghost : au-dela, il refuse l'enregistrement entier.
LIMITES = {"seo_title": 300, "meta_description": 500}
GHOST = {"seo_title": "meta_title", "meta_description": "meta_description"}

REASONS = {
    "version": "Ghost < 5 ou API Admin absente",
    "acces_refuse": "Clé Admin API refusée",
    "autre_site": "Clé d’un autre site Ghost",
    "adresse": "Adresse hors du domaine du projet ou invalide",
    "adresse_privee": "Adresse non publique",
    "redirection": "Ghost redirige l’adresse",
    "reponse_illisible": "Réponse Ghost inexploitable",
    "injoignable": "Ghost injoignable ou réponse interrompue",
    "saisie": "Clé Admin API mal saisie",
    "enregistrement": "Erreur Noyaru à l’enregistrement",
}


def _hote(url):
    return (urlsplit(url).hostname or "").lower()


def site_url(value, project_url):
    try:
        parts = urlsplit(value.strip())
        host = (parts.hostname or "").lower()
        if (parts.scheme != "https" or not host or parts.username or parts.password or parts.query or parts.fragment
                or parts.port not in {None, 443} or not (host == _hote(project_url) or host.endswith(".ghost.io"))
                or any(ord(ch) < 32 or ch == "\\" for ch in unquote(value))
                or any(segment in {".", ".."} for segment in unquote(parts.path).split("/"))):
            raise ValueError()
        path = parts.path.rstrip("/")
        # L'adresse de l'administration se colle souvent telle quelle : …/ghost/
        if path.endswith("/ghost"):
            path = path[:-len("/ghost")]
        return urlunsplit(("https", host.encode("idna").decode("ascii"), path, "", ""))
    except (ValueError, UnicodeError, AttributeError):
        raise CmsError("Utilisez l’adresse HTTPS de votre site Ghost, sur le même domaine que ce projet, "
                       "ou celle de son administration en .ghost.io.", code="adresse") from None


def jeton(cle, maintenant=None):
    """Le jeton d'une requête : HS256, `kid` = identifiant de la clé, audience /admin/, 4 minutes."""
    ident, secret = cle.split(":", 1)
    iat = int(time.time() if maintenant is None else maintenant)

    def b64(data):
        return base64.urlsafe_b64encode(data).rstrip(b"=")

    signe = (b64(json.dumps({"alg": "HS256", "kid": ident, "typ": "JWT"}, separators=(",", ":")).encode()) + b"."
             + b64(json.dumps({"iat": iat, "exp": iat + 240, "aud": "/admin/"}, separators=(",", ":")).encode()))
    return (signe + b"." + b64(hmac.new(bytes.fromhex(secret), signe, hashlib.sha256).digest())).decode()


def public_address(host):
    return adresse_publique(host, "Ghost")


_PinnedHTTPS = HTTPSEpingle


def exchange(base_url, cle, method, path, payload=None):
    parsed = urlsplit(base_url)
    address = public_address(parsed.hostname)
    connection = _PinnedHTTPS(parsed.hostname, address)
    write = method == "PUT"
    try:
        connection.request(method, parsed.path.rstrip("/") + API + path,
                           body=json.dumps(payload).encode() if payload is not None else None,
                           headers={"Authorization": "Ghost " + jeton(cle), "Accept-Version": VERSION,
                                    "Accept": "application/json", "Content-Type": "application/json",
                                    "User-Agent": "Noyaru-CMS/1.0"})
        response = connection.getresponse()
        data = response.read(262145)
        if 300 <= response.status < 400:
            raise CmsError("Ghost redirige cette adresse. Utilisez l’adresse où vous vous connectez à l’administration Ghost.",
                           uncertain=write, code="redirection")
        if response.status in {401, 403}:
            raise CmsError("Ghost refuse l’accès. Vérifiez la clé Admin API de l’intégration personnalisée.",
                           uncertain=write, code="acces_refuse")
        if response.status == 404:
            raise CmsError("Ghost ne trouve pas cette ressource.", uncertain=write, code="introuvable")
        if response.status == 409:
            # Collision : la page a change depuis la lecture, Ghost n'a rien enregistre.
            raise CmsError("La page a changé dans Ghost depuis l’aperçu. L’opération est arrêtée pour conserver les modifications actuelles.",
                           code="collision")
        if response.status >= 400 or len(data) > 262144 or "json" not in response.getheader("Content-Type", "").lower():
            raise CmsError("Ghost n’a pas renvoyé une réponse exploitable.", uncertain=write, code="reponse_illisible")
        return json.loads(data)
    except CmsError:
        raise
    except Exception:
        raise CmsError("La réponse de Ghost n’a pas pu être confirmée. Vérifiez la connexion avant de poursuivre.",
                       uncertain=write, code="injoignable") from None
    finally:
        connection.close()


def values(page):
    return values_of(page, "Ghost")


class GhostClient:
    def __init__(self, credentials):
        self.credentials = credentials

    def request(self, method, path, payload=None):
        c = self.credentials
        return exchange(c["site_url"], c["cle"], method, path, payload)

    def check(self):
        try:
            site = self.request("GET", "site/")
        except CmsError as error:
            if error.code == "introuvable":
                raise CmsError("Cette adresse ne répond pas comme un site Ghost 5 ou supérieur. Vérifiez l’adresse et la version de Ghost.",
                               code="version") from None
            raise
        site = site.get("site") if isinstance(site, dict) else None
        if not isinstance(site, dict) or not isinstance(site.get("url"), str):
            raise CmsError("Ghost n’a pas renvoyé une réponse exploitable.", code="reponse_illisible")
        version = str(site.get("version") or "")
        if not re.match(r"\d+", version) or int(re.match(r"\d+", version).group()) < 5:
            raise CmsError("Cette adresse ne répond pas comme un site Ghost 5 ou supérieur. Vérifiez l’adresse et la version de Ghost.",
                           code="version")
        if _hote(site["url"]) != self.credentials["domaine"]:
            raise CmsError("Cette clé appartient à un autre site Ghost que celui de ce projet.", code="autre_site")
        # /site/ répond sans clé : une lecture authentifiée prouve que la clé est acceptée.
        self.request("GET", "posts/?limit=1&fields=id")
        return str(site.get("title") or "Site Ghost")

    def contains(self, url):
        try:
            p = urlsplit(url)
            return (p.scheme == "https" and (p.hostname or "").lower() == self.credentials["domaine"]
                    and p.port in {None, 443} and not p.username and not p.password)
        except (ValueError, TypeError):
            return False

    def read(self, url):
        if not self.contains(url):
            raise CmsError("Cette page ne se trouve pas dans le site Ghost connecté.")
        segments = [segment for segment in urlsplit(url).path.split("/") if segment]
        if not segments:
            raise CmsError("La page d’accueil se règle dans Ghost (Paramètres → Général → Méta-données), pas article par article.")
        slug = quote(unquote(segments[-1]), safe="")
        for kind in ("posts", "pages"):
            try:
                result = self.request("GET", f"{kind}/slug/{slug}/")
            except CmsError as error:
                if error.code == "introuvable":
                    continue
                raise
            items = result.get(kind) if isinstance(result, dict) else None
            if isinstance(items, list) and len(items) == 1:
                return self.validate_page(items[0], kind, url)
        raise CmsError("Ghost ne trouve pas d’article ou de page publié à cette adresse.")

    def validate_page(self, item, kind, url):
        if (not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"]
                or item.get("status") != "published" or not isinstance(item.get("updated_at"), str)
                or not same_page(item.get("url"), url)):
            raise CmsError("La réponse ne correspond pas à la page ou à l’article publié attendu.")
        page = {"post_id": f"{kind}:{item['id']}", "permalink": item["url"], "post_title": str(item.get("title") or ""),
                "seo_title": item.get("meta_title"), "meta_description": item.get("meta_description"),
                "updated_at": item["updated_at"],
                # Ce que le thème affiche quand le champ est vide : le titre, l'extrait personnalisé.
                "seo_title_rendered": item.get("meta_title") or item.get("title") or "",
                "meta_description_rendered": item.get("meta_description") or item.get("custom_excerpt") or ""}
        values(page)
        return page

    def update(self, page, changes):
        if not changes or not changes.keys() <= FIELDS:
            raise CmsError("Seuls le titre SEO et la méta-description sont pris en charge.")
        if any(value is not None and len(value) > LIMITES[name] for name, value in changes.items()):
            raise CmsError("Ghost limite le titre SEO à 300 caractères et la méta-description à 500.")
        kind, ident = page["post_id"].split(":", 1)
        result = self.request("PUT", f"{kind}/{quote(ident, safe='')}/", {
            kind: [{"updated_at": page["updated_at"], **{GHOST[name]: value for name, value in changes.items()}}]})
        try:
            items = result.get(kind) if isinstance(result, dict) else None
            if not isinstance(items, list) or len(items) != 1:
                raise CmsError("Ghost n’a pas renvoyé une réponse exploitable.")
            written = self.validate_page(items[0], kind, page["permalink"])
            if written["post_id"] != page["post_id"] or any(written[name] != value for name, value in changes.items()):
                raise CmsError("Les valeurs publiées diffèrent de l’aperçu validé.")
        except CmsError as error:
            raise CmsError(str(error), uncertain=True) from None
        return written


def identifiants(form, project):
    """Le formulaire de connexion -> identifiants (adresse validée, clé bien formée, domaine du projet)."""
    cle = str(form.get("cle") or "").strip()
    if not CLE.fullmatch(cle):
        raise CmsError("Collez la clé Admin API de votre intégration personnalisée Ghost (de la forme identifiant:secret).",
                       code="saisie")
    return {"site_url": site_url(str(form.get("site_url") or ""), project.base_url), "cle": cle,
            "domaine": _hote(project.base_url)}


def recharger(value, project):
    if not CLE.fullmatch(str(value.get("cle") or "")):
        raise ValueError()
    return {**value, "site_url": site_url(value["site_url"], project.base_url), "domaine": _hote(project.base_url)}


CONNECTEUR = Connecteur(
    nom="ghost", libelle="Ghost", libelle_complet="Ghost", provider="ghost_admin",
    client=lambda c: GhostClient(c), identifiants=identifiants, recharger=recharger,
    plateforme_ok=lambda site: site["platform"] == "ghost",
    message_plateforme="Choisissez Ghost pour utiliser cette connexion.",
    values=values, raisons=REASONS, champs=("site_url", "cle"))
