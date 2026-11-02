"""Yoast's official WordPress Abilities API, using HTTPS pinned to validated public IPs."""
from __future__ import annotations

import base64
import json
from urllib.parse import unquote, urlencode, urlsplit, urlunsplit

try:
    # Le socle commun des connecteurs (02/11/2026) : ces noms restent ceux de ce module.
    from .cms_commun import FIELDS, CmsError, Connecteur, HTTPSEpingle, adresse_publique, fingerprint, same_connection, same_page
except ImportError:
    from cms_commun import FIELDS, CmsError, Connecteur, HTTPSEpingle, adresse_publique, fingerprint, same_connection, same_page

__all__ = ["FIELDS", "CmsError", "fingerprint", "same_connection", "same_page", "WordPressClient", "CONNECTEUR"]

READ = "yoast-seo/get-post-seo-data"
WRITE = "yoast-seo/update-post-seo-data"


# Why a connection was declined, as read on /settings/operations.
REASONS = {
    "version": "WordPress < 6.9 ou Yoast < 28.7 (fonctions absentes)",
    "yoast_ancien": "Yoast sans écriture des titres et descriptions",
    "droits": "Compte sans droit de modifier Yoast",
    "acces_refuse": "Mot de passe d’application refusé",
    "compte_inconnu": "Compte WordPress non identifié",
    "adresse": "Adresse hors du domaine du projet ou invalide",
    "adresse_privee": "Adresse non publique",
    "redirection": "WordPress redirige l’adresse",
    "reponse_illisible": "Réponse WordPress inexploitable",
    "injoignable": "WordPress injoignable ou réponse interrompue",
    "saisie": "Identifiant ou mot de passe mal saisi",
    "enregistrement": "Erreur Noyaru à l’enregistrement",
}


def site_url(value, project_url):
    try:
        parts, project = urlsplit(value.strip()), urlsplit(project_url)
        if (parts.scheme != "https" or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment
                or parts.port not in {None, 443} or parts.hostname.lower() != (project.hostname or "").lower()
                or any(ord(ch) < 32 or ch == "\\" for ch in unquote(value))
                or any(segment in {".", ".."} for segment in unquote(parts.path).split("/"))):
            raise ValueError()
        host = parts.hostname.encode("idna").decode("ascii").lower()
        return urlunsplit(("https", host, parts.path.rstrip("/"), "", ""))
    except (ValueError, UnicodeError, AttributeError):
        raise CmsError("Utilisez l’adresse HTTPS de WordPress, sur le même domaine que ce projet.", code="adresse") from None


def public_address(host):
    return adresse_publique(host, "WordPress")


# Module-level names, so a test can replace the connection without touching other connectors.
_PinnedHTTPS = HTTPSEpingle


def exchange(base_url, username, password, method, path, payload=None):
    parsed = urlsplit(base_url)
    address = public_address(parsed.hostname)
    connection = _PinnedHTTPS(parsed.hostname, address)
    write = method == "POST"
    try:
        authorization = base64.b64encode(f"{username}:{password}".encode()).decode()
        connection.request(method, parsed.path.rstrip("/") + "/wp-json/" + path,
                           body=json.dumps(payload).encode() if payload is not None else None,
                           headers={"Authorization": "Basic " + authorization, "Accept": "application/json",
                                    "Content-Type": "application/json", "User-Agent": "Noyaru-CMS/1.0"})
        response = connection.getresponse()
        data = response.read(262145)
        if 300 <= response.status < 400:
            raise CmsError("WordPress redirige cette adresse. Utilisez son adresse HTTPS finale.", uncertain=write, code="redirection")
        if response.status in {401, 403}:
            raise CmsError("WordPress refuse l’accès. Vérifiez le mot de passe d’application et les droits de modification Yoast.", uncertain=write, code="acces_refuse")
        if response.status == 404:
            raise CmsError("Cette fonction n’est pas disponible. Vérifiez WordPress 6.9 ou supérieur, Yoast SEO 28.7 ou supérieur et l’accès aux fonctions Yoast.", uncertain=write, code="version")
        if response.status >= 400 or len(data) > 262144 or "json" not in response.getheader("Content-Type", "").lower():
            raise CmsError("WordPress n’a pas renvoyé une réponse exploitable.", uncertain=write, code="reponse_illisible")
        return json.loads(data)
    except CmsError:
        raise
    except Exception:
        raise CmsError("La réponse de WordPress n’a pas pu être confirmée. Vérifiez la connexion avant de poursuivre.", uncertain=write, code="injoignable") from None
    finally:
        connection.close()


def values(page):
    result = {}
    for field in FIELDS:
        if field not in page:
            raise CmsError("La réponse WordPress ne contient pas les métadonnées attendues.")
        value = page.get(field)
        if value is not None and not isinstance(value, str):
            raise CmsError("Les métadonnées renvoyées par WordPress ne sont pas compatibles.")
        result[field] = value
    return result


class WordPressClient:
    def __init__(self, credentials):
        self.credentials = credentials

    def request(self, method, path, payload=None):
        c = self.credentials
        return exchange(c["site_url"], c["username"], c["password"], method, path, payload)

    def check(self):
        user = self.request("GET", "wp/v2/users/me?context=edit")
        if not isinstance(user, dict) or not user.get("id"):
            raise CmsError("Le compte WordPress n’a pas pu être identifié.", code="compte_inconnu")
        capabilities = user.get("capabilities") or {}
        if not isinstance(capabilities, dict) or not capabilities.get("wpseo_edit_advanced_metadata"):
            raise CmsError("Ce compte WordPress ne peut pas modifier les métadonnées Yoast. Utilisez un compte disposant des droits adaptés.", code="droits")
        for name in (READ, WRITE):
            ability = self.request("GET", "wp-abilities/v1/abilities/" + name)
            schema = ability.get("input_schema") if isinstance(ability, dict) else None
            properties = schema.get("properties") if isinstance(schema, dict) else None
            properties = properties if isinstance(properties, dict) else {}
            if not isinstance(ability, dict) or ability.get("name") != name or (name == WRITE and not FIELDS <= properties.keys()):
                raise CmsError("La version de Yoast ne propose pas encore la modification des titres et descriptions. Mettez Yoast à jour.", code="yoast_ancien")
        return str(user.get("name") or user.get("slug") or "Compte WordPress")

    def read(self, url):
        if not self.contains(url):
            raise CmsError("Cette page ne se trouve pas dans le WordPress connecté.")
        result = self.request("GET", "wp-abilities/v1/abilities/" + READ + "/run?" + urlencode({"input[permalink]": url}))
        if not isinstance(result, list) or len(result) != 1:
            raise CmsError("WordPress ne trouve pas une page unique modifiable pour cette adresse.")
        return self.validate_page(result[0], url)

    def contains(self, url):
        try:
            p, base = urlsplit(url), urlsplit(self.credentials["site_url"])
            # WordPress core can live in /wordpress while public permalinks live at /.
            # Credentials always go to the configured API base; the permalink is only input.
            return (p.scheme == "https" and p.hostname == base.hostname and p.port in {None, 443}
                    and not p.username and not p.password)
        except (ValueError, TypeError):
            return False

    def validate_page(self, page, url):
        if (not isinstance(page, dict) or type(page.get("post_id")) is not int or page["post_id"] <= 0
                or page.get("post_status") != "publish" or page.get("post_type") not in {"post", "page"}
                or not same_page(page.get("permalink"), url)):
            raise CmsError("La réponse ne correspond pas à la page ou à l’article publié attendu.")
        values(page)
        return page

    def update(self, page, changes):
        if not changes or not changes.keys() <= FIELDS:
            raise CmsError("Seuls le titre SEO et la méta-description sont pris en charge.")
        result = self.request("POST", "wp-abilities/v1/abilities/" + WRITE + "/run", {
            "input": {"post_id": page["post_id"], "permalink": page["permalink"], **changes}})
        try:
            self.validate_page(result, page["permalink"])
            if result["post_id"] != page["post_id"] or any(result.get(k) != v for k, v in changes.items()):
                raise CmsError("Les valeurs publiées diffèrent de l’aperçu validé.")
        except CmsError as error:
            raise CmsError(str(error), uncertain=True) from None
        return result


def identifiants(form, project):
    """Le formulaire de connexion -> identifiants valides (adresse sur le domaine du projet)."""
    username, password = str(form.get("username") or ""), str(form.get("password") or "")
    if (not username.strip() or len(username) > 200 or len(password) > 200 or ":" in username
            or any(c in username + password for c in "\r\n\0") or not password.strip()):
        raise CmsError("Renseignez votre identifiant WordPress et un mot de passe d’application valide.", code="saisie")
    return {"site_url": site_url(str(form.get("site_url") or ""), project.base_url),
            "username": username.strip(), "password": password.strip()}


def recharger(value, project):
    value["site_url"] = site_url(value["site_url"], project.base_url)
    if not value.get("username") or not value.get("password"):
        raise ValueError()
    return value


CONNECTEUR = Connecteur(
    nom="wordpress", libelle="WordPress", libelle_complet="WordPress + Yoast SEO",
    provider="wordpress_yoast", client=lambda c: WordPressClient(c), identifiants=identifiants,
    recharger=recharger,
    plateforme_ok=lambda site: site["platform"] == "wordpress" and site["seo_plugin"] in {"", "yoast"},
    message_plateforme="Choisissez WordPress avec Yoast pour utiliser cette connexion.",
    values=values, raisons=REASONS, champs=("site_url", "username", "password"))
