"""Yoast's official WordPress Abilities API, using HTTPS pinned to validated public IPs."""
from __future__ import annotations

import base64
import hashlib
import http.client
import ipaddress
import json
import socket
import ssl
from urllib.parse import unquote, urlencode, urlsplit, urlunsplit

READ = "yoast-seo/get-post-seo-data"
WRITE = "yoast-seo/update-post-seo-data"
FIELDS = {"seo_title", "meta_description"}


class CmsError(Exception):
    def __init__(self, message, *, uncertain=False):
        super().__init__(message)
        self.uncertain = uncertain


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
        raise CmsError("Utilisez l’adresse HTTPS de WordPress, sur le même domaine que ce projet.") from None


def same_page(left, right):
    def key(value):
        p = urlsplit(value)
        return p.scheme, (p.hostname or "").lower(), p.port or 443, p.path or "/", p.query
    try:
        return key(left) == key(right)
    except (ValueError, TypeError):
        return False


def public_address(host):
    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        addresses = sorted({entry[4][0] for entry in infos})
        if not addresses or any(not ipaddress.ip_address(ip).is_global or ipaddress.ip_address(ip).is_multicast for ip in addresses):
            raise ValueError()
        return addresses[0]
    except (OSError, ValueError):
        raise CmsError("L’adresse WordPress doit être accessible sur Internet et résoudre vers une adresse publique.") from None


class _PinnedHTTPS(http.client.HTTPSConnection):
    def __init__(self, host, address):
        super().__init__(host, timeout=12, context=ssl.create_default_context())
        self.address = address

    def connect(self):
        self.sock = socket.create_connection((self.address, 443), self.timeout)
        try:
            self.sock = self._context.wrap_socket(self.sock, server_hostname=self.host)
        except BaseException:
            self.sock.close()
            raise


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
            raise CmsError("WordPress redirige cette adresse. Utilisez son adresse HTTPS finale.", uncertain=write)
        if response.status in {401, 403}:
            raise CmsError("WordPress refuse l’accès. Vérifiez le mot de passe d’application et les droits de modification Yoast.", uncertain=write)
        if response.status == 404:
            raise CmsError("Cette fonction n’est pas disponible. Vérifiez WordPress 6.9 ou supérieur, Yoast SEO 28.7 ou supérieur et l’accès aux fonctions Yoast.", uncertain=write)
        if response.status >= 400 or len(data) > 262144 or "json" not in response.getheader("Content-Type", "").lower():
            raise CmsError("WordPress n’a pas renvoyé une réponse exploitable.", uncertain=write)
        return json.loads(data)
    except CmsError:
        raise
    except Exception:
        raise CmsError("La réponse de WordPress n’a pas pu être confirmée. Vérifiez la connexion avant de poursuivre.", uncertain=write) from None
    finally:
        connection.close()


def fingerprint(credentials):
    return hashlib.sha256(json.dumps(credentials, sort_keys=True).encode()).hexdigest()


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
            raise CmsError("Le compte WordPress n’a pas pu être identifié.")
        capabilities = user.get("capabilities") or {}
        if not isinstance(capabilities, dict) or not capabilities.get("wpseo_edit_advanced_metadata"):
            raise CmsError("Ce compte WordPress ne peut pas modifier les métadonnées Yoast. Utilisez un compte disposant des droits adaptés.")
        for name in (READ, WRITE):
            ability = self.request("GET", "wp-abilities/v1/abilities/" + name)
            schema = ability.get("input_schema") if isinstance(ability, dict) else None
            properties = schema.get("properties") if isinstance(schema, dict) else None
            properties = properties if isinstance(properties, dict) else {}
            if not isinstance(ability, dict) or ability.get("name") != name or (name == WRITE and not FIELDS <= properties.keys()):
                raise CmsError("La version de Yoast ne propose pas encore la modification des titres et descriptions. Mettez Yoast à jour.")
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
            return (p.scheme == "https" and p.hostname == base.hostname and p.port in {None, 443}
                    and not p.username and not p.password
                    and (p.path == base.path or p.path.startswith(base.path.rstrip("/") + "/")))
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
