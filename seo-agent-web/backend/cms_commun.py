"""Ce que partagent les connecteurs CMS : l'erreur, l'empreinte d'une connexion, la description.

Le pilote WordPress (Codex, 17-19/10/2026) etait ecrit pour WordPress seul. Demande du
proprietaire, 02/11/2026 : le meme parcours pour Shopify, PrestaShop, Wix, Webflow et Ghost —
proposition, apercu valide, ecriture relue, controle par une nouvelle analyse. Une seule mise en
oeuvre de ce parcours (`cms_routes`) ; chaque CMS n'apporte que ce qui lui est propre : comment on
s'y connecte, comment on lit et ecrit le titre SEO et la meta-description d'une page.

CONTRAT D'UN CLIENT (`Connecteur.client(identifiants)`) :
  check()            -> le nom du compte connecte, ou CmsError(code=...)
  contains(url)      -> l'adresse est-elle servie par ce site ?
  read(url)          -> la page : {post_id, permalink, post_title, seo_title, meta_description,
                        seo_title_rendered?, meta_description_rendered?} ; CmsError sinon
  update(page, changes) -> la page relue apres ecriture ; CmsError(uncertain=True) si le resultat
                        ne peut pas etre confirme
`post_id` est l'identifiant de la page DANS LE CMS (le nom vient de WordPress ; il est garde pour
ne pas migrer les operations deja journalisees).
"""
from __future__ import annotations

import hashlib
import hmac
import http.client
import ipaddress
import json
import socket
import ssl
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urlsplit

FIELDS = {"seo_title", "meta_description"}


class CmsError(Exception):
    def __init__(self, message, *, uncertain=False, code=""):
        super().__init__(message)
        self.uncertain = uncertain
        # A stable reason for the operations journal; the message stays the customer's.
        self.code = code


def fingerprint(credentials, key):
    # Keyed: a stored fingerprint must not let anyone test a guessed password offline.
    return "hmac:" + hmac.new(key, json.dumps(credentials, sort_keys=True).encode(), hashlib.sha256).hexdigest()


def same_connection(credentials, stored, key):
    stored = str(stored or "")
    if stored.startswith("hmac:"):
        return hmac.compare_digest(stored, fingerprint(credentials, key))
    # Changes recorded before the keyed fingerprint can still be restored.
    return hmac.compare_digest(stored, hashlib.sha256(json.dumps(credentials, sort_keys=True).encode()).hexdigest())


def same_page(left, right):
    def key(value):
        p = urlsplit(value)
        return p.scheme, (p.hostname or "").lower(), p.port or 443, p.path or "/", p.query
    try:
        return key(left) == key(right)
    except (ValueError, TypeError):
        return False


def adresse_publique(host, cms):
    """L'adresse IP publique ou l'on enverra les identifiants ; `cms` nomme le CMS dans l'erreur."""
    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        addresses = sorted({entry[4][0] for entry in infos})
        if not addresses or any(not ipaddress.ip_address(ip).is_global or ipaddress.ip_address(ip).is_multicast for ip in addresses):
            raise ValueError()
        return addresses[0]
    except (OSError, ValueError):
        raise CmsError("L’adresse %s doit être accessible sur Internet et résoudre vers une adresse publique." % cms,
                       code="adresse_privee") from None


class HTTPSEpingle(http.client.HTTPSConnection):
    """HTTPS vers l'adresse IP deja validee : un DNS qui change entre-temps n'y change rien."""

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


def values_of(page, cms):
    """Les deux champs d'une page, tels que le CMS les a rendus. `cms` nomme le CMS dans l'erreur."""
    result = {}
    for name in FIELDS:
        if name not in page:
            raise CmsError("La réponse %s ne contient pas les métadonnées attendues." % cms)
        value = page.get(name)
        if value is not None and not isinstance(value, str):
            raise CmsError("Les métadonnées renvoyées par %s ne sont pas compatibles." % cms)
        result[name] = value
    return result


@dataclass(frozen=True)
class Connecteur:
    """Ce qu'un CMS apporte au parcours commun."""
    nom: str                      # dans les adresses : /projects/<slug>/cms/<nom>/...
    libelle: str                  # dans les phrases : « Connectez WordPress… »
    libelle_complet: str          # sous le titre d'un ecran : « WordPress + Yoast SEO »
    provider: str                 # journal de la connexion
    client: Callable[[dict], Any]
    # Le formulaire de connexion -> identifiants valides, ou CmsError(code="saisie" | ...).
    identifiants: Callable[[dict, Any], dict]
    # Identifiants relus en base -> identifiants revalides (le domaine du projet a pu changer).
    recharger: Callable[[dict, Any], dict]
    # La plateforme declaree du projet permet-elle ce connecteur ?
    plateforme_ok: Callable[[dict], bool]
    message_plateforme: str
    values: Callable[[dict], dict]
    raisons: dict = field(default_factory=dict)
    # Les champs du formulaire de connexion, dans l'ordre (gabarit cms_connexion_<nom>.html).
    champs: tuple = ()
    # Un CMS qui garde l'écriture EN ATTENTE jusqu'à une publication du client (Webflow) : ce
    # qu'il faut faire avant le contrôle. Vide : l'écriture est en ligne aussitôt.
    publication: str = ""
