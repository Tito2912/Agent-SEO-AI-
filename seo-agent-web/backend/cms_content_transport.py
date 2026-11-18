"""Bounded, pinned HTTP for editorial CMS adapters; no client secrets in errors.

`check()` is read-only and must prove the remote site's association with the
project before this instance permits writes. It does not prove publication
permissions: the CMS still decides those at each explicitly approved operation.
"""
from __future__ import annotations

import base64
import http.client
import json
import re
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import parse_qsl, unquote, urlsplit

from .cms_commun import CmsError, HTTPSEpingle, adresse_publique
from .ghost_cms import jeton as ghost_token

MAX_RESPONSE = 4 * 1024 * 1024
MAX_REQUEST = 2 * 1024 * 1024
SHOPIFY_PATH = "/admin/api/2026-10/graphql.json"
PLATFORMS = {"wordpress", "ghost", "shopify", "webflow", "wix", "prestashop"}
_PinnedHTTPS = HTTPSEpingle
_TOKEN = re.compile(r"[A-Za-z_][A-Za-z_0-9]*|\.\.\.|[^\s,]")
_ID = r"[A-Za-z0-9_-]+"


@dataclass(frozen=True)
class TransportResponse:
    """Structural match for cms_content_adapters.base.TransportResponse."""

    status: int
    headers: Mapping[str, str]
    body: bytes
    write: bool = False

    def json(self):
        try:
            return json.loads(self.body.decode("utf-8"))
        except (ValueError, UnicodeError):
            raise _fail("reponse_illisible", uncertain=self.write) from None

    def text(self):
        try:
            return self.body.decode("utf-8")
        except UnicodeError:
            raise _fail("reponse_illisible", uncertain=self.write) from None


def _fail(code, *, uncertain=False):
    messages = {
        "configuration": "La configuration de publication est incomplète ou invalide.",
        "adresse": "Utilisez une adresse HTTPS finale autorisée pour ce projet.",
        "chemin": "Cette opération sort du périmètre éditorial autorisé.",
        "entetes": "Les en-têtes de cette opération ne sont pas autorisés.",
        "non_verifie": "Vérifiez la connexion et son association au projet avant toute écriture.",
        "autre_site": "La connexion ne désigne pas le site de ce projet.",
        "acces_refuse": "Le CMS refuse la connexion ou les droits demandés.",
        "introuvable": "Le CMS ne trouve pas cette ressource.",
        "limite": "Le CMS limite les requêtes. L’opération n’a pas été réessayée.",
        "refus": "Le CMS a refusé cette opération.",
        "redirection": "Le CMS redirige cette adresse. Aucune redirection n’a été suivie.",
        "reponse_illisible": "La réponse du CMS ne permet pas de confirmer l’opération.",
        "injoignable": "La réponse du CMS n’a pas pu être confirmée. Vérifiez son état avant de réessayer.",
    }
    return CmsError(messages[code], uncertain=uncertain, code=code)


def _url(value):
    if not isinstance(value, str) or len(value) > 2048:
        raise _fail("adresse")
    try:
        p = urlsplit(value.strip())
        if (p.scheme != "https" or not p.hostname or p.username or p.password
                or p.port not in {None, 443} or p.query or p.fragment):
            raise ValueError()
        host = p.hostname.encode("idna").decode("ascii").lower()
        _safe_path(p.path or "/")
        return host, p.path.rstrip("/")
    except (ValueError, UnicodeError):
        raise _fail("adresse") from None


def _safe_path(path):
    if (not isinstance(path, str) or len(path) > 8192 or not path.startswith("/")
            or path.startswith("//") or "#" in path or "://" in path):
        raise _fail("chemin")
    decoded = path
    for _ in range(4):
        if any(ord(c) < 32 or ord(c) == 127 for c in decoded) or "\\" in decoded:
            raise _fail("chemin")
        before, decoded = decoded, unquote(decoded)
        if before == decoded:
            break
    if ("%" in decoded or decoded.startswith("//") or "://" in decoded
            or any(s in {".", ".."} for s in urlsplit(decoded).path.split("/"))):
        raise _fail("chemin")


def _secret(credentials, name):
    value = credentials.get(name)
    if (not isinstance(value, str) or not value.strip() or len(value) > 16000
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise _fail("configuration")
    return value.strip()


def _graphql_kind(payload):
    """Allow one operation and only editorial root fields, including aliased ones."""
    if not isinstance(payload, dict) or not isinstance(payload.get("query"), str):
        raise _fail("chemin")
    query = payload["query"]
    if len(query) > 30000:
        raise _fail("chemin")
    # Strings/comments cannot introduce operations. Block strings are not used by
    # these adapters: reject instead of maintaining a second GraphQL parser.
    if '"""' in query or "..." in query or "@" in query:
        raise _fail("chemin")
    query = re.sub(r'"(?:\\.|[^"\\])*"', '""', query)
    query = re.sub(r"#[^\n]*", "", query)
    tokens = _TOKEN.findall(query)
    if not tokens:
        raise _fail("chemin")
    kind = tokens[0] if tokens[0] in {"query", "mutation"} else "query" if tokens[0] == "{" else ""
    if not kind:
        raise _fail("chemin")
    parentheses = depth = 0
    roots, closed, started = [], False, False
    for i, lexeme in enumerate(tokens):
        if closed:
            raise _fail("chemin")
        if lexeme == "(":
            parentheses += 1
        elif lexeme == ")":
            parentheses -= 1
        elif lexeme == "{" and not parentheses:
            depth += 1
            started = True
        elif lexeme == "}" and not parentheses:
            depth -= 1
            closed = started and depth == 0
        elif depth == 1 and parentheses == 0 and re.fullmatch(r"[A-Za-z_]\w*", lexeme):
            if i + 1 < len(tokens) and tokens[i + 1] == ":":
                continue  # alias: next name is the actual field
            roots.append(lexeme)
        if parentheses < 0 or depth < 0:
            raise _fail("chemin")
    allowed = {"articleCreate", "articleUpdate"} if kind == "mutation" else {"shop", "article", "blog", "blogs"}
    if not closed or parentheses or not roots or not set(roots) <= allowed:
        raise _fail("chemin")
    return kind


class ContentTransport:
    def __init__(self, platform, credentials, *, project_url):
        if platform not in PLATFORMS or not isinstance(credentials, dict):
            raise _fail("configuration")
        self.platform = platform
        self._credentials = dict(credentials)
        self.project_host, self.project_prefix = _url(project_url)
        self.project_url = "https://" + self.project_host + self.project_prefix
        self._verified = False
        self._destinations = {}
        self.prefix = ""
        if platform in {"wordpress", "ghost", "prestashop"}:
            self.host, self.prefix = _url(_secret(credentials, "base_url"))
            if self.host != self.project_host and not (platform == "ghost" and self.host.endswith(".ghost.io")):
                raise _fail("autre_site")
            if platform == "wordpress":
                self._credentials["username"] = _secret(credentials, "username")
                self._credentials["password"] = _secret(credentials, "password")
            else:
                key = _secret(credentials, "key")
                self._credentials["key"] = key
                if platform == "ghost" and not re.fullmatch(r"[0-9a-f]{24}:[0-9a-f]{64}", key):
                    raise _fail("configuration")
        elif platform == "shopify":
            shop = _secret(credentials, "shop").lower()
            if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.myshopify\.com", shop):
                raise _fail("adresse")
            self.host = shop
            self._credentials["token"] = _secret(credentials, "token")
        else:
            self.host = "api.webflow.com" if platform == "webflow" else "www.wixapis.com"
            self._credentials["token"] = _secret(credentials, "token")
            if not re.fullmatch(_ID, _secret(credentials, "site_id")):
                raise _fail("configuration")
            self._credentials["site_id"] = _secret(credentials, "site_id")

    def _belongs(self, value):
        try:
            host, prefix = _url(value)
            return host == self.project_host and (not self.project_prefix or prefix == self.project_prefix)
        except CmsError:
            return False

    def _route(self, method, path, payload):
        _safe_path(path)
        parsed = urlsplit(path)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        if any(key.lower() in {"_method", "rest_route", "access_token", "api_key", "ws_key"} for key, _ in query):
            raise _fail("chemin")
        name, p = self.platform, parsed.path.rstrip("/")
        if method not in {"GET", "POST", "PUT", "PATCH"}:
            raise _fail("chemin")
        write = method != "GET"
        if name == "wordpress":
            allowed = bool(re.fullmatch(r"/wp-json/wp/v2/(posts|media)(/\d+)?", p))
            allowed |= method == "GET" and p == "/wp-json/wp/v2/users/me"
            allowed &= all(k in {"context", "status", "slug", "search", "per_page", "page", "include", "_fields", "order", "orderby"} for k, _ in query)
        elif name == "ghost":
            allowed = bool(re.fullmatch(r"/ghost/api/admin/(posts(/" + _ID + r")?|site)", p))
            allowed &= p != "/ghost/api/admin/site" or method == "GET"
            allowed |= method == "POST" and p == "/ghost/api/admin/images/upload"
            allowed &= all(k in {"source", "limit", "fields", "filter", "formats", "status", "page", "include"} for k, _ in query)
        elif name == "shopify":
            allowed = method == "POST" and p == SHOPIFY_PATH and not query
            if allowed:
                write = _graphql_kind(payload) == "mutation"
        elif name == "webflow":
            allowed = bool(re.fullmatch(r"/v2/collections/" + _ID + r"(/items(/(" + _ID + r"|insert|publish))?)?", p))
            allowed |= method == "GET" and bool(re.fullmatch(r"/v2/sites/" + re.escape(self._credentials["site_id"]) + r"(/collections)?", p))
            allowed &= all(k in {"limit", "offset", "cmsLocaleId", "cmsLocaleIds"} for k, _ in query)
            collection = re.match(r"/v2/collections/(" + _ID + r")", p)
            if collection and collection.group(1) not in self._destinations:
                raise _fail("autre_site")
            # Editing collection definitions is outside this content-only client.
            if collection and p == collection.group(0) and method != "GET":
                raise _fail("chemin")
        elif name == "wix":
            allowed = bool(re.fullmatch(r"/blog/v3/(draft-posts(/" + _ID + r")?(/publish)?|posts(/" + _ID + r")?)", p))
            allowed &= not p.startswith("/blog/v3/posts") or method == "GET"
            allowed |= method == "GET" and p == "/promote/seo/v1/item-seo-tags/STATIC_PAGE"
            allowed &= all(k in {"paging.limit", "paging.cursor", "fieldsets"} for k, _ in query)
        else:
            allowed = bool(re.fullmatch(r"/api/content_management_system(/\d+)?", p))
            allowed &= all(k in {"schema", "display", "limit", "sort", "language", "id_shop", "output_format"}
                           or re.fullmatch(r"filter\[[A-Za-z_]+\]", k) for k, _ in query)
        if not allowed:
            raise _fail("chemin")
        if write and not self._verified:
            raise _fail("non_verifie")
        return write

    def _headers(self, custom, content_type):
        result = {"Accept": "application/xml" if self.platform == "prestashop" else "application/json",
                  "Content-Type": content_type, "User-Agent": "Noyaru-Editorial/1.0"}
        if custom is not None and not isinstance(custom, Mapping):
            raise _fail("entetes")
        for name, value in (custom or {}).items():
            if (not isinstance(name, str) or name.lower() not in {"accept", "content-type", "accept-version", "if-match"}
                    or not isinstance(value, str) or len(value) > 256
                    or any(ord(c) < 32 or ord(c) > 126 for c in value)):
                raise _fail("entetes")
            result[{"accept": "Accept", "content-type": "Content-Type", "accept-version": "Accept-Version", "if-match": "If-Match"}[name.lower()]] = value
        c, name = self._credentials, self.platform
        if name in {"wordpress", "prestashop"}:
            raw = f"{c['username']}:{c['password']}" if name == "wordpress" else c["key"] + ":"
            result["Authorization"] = "Basic " + base64.b64encode(raw.encode("utf-8")).decode("ascii")
        elif name == "ghost":
            result["Authorization"] = "Ghost " + ghost_token(c["key"])
            result.setdefault("Accept-Version", "v5.0")
        elif name == "shopify":
            result["X-Shopify-Access-Token"] = c["token"]
        else:
            result["Authorization"] = ("Bearer " if name == "webflow" else "") + c["token"]
            if name == "wix":
                result["wix-site-id"] = c["site_id"]
        return result

    def __call__(self, method, path, payload=None, headers=None):
        if not isinstance(method, str):
            raise _fail("chemin")
        method = method.upper()
        write = self._route(method, path, payload)
        if isinstance(payload, dict) and {"_method", "rest_route"}.intersection(payload):
            raise _fail("chemin")
        try:
            if self.platform == "prestashop" and isinstance(payload, (str, bytes)):
                body = payload.encode("utf-8") if isinstance(payload, str) else payload
                content_type = "application/xml; charset=utf-8"
            else:
                body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8") if payload is not None else None
                content_type = "application/json; charset=utf-8"
            if body is not None and len(body) > MAX_REQUEST:
                raise ValueError()
        except (ValueError, TypeError, UnicodeError):
            raise _fail("configuration") from None
        request_headers = self._headers(headers, content_type)
        return self._exchange(method, path, body, request_headers, write=write)

    def _exchange(self, method, path, body, request_headers, *, write):
        """The same pinned exchange for bounded JSON/XML and validated image bodies."""
        address = adresse_publique(self.host, self.platform)
        connection = _PinnedHTTPS(self.host, address)
        try:
            connection.request(method, self.prefix + path, body=body, headers=request_headers)
            response = connection.getresponse()
            status = response.status
            raw = response.read(MAX_RESPONSE + 1)
            if 300 <= status < 400:
                raise _fail("redirection", uncertain=write)
            if status in {401, 403}:
                raise _fail("acces_refuse")
            if status == 404:
                raise _fail("introuvable")
            if status == 429:
                raise _fail("limite")
            if status in {400, 409, 413, 415, 422}:
                raise _fail("refus")
            content = response.getheader("Content-Type", "").lower()
            expected = "xml" if self.platform == "prestashop" else "json"
            if status >= 400 or len(raw) > MAX_RESPONSE or (raw and expected not in content):
                raise _fail("reponse_illisible", uncertain=write)
            # Expose only response metadata used for optimistic revisions. Cookies,
            # vendor diagnostics and echoed authorization never reach the journal.
            safe_headers = {"content-type": content}
            for field in ("ETag", "Last-Modified"):
                value = response.getheader(field)
                if value and len(value) <= 256 and not any(ord(c) < 32 for c in value):
                    safe_headers[field.lower()] = value
            return TransportResponse(status, safe_headers, raw, write=write)
        except CmsError:
            raise
        except (OSError, http.client.HTTPException, ValueError, UnicodeError):
            raise _fail("injoignable", uncertain=write) from None
        finally:
            connection.close()

    @property
    def supports_image_upload(self):
        return self.platform in {"wordpress", "ghost"}

    def upload_image(self, *, data: bytes, filename: str, content_type: str):
        """Transfer one validated image after the caller's journaled consent.

        This method does not publish an article, retry a transfer, delete an asset,
        or assert that the public image has been fetched successfully.
        """
        from .cms_content_media import ghost_multipart, public_image_url, validate_image

        if not self.supports_image_upload:
            raise CmsError("Le transfert d’images n’est pas disponible pour cette plateforme.", code="unsupported")
        if not self._verified:
            raise _fail("non_verifie")
        image = validate_image(data=data, filename=filename, content_type=content_type)
        if self.platform == "wordpress":
            path = "/wp-json/wp/v2/media"
            body = image.data
            headers = self._headers(None, image.content_type)
            headers["Content-Disposition"] = 'attachment; filename="' + image.filename + '"'
        else:
            path = "/ghost/api/admin/images/upload/"
            body, mime = ghost_multipart(image)
            headers = self._headers(None, mime)
        self._route("POST", path, None)
        response = self._exchange("POST", path, body, headers, write=True)
        try:
            result = response.json()
            if response.status not in {200, 201} or not isinstance(result, dict):
                raise ValueError()
            if self.platform == "wordpress":
                remote_id = result.get("id")
                if not isinstance(remote_id, int) or isinstance(remote_id, bool) or remote_id <= 0:
                    raise ValueError()
                url = public_image_url(result.get("source_url"), project_url=self.project_url,
                                       resolve_public=adresse_publique)
                remote_id = str(remote_id)
            else:
                images = result.get("images")
                if not isinstance(images, list) or len(images) != 1 or not isinstance(images[0], dict):
                    raise ValueError()
                url = public_image_url(images[0].get("url"), project_url=self.project_url,
                                       resolve_public=adresse_publique, relative=True)
                # Ghost's image resource has no numeric ID: its URL identifies it.
                remote_id = url
            return {"id": remote_id, "url": url}
        except CmsError:
            raise
        except (ValueError, UnicodeError, TypeError):
            raise _fail("reponse_illisible", uncertain=True) from None

    def _json(self, method, path, payload=None):
        response = self(method, path, payload)
        try:
            data = response.json()
            if not isinstance(data, dict):
                raise ValueError()
            return data
        except (ValueError, TypeError, UnicodeError):
            raise _fail("reponse_illisible") from None

    def check(self):
        """Read-only site association; this instance never accepts a stored verified flag."""
        self._verified = False
        self._destinations = {}
        label, destinations = self.platform, []
        try:
            if self.platform == "wordpress":
                # context=edit requires authentication, unlike the public posts list.
                self("GET", "/wp-json/wp/v2/posts?context=edit&per_page=1")
                associated = self.host == self.project_host
                destinations = [{"id": "posts", "title": "Articles", "kind": "blog"}]
            elif self.platform == "prestashop":
                self("GET", "/api/content_management_system?display=[id]&limit=1")
                associated = self.host == self.project_host
                destinations = [{"id": "cms-pages", "title": "Pages de contenu", "kind": "page"}]
            elif self.platform == "ghost":
                site = self._json("GET", "/ghost/api/admin/site/").get("site") or {}
                associated = isinstance(site, dict) and self._belongs(site.get("url"))
                self("GET", "/ghost/api/admin/posts/?limit=1&fields=id")
                label = site.get("title") or label
                destinations = [{"id": "posts", "title": "Articles", "kind": "blog"}]
            elif self.platform == "shopify":
                data = self._json("POST", SHOPIFY_PATH, {"query": "query NoyaruSite { shop { name primaryDomain { url } } }"})
                shop = (data.get("data") or {}).get("shop") or {}
                associated = not data.get("errors") and self._belongs((shop.get("primaryDomain") or {}).get("url"))
                label = shop.get("name") or label
                if associated:
                    blogs = self._json("POST", SHOPIFY_PATH, {"query": "query NoyaruBlogs { blogs(first:100) { nodes { id title } pageInfo { hasNextPage } } }"})
                    if blogs.get("errors"):
                        raise _fail("acces_refuse")
                    listed = (blogs.get("data") or {}).get("blogs") or {}
                    nodes = listed.get("nodes")
                    if not isinstance(nodes, list) or (listed.get("pageInfo") or {}).get("hasNextPage"):
                        # No silent truncation; a larger catalog needs a paged picker.
                        raise _fail("reponse_illisible")
                    destinations = [{"id": row["id"], "title": str(row.get("title") or "Blog")[:255], "kind": "blog"}
                                    for row in nodes if isinstance(row, dict) and re.fullmatch(r"gid://shopify/Blog/\d+", str(row.get("id") or ""))]
            elif self.platform == "webflow":
                ident = self._credentials["site_id"]
                site = self._json("GET", "/v2/sites/" + ident)
                domains = ["https://" + str(site.get("shortName") or "") + ".webflow.io"]
                domains += [str(d.get("url") or "") for d in site.get("customDomains") or [] if isinstance(d, dict)]
                associated = site.get("id") == ident and any(self._belongs(d if "://" in d else "https://" + d) for d in domains)
                label = site.get("displayName") or label
                if associated:
                    listed = self._json("GET", "/v2/sites/" + ident + "/collections")
                    if not isinstance(listed.get("collections"), list) or len(listed["collections"]) > 100:
                        raise _fail("reponse_illisible")
                    destinations = [{"id": row["id"], "title": str(row.get("displayName") or "Collection")[:255],
                                     "kind": "collection", "fields": [], "schema_loaded": False}
                                    for row in listed["collections"] if isinstance(row, dict) and re.fullmatch(_ID, str(row.get("id") or ""))]
            else:
                # Default (inherited) homepage canonical associates the site ID with
                # a published domain. A hand-edited canonical is not ownership proof.
                data = self._json("GET", "/promote/seo/v1/item-seo-tags/STATIC_PAGE?paging.limit=100")
                associated = False
                for item in data.get("itemSeoTags") or []:
                    for entry in item.get("resolvedTags") or []:
                        tag = entry.get("inheritedTag") if entry.get("source") == "TAG_SOURCE_ITEM" else entry.get("tag")
                        props = (tag or {}).get("props") or {}
                        if (isinstance(tag, dict) and tag.get("type") == "link" and props.get("rel") == "canonical"
                                and self._belongs(props.get("href"))):
                            associated = True
                destinations = [{"id": "blog", "title": "Blog", "kind": "blog"}]
            if not associated:
                raise _fail("autre_site")
            self._verified = True
            self._destinations = {d["id"]: d for d in destinations}
            return {"verified": True, "label": str(label)[:255], "reason": "", "destinations": destinations}
        except CmsError as error:
            return {"verified": False, "label": self.platform, "reason": error.code or "reponse_illisible", "destinations": []}
        except (ValueError, TypeError, AttributeError, KeyError):
            return {"verified": False, "label": self.platform, "reason": "reponse_illisible", "destinations": []}

    def describe_destination(self, destination_id):
        """Read one chosen collection schema; never infer an editorial mapping."""
        if not self._verified:
            raise _fail("non_verifie")
        if destination_id not in self._destinations:
            raise _fail("autre_site")
        result = dict(self._destinations[destination_id])
        if self.platform != "webflow":
            return result
        schema = self._json("GET", "/v2/collections/" + destination_id)
        if schema.get("id") != destination_id or not isinstance(schema.get("fields"), list):
            raise _fail("reponse_illisible")
        result["fields"] = [{"slug": field["slug"], "title": str(field.get("displayName") or field["slug"])[:255],
                             "type": str(field.get("type") or ""), "required": bool(field.get("isRequired")),
                             "editable": field.get("isEditable") is not False}
                            for field in schema["fields"] if isinstance(field, dict) and re.fullmatch(_ID, str(field.get("slug") or ""))]
        result["schema_loaded"] = True
        return result
