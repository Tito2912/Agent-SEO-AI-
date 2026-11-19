"""Editorial transport never sends secrets outside its pinned, verified CMS."""
from __future__ import annotations

import json

import pytest

from backend import cms_content_transport as transport, cms_commun
from backend.cms_commun import CmsError

PROJECT = "https://atelier.example/"
SITE = "a" * 24
KEY = "a" * 24 + ":" + "b" * 64


class Response:
    def __init__(self, body=None, *, status=200, content_type="application/json", raw=None):
        self.status = status
        self.body = raw if raw is not None else json.dumps(body if body is not None else {}).encode("utf-8")
        self.content_type = content_type

    def read(self, size):
        return self.body[:size]

    def getheader(self, name, default=None):
        return {"Content-Type": self.content_type, "ETag": '"revision-1"',
                "Set-Cookie": "private-cookie"}.get(name, default)


@pytest.fixture()
def wire(monkeypatch):
    data = {"responses": [], "calls": [], "closed": 0, "dns": []}

    def address(host, platform):
        data["dns"].append((host, platform))
        return "93.184.216.34"

    class Connection:
        def __init__(self, host, ip):
            assert ip == "93.184.216.34"
            self.host = host

        def request(self, method, path, body=None, headers=None):
            data["calls"].append({"host": self.host, "method": method, "path": path,
                                  "body": body, "headers": headers})

        def getresponse(self):
            response = data["responses"].pop(0)
            if isinstance(response, Exception):
                raise response
            return response

        def close(self):
            data["closed"] += 1

    monkeypatch.setattr(transport, "adresse_publique", address)
    monkeypatch.setattr(transport, "_PinnedHTTPS", Connection)
    return data


def wp():
    return transport.ContentTransport("wordpress", {"base_url": PROJECT, "username": "editor", "password": "fictitious-password"}, project_url=PROJECT)


def verify_wp(client, wire):
    wire["responses"].append(Response([]))
    assert client.check()["verified"]


def test_wordpress_check_is_read_only_and_secrets_are_headers_only(wire):
    client = wp()
    verify_wp(client, wire)
    wire["responses"].append(Response({"id": 42}))
    result = client("POST", "/wp-json/wp/v2/posts", {"title": "Une page", "status": "draft"})
    assert result.json() == {"id": 42}
    assert wire["calls"][0]["method"] == "GET"
    assert "context=edit" in wire["calls"][0]["path"]
    assert wire["calls"][1]["host"] == "atelier.example"
    assert wire["calls"][1]["headers"]["Authorization"].startswith("Basic ")
    assert "fictitious-password" not in wire["calls"][1]["path"]
    assert set(result.headers) == {"content-type", "etag"}
    assert wire["closed"] == 2


@pytest.mark.parametrize("path", [
    "https://attacker.example/wp-json/wp/v2/posts", "//attacker.example/posts", "/wp-json/wp/v2/../users",
    "/wp-json/wp/v2/%2e%2e/posts", "/wp-json/wp/v2/posts?rest_route=/users", "/wp-json/wp/v2/posts?_method=DELETE",
    "/wp-json/wp/v2/posts%0d%0aAuthorization:bad", "/wp-json/wp/v2/posts#fragment", "/wp-json/wp/v2/users/1",
    "/wp-json/wp/v2/posts?ws_key=private", "/wp-json/wp/v2/%25252e%25252e/posts", "/wp-json/wp/v2/posts\\other",
], ids=["absolute", "authority", "traversal", "encoded", "route-override", "method-override", "control", "fragment", "users", "key-query", "nested-encoding", "backslash"])
def test_path_escape_is_refused_before_dns_or_network(wire, path):
    with pytest.raises(CmsError) as caught:
        wp()("GET", path)
    assert caught.value.code == "chemin"
    assert not wire["calls"] and not wire["dns"]


@pytest.mark.parametrize("platform,credentials", [
    ("wordpress", {"base_url": "https://other.example", "username": "e", "password": "p"}),
    ("prestashop", {"base_url": "https://other.example", "key": "k"}),
    ("ghost", {"base_url": "https://other.example", "key": KEY}),
    ("shopify", {"shop": "atelier.myshopify.com.attacker.example", "token": "fictitious"}),
    ("wordpress", {"base_url": "https://user:pass@atelier.example", "username": "e", "password": "p"}),
    ("wordpress", {"base_url": "http://atelier.example", "username": "e", "password": "p"}),
], ids=["wp-other", "presta-other", "ghost-other", "shop-suffix", "userinfo", "http"])
def test_credentials_cannot_select_another_receiver(wire, platform, credentials):
    with pytest.raises(CmsError):
        transport.ContentTransport(platform, credentials, project_url=PROJECT)
    assert not wire["calls"]


def test_stored_verified_flag_cannot_enable_writes(wire):
    client = transport.ContentTransport("wordpress", {"base_url": PROJECT, "username": "e", "password": "p", "verified": True}, project_url=PROJECT)
    with pytest.raises(CmsError) as caught:
        client("POST", "/wp-json/wp/v2/posts", {"status": "draft"})
    assert caught.value.code == "non_verifie" and not caught.value.uncertain
    assert not wire["calls"]


def test_real_public_ip_guard_prevents_connection_to_private_resolution(wire, monkeypatch):
    monkeypatch.setattr(transport, "adresse_publique", cms_commun.adresse_publique)
    monkeypatch.setattr(cms_commun.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("127.0.0.1", 443))])
    with pytest.raises(CmsError) as caught:
        wp()("GET", "/wp-json/wp/v2/posts")
    assert caught.value.code == "adresse_privee"
    assert not wire["calls"]


@pytest.mark.parametrize("headers", [{"Authorization": "override"}, {"Host": "attacker.example"},
    {"Accept": "application/json\r\nCookie: bad"}, {"X-HTTP-Method-Override": "DELETE"}],
    ids=["auth", "host", "control", "method"])
def test_caller_cannot_override_auth_or_route_headers(wire, headers):
    with pytest.raises(CmsError) as caught:
        wp()("GET", "/wp-json/wp/v2/posts", headers=headers)
    assert caught.value.code == "entetes"
    assert not wire["calls"]


@pytest.mark.parametrize("failure,code,uncertain", [
    (Response(status=302), "redirection", True), (Response(status=403), "acces_refuse", False),
    (Response(status=429), "limite", False), (Response(status=500), "reponse_illisible", True),
    (Response(status=428), "reponse_illisible", True),
    (Response(raw=b"not-json", content_type="text/html"), "reponse_illisible", True),
    (OSError("fictitious-password"), "injoignable", True),
], ids=["redirect", "permission", "quota", "server", "draft-saved", "html", "timeout"])
def test_writes_never_retry_and_mark_ambiguous_results(wire, failure, code, uncertain):
    client = wp()
    verify_wp(client, wire)
    wire["responses"].append(failure)
    with pytest.raises(CmsError) as caught:
        client("POST", "/wp-json/wp/v2/posts", {"title": "Une page", "status": "draft"})
    assert caught.value.code == code and caught.value.uncertain is uncertain
    assert "fictitious-password" not in str(caught.value)
    assert len(wire["calls"]) == 2 and wire["closed"] == 2


def test_size_limit_stops_a_response_after_one_bounded_read(wire, monkeypatch):
    monkeypatch.setattr(transport, "MAX_RESPONSE", 4)
    wire["responses"].append(Response(raw=b"123456789"))
    with pytest.raises(CmsError) as caught:
        wp()("GET", "/wp-json/wp/v2/posts")
    assert caught.value.code == "reponse_illisible" and not caught.value.uncertain


def test_ghost_admin_host_requires_authenticated_site_association(wire):
    client = transport.ContentTransport("ghost", {"base_url": "https://atelier.ghost.io", "key": KEY}, project_url=PROJECT)
    wire["responses"] += [Response({"site": {"url": PROJECT, "title": "Atelier"}}), Response({"posts": []})]
    assert client.check()["verified"]
    assert wire["calls"][0]["headers"]["Authorization"].startswith("Ghost ")
    assert wire["calls"][0]["headers"]["Accept-Version"] == "v5.0"
    assert wire["calls"][1]["method"] == "GET"
    wire["responses"] += [Response({"site": {"url": "https://other.example/"}}), Response({"posts": []})]
    assert client.check()["verified"] is False
    with pytest.raises(CmsError) as caught:
        client("POST", "/ghost/api/admin/posts/", {})
    assert caught.value.code == "non_verifie"


def test_shopify_domain_query_is_allowed_before_verification_but_mutation_is_not(wire):
    client = transport.ContentTransport("shopify", {"shop": "atelier.myshopify.com", "token": "fictitious-token"}, project_url=PROJECT)
    wire["responses"].append(Response({"data": {"shop": {"name": "Atelier", "primaryDomain": {"url": PROJECT}}}}))
    wire["responses"].append(Response({"data": {"blogs": {"nodes": [{"id": "gid://shopify/Blog/1", "title": "Conseils"}], "pageInfo": {"hasNextPage": False}}}}))
    assert client.check()["verified"]
    wire["responses"].append(Response({"data": {"articleCreate": {"article": {"id": "gid://shopify/Article/1"}}}}))
    client("POST", transport.SHOPIFY_PATH, {"query": "mutation Make($article: ArticleCreateInput!) { articleCreate(article: $article) { article { id } userErrors { message } } }", "variables": {"article": {"title": "Une page"}}})
    assert wire["calls"][2]["headers"]["X-Shopify-Access-Token"] == "fictitious-token"
    assert wire["calls"][0]["host"] == "atelier.myshopify.com"


@pytest.mark.parametrize("query", [
    "mutation { productDelete(input:{id:1}) { deletedProductId } }",
    "mutation { articleCreate(article:{title:\"ok\"}) { article{id} } productDelete(input:{id:1}) {id} }",
    "query A {shop{name}} mutation B { productDelete(input:{id:1}){id} }",
    "query { customers { id } }", "subscription { shop { name } }", "mutation { harmless: productDelete(input:{id:1}){id} }",
], ids=["product", "second-field", "second-operation", "customers", "subscription", "alias"])
def test_graphql_cannot_smuggle_a_non_editorial_operation(wire, query):
    client = transport.ContentTransport("shopify", {"shop": "atelier.myshopify.com", "token": "fictitious-token"}, project_url=PROJECT)
    with pytest.raises(CmsError) as caught:
        client("POST", transport.SHOPIFY_PATH, {"query": query})
    assert caught.value.code == "chemin" and not wire["calls"]


def test_webflow_requires_matching_site_id_and_domain(wire):
    client = transport.ContentTransport("webflow", {"site_id": SITE, "token": "fictitious-token"}, project_url=PROJECT)
    wire["responses"].append(Response({"id": SITE, "shortName": "atelier", "customDomains": [{"url": "atelier.example"}]}))
    wire["responses"].append(Response({"collections": []}))
    assert client.check()["verified"]
    wire["responses"].append(Response({"id": SITE, "customDomains": [{"url": "other.example"}]}))
    assert client.check() == {"verified": False, "label": "webflow", "reason": "autre_site", "destinations": []}


def test_webflow_picker_loads_only_schema_of_a_verified_site_collection(wire):
    client = transport.ContentTransport("webflow", {"site_id": SITE, "token": "fictitious-token"}, project_url=PROJECT)
    wire["responses"] += [Response({"id": SITE, "shortName": "atelier", "customDomains": [{"url": "atelier.example"}]}),
                          Response({"collections": [{"id": "collection1", "displayName": "Guides"}]})]
    state = client.check()
    assert state["destinations"] == [{"id": "collection1", "title": "Guides", "kind": "collection", "fields": [], "schema_loaded": False}]
    wire["responses"].append(Response({"id": "collection1", "fields": [{"slug": "body", "displayName": "Texte", "type": "RichText", "isRequired": True}]}))
    assert client.describe_destination("collection1")["fields"] == [{"slug": "body", "title": "Texte", "type": "RichText", "required": True, "editable": True}]
    count = len(wire["calls"])
    with pytest.raises(CmsError) as caught:
        client.describe_destination("other-site-collection")
    assert caught.value.code == "autre_site"
    with pytest.raises(CmsError):
        client("POST", "/v2/collections/other-site-collection/items/insert", {})
    assert len(wire["calls"]) == count


def test_wix_uses_inherited_default_url_and_never_trusts_an_edited_canonical(wire):
    client = transport.ContentTransport("wix", {"site_id": SITE, "token": "fictitious-token"}, project_url=PROJECT)
    tag = {"type": "link", "props": {"rel": "canonical", "href": PROJECT}}
    inherited = {"type": "link", "props": {"rel": "canonical", "href": "https://other.example/"}}
    wire["responses"].append(Response({"itemSeoTags": [{"resolvedTags": [{"source": "TAG_SOURCE_ITEM", "tag": tag, "inheritedTag": inherited}]}]}))
    assert not client.check()["verified"]
    wire["responses"].append(Response({"itemSeoTags": [{"resolvedTags": [{"source": "TAG_SOURCE_DEFAULT", "tag": tag}]}]}))
    assert client.check()["verified"]
    assert wire["calls"][-1]["headers"]["wix-site-id"] == SITE
    assert wire["calls"][-1]["headers"]["Authorization"] == "fictitious-token"


def test_prestashop_sends_xml_unchanged_and_preserves_installation_prefix(wire):
    client = transport.ContentTransport("prestashop", {"base_url": "https://atelier.example/shop", "key": "fictitious-key"}, project_url=PROJECT)
    wire["responses"].append(Response(raw=b"<prestashop><content_management_system/></prestashop>", content_type="application/xml"))
    assert client.check()["verified"]
    xml = '<prestashop><content><active>0</active><content><language id="1">Vélo</language></content></content></prestashop>'
    wire["responses"].append(Response(raw=b"<prestashop/>", content_type="application/xml"))
    response = client("POST", "/api/content_management_system", xml, {"Content-Type": "application/xml"})
    assert response.text() == "<prestashop/>"
    assert wire["calls"][-1]["body"] == xml.encode("utf-8")
    assert wire["calls"][-1]["path"] == "/shop/api/content_management_system"
    assert wire["calls"][-1]["headers"]["Authorization"].startswith("Basic ")


@pytest.mark.parametrize("method", ["GET", "POST"], ids=["read", "write"])
@pytest.mark.parametrize("raw,parser", [(b"not-json", "json"), (b"\xff", "text")], ids=["invalid-json", "invalid-utf8"])
def test_unreadable_success_body_preserves_write_uncertainty_without_exposing_it(wire, method, raw, parser):
    client = wp()
    verify_wp(client, wire)
    wire["responses"].append(Response(raw=raw))
    response = client(method, "/wp-json/wp/v2/posts", {"status": "draft"} if method == "POST" else None)
    with pytest.raises(CmsError) as error:
        getattr(response, parser)()
    assert error.value.code == "reponse_illisible" and error.value.uncertain is (method == "POST")
    assert "not-json" not in str(error.value) and len(wire["calls"]) == 2
