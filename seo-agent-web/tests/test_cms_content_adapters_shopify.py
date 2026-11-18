"""Adaptateur Shopify GraphQL : variables, userErrors en HTTP 200, auteur obligatoire."""
from __future__ import annotations

import json

import pytest

from backend.cms_content_adapters import AdapterError, ArticleDraft, TransportResponse, get_adapter


def _draft(**overrides):
    base = {"title": "Un titre", "html": "<p>Corps.</p>", "slug": "un-titre",
            "excerpt": "Resume.", "language": "fr", "author": "Alice Auteur"}
    base.update(overrides)
    return ArticleDraft(**base)


def _config(**overrides):
    base = {"blog_id": "gid://shopify/Blog/9"}
    base.update(overrides)
    return base


def _json(status, body):
    return TransportResponse(status=status, headers={"content-type": "application/json"},
                             body=json.dumps(body).encode("utf-8"))


def _article(**overrides):
    body = {"id": "gid://shopify/Article/1234", "title": "Un titre",
            "handle": "un-titre", "isPublished": False, "publishedAt": None,
            "updatedAt": "2026-11-17T10:00:00Z",
            "author": {"name": "Alice Auteur"}, "blog": {"id": "gid://shopify/Blog/9"}}
    body.update(overrides)
    return body


class _Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, path, payload=None, headers=None):
        self.calls.append({"method": method, "path": path, "payload": payload,
                           "headers": dict(headers or {})})
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_create_draft_uses_graphql_variables_not_interpolation():
    transport = _Transport([_json(200, {"data": {"articleCreate": {"article": _article(),
                                                                    "userErrors": []}}})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    article = adapter.create_draft(_draft())
    assert article.status == "draft"
    call = transport.calls[0]
    assert call["method"] == "POST"
    assert call["path"].endswith("/graphql.json")
    # La charge utilise bien `variables` (pas d'interpolation dans la requete GraphQL).
    assert "variables" in call["payload"]
    variables = call["payload"]["variables"]["article"]
    assert variables["blogId"] == "gid://shopify/Blog/9"
    assert variables["isPublished"] is False
    assert variables["author"] == {"name": "Alice Auteur"}
    # La requete GraphQL elle-meme ne doit JAMAIS contenir le titre interpole.
    assert "Un titre" not in call["payload"]["query"]


def test_user_errors_in_http_200_are_errors():
    transport = _Transport([_json(200, {"data": {"articleCreate": {
        "article": None,
        "userErrors": [{"field": ["handle"], "message": "handle deja utilise", "code": "TAKEN"}]}}})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "user_errors"
    assert excinfo.value.details["userErrors"][0]["code"] == "TAKEN"


def test_top_level_graphql_errors_mapped_to_refused():
    transport = _Transport([_json(200, {"errors": [{"message": "Access denied"}]})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "refused"


def test_create_draft_rejects_missing_author():
    transport = _Transport([])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft(author=""))
    assert excinfo.value.code == "author"
    assert transport.calls == []


def test_publish_requires_expected_revision():
    transport = _Transport([_json(200, {"data": {"article": _article()}})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("1234")
    assert excinfo.value.code == "revision_required"


def test_publish_toggles_is_published():
    transport = _Transport([
        _json(200, {"data": {"article": _article(updatedAt="2026-11-17T10:00:00Z")}}),
        _json(200, {"data": {"articleUpdate": {
            "article": _article(isPublished=True, publishedAt="2026-11-17T11:00:00Z",
                                 updatedAt="2026-11-17T11:00:00Z"),
            "userErrors": []}}}),
    ])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    article = adapter.publish("1234", expected_revision="2026-11-17T10:00:00Z")
    assert article.status == "published"
    variables = transport.calls[1]["payload"]["variables"]
    assert variables["article"]["isPublished"] is True
    assert variables["id"] == "gid://shopify/Article/1234"


def test_numeric_remote_id_is_wrapped_in_gid():
    transport = _Transport([_json(200, {"data": {"article": _article()}})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    adapter.read("1234")
    assert transport.calls[0]["payload"]["variables"]["id"] == "gid://shopify/Article/1234"


def test_read_handles_missing_article_as_not_found():
    transport = _Transport([_json(200, {"data": {"article": None}})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("1234")
    assert excinfo.value.code == "not_found"


def test_create_always_sets_is_published_false():
    transport = _Transport([_json(200, {"data": {"articleCreate": {
        "article": _article(), "userErrors": []}}})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    adapter.create_draft(_draft())
    variables = transport.calls[0]["payload"]["variables"]["article"]
    assert variables["isPublished"] is False
    assert variables["blogId"] == "gid://shopify/Blog/9"


def test_conflict_stops_before_write():
    transport = _Transport([_json(200, {"data": {"article": _article(updatedAt="current")}})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("1234", _draft(title="Autre"), expected_revision="stale")
    assert excinfo.value.code == "conflict"
    # Une seule requete GraphQL (le read inclus dans update_draft) : aucun articleUpdate envoye.
    assert len(transport.calls) == 1
    assert "articleUpdate" not in transport.calls[0]["payload"]["query"]


def test_user_errors_on_update_are_errors():
    transport = _Transport([
        _json(200, {"data": {"article": _article(updatedAt="A")}}),
        _json(200, {"data": {"articleUpdate": {
            "article": None,
            "userErrors": [{"field": ["title"], "message": "titre deja utilise",
                             "code": "TAKEN"}]}}}),
    ])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("1234", _draft(title="Autre"), expected_revision="A")
    assert excinfo.value.code == "user_errors"


def test_http_500_on_write_is_uncertain():
    transport = _Transport([TransportResponse(status=500, headers={}, body=b"{}")])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "server"
    assert excinfo.value.uncertain is True


def test_http_401_is_auth_not_uncertain():
    transport = _Transport([TransportResponse(status=401, headers={}, body=b"{}")])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("1234")
    assert excinfo.value.code == "auth"
    assert excinfo.value.uncertain is False


def test_path_is_relative_not_full_url():
    transport = _Transport([_json(200, {"data": {"articleCreate": {
        "article": _article(), "userErrors": []}}})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    adapter.create_draft(_draft())
    path = transport.calls[0]["path"]
    assert path.startswith("/admin/api/")
    assert "://" not in path


def test_create_graphql_query_is_a_constant_string():
    transport = _Transport([_json(200, {"data": {"articleCreate": {
        "article": _article(), "userErrors": []}}})])
    adapter = get_adapter("shopify", transport=transport, config=_config())
    adapter.create_draft(_draft())
    sent_query_1 = transport.calls[0]["payload"]["query"]
    # Deuxieme appel avec des valeurs differentes : la requete elle-meme ne doit pas changer.
    transport.responses.append(_json(200, {"data": {"articleCreate": {
        "article": _article(), "userErrors": []}}}))
    adapter.create_draft(_draft(title="Different", slug="autre-slug"))
    sent_query_2 = transport.calls[1]["payload"]["query"]
    assert sent_query_1 == sent_query_2
    assert "Different" not in sent_query_2 and "autre-slug" not in sent_query_2
