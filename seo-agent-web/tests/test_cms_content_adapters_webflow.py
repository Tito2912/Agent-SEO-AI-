"""Adaptateur Webflow Data v2 : mapping explicite, erreurs partielles de publication."""
from __future__ import annotations

import json

import pytest

from backend.cms_content_adapters import AdapterError, ArticleDraft, TransportResponse, get_adapter


def _draft(**overrides):
    base = {"title": "Un titre", "html": "<p>Corps.</p>", "slug": "un-titre",
            "excerpt": "Resume.", "language": "fr", "author": "Alice"}
    base.update(overrides)
    return ArticleDraft(**base)


def _config(**overrides):
    base = {"collection_id": "col-123",
            "field_mapping": {"title": "name", "slug": "slug", "body": "body",
                              "excerpt": "summary", "author": "author"}}
    base.update(overrides)
    return base


def _json(status, body):
    return TransportResponse(status=status, headers={"content-type": "application/json"},
                             body=json.dumps(body).encode("utf-8"))


def _item(**overrides):
    body = {"id": "item-1", "fieldData": {"name": "Un titre", "slug": "un-titre"},
            "lastUpdated": "2026-11-17T10:00:00Z", "isDraft": True}
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


def test_create_draft_uses_items_insert_and_applies_mapping():
    transport = _Transport([_json(201, {"items": [_item()]})])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    article = adapter.create_draft(_draft())
    assert article.status == "draft"
    assert article.revision == "2026-11-17T10:00:00Z"
    call = transport.calls[0]
    assert call["path"].endswith("/items/insert")
    fields = call["payload"]["items"][0]["fieldData"]
    assert fields == {"name": "Un titre", "slug": "un-titre", "body": "<p>Corps.</p>",
                      "summary": "Resume.", "author": "Alice"}


def test_create_draft_rejects_missing_mapped_author():
    transport = _Transport([])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft(author=""))
    assert excinfo.value.code == "author"
    assert transport.calls == []


def test_publish_partial_error_is_signaled_as_uncertain():
    transport = _Transport([
        _json(200, _item()),
        _json(200, {"publishedItemIds": [],
                    "errors": [{"itemId": "item-1", "reason": "missing_required"}]}),
    ])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("item-1", expected_revision="2026-11-17T10:00:00Z")
    assert excinfo.value.code == "partial"
    assert excinfo.value.uncertain is True


def test_publish_missing_item_in_response_is_partial():
    transport = _Transport([
        _json(200, _item()),
        _json(200, {"publishedItemIds": ["other-item"]}),
    ])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("item-1", expected_revision="2026-11-17T10:00:00Z")
    assert excinfo.value.code == "partial"


def test_publish_success_rereads_item():
    transport = _Transport([
        _json(200, _item(lastUpdated="2026-11-17T10:00:00Z")),
        _json(200, {"publishedItemIds": ["item-1"]}),
        _json(200, _item(lastUpdated="2026-11-17T11:00:00Z", isDraft=False,
                          lastPublished="2026-11-17T11:00:00Z")),
    ])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    article = adapter.publish("item-1", expected_revision="2026-11-17T10:00:00Z")
    assert article.status == "published"
    assert article.revision == "2026-11-17T11:00:00Z"


def test_update_draft_patches_field_data():
    transport = _Transport([
        _json(200, _item(lastUpdated="A")),
        _json(200, _item(lastUpdated="B", fieldData={"name": "Nouveau", "slug": "un-titre"})),
    ])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    article = adapter.update_draft("item-1", _draft(title="Nouveau"),
                                    expected_revision="A")
    assert article.revision == "B"
    patch = transport.calls[1]
    assert patch["method"] == "PATCH"
    assert patch["payload"]["fieldData"]["name"] == "Nouveau"


def test_locales_are_sent_when_configured():
    transport = _Transport([_json(201, {"items": [_item()]})])
    adapter = get_adapter("webflow", transport=transport,
                          config={**_config(), "locales": ("fr", "en")})
    adapter.create_draft(_draft())
    assert transport.calls[0]["payload"]["locales"] == ["fr", "en"]


def test_invalid_collection_id_refused_before_write():
    with pytest.raises(AdapterError) as excinfo:
        get_adapter("webflow", transport=lambda *a, **k: None,
                    config={"collection_id": "bad id!", "field_mapping": {"title": "t",
                                                                            "slug": "s",
                                                                            "body": "b"}})
    assert excinfo.value.code == "validation"


def test_create_uses_items_insert_path_plural_locales():
    """Nouvel endpoint /items/insert : locales au pluriel quand configuree."""
    transport = _Transport([_json(201, {"items": [_item()]})])
    adapter = get_adapter("webflow", transport=transport,
                          config={**_config(), "locales": ("en", "fr")})
    adapter.create_draft(_draft())
    assert transport.calls[0]["path"].endswith("/items/insert")
    assert transport.calls[0]["payload"]["locales"] == ["en", "fr"]


def test_conflict_stops_before_write():
    transport = _Transport([_json(200, _item(lastUpdated="current"))])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("item-1", _draft(title="Autre"), expected_revision="stale")
    assert excinfo.value.code == "conflict"
    assert [c["method"] for c in transport.calls] == ["GET"]


def test_conflict_on_publish_stops_before_publishing():
    transport = _Transport([_json(200, _item(lastUpdated="current"))])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("item-1", expected_revision="stale")
    assert excinfo.value.code == "conflict"
    assert len(transport.calls) == 1
    assert not transport.calls[0]["path"].endswith("/publish")


def test_create_without_optional_mapping_is_fine():
    """field_mapping sans excerpt/author : adaptateur accepte, ne reclame pas l'auteur."""
    transport = _Transport([_json(201, {"items": [_item()]})])
    adapter = get_adapter("webflow", transport=transport, config={
        "collection_id": "col-123",
        "field_mapping": {"title": "name", "slug": "slug", "body": "body"}})
    adapter.create_draft(_draft(author=""))
    fields = transport.calls[0]["payload"]["items"][0]["fieldData"]
    assert set(fields) == {"name", "slug", "body"}


def test_http_401_mapped_to_auth():
    transport = _Transport([TransportResponse(status=401, headers={}, body=b"{}")])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("item-1")
    assert excinfo.value.code == "auth"


def test_http_429_mapped_to_limit():
    transport = _Transport([TransportResponse(status=429, headers={}, body=b"{}")])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("item-1")
    assert excinfo.value.code == "limit"


def test_server_error_on_write_is_uncertain():
    transport = _Transport([TransportResponse(status=502, headers={}, body=b"{}")])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "server"
    assert excinfo.value.uncertain is True


def test_path_is_relative_not_full_url():
    transport = _Transport([_json(201, {"items": [_item()]})])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    adapter.create_draft(_draft())
    assert transport.calls[0]["path"].startswith("/v2/collections/")
    assert "://" not in transport.calls[0]["path"]


def test_create_draft_does_not_publish():
    """L'insert ne publie jamais : on reste en 'draft' tant que /items/publish n'est pas appele."""
    transport = _Transport([_json(201, {"items": [_item(isDraft=True)]})])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    article = adapter.create_draft(_draft())
    assert article.status == "draft"


def test_publish_errors_list_signals_partial_and_uncertain():
    """Une erreur partielle ne retente pas l'envoi : l'appelant doit verifier cote CMS."""
    transport = _Transport([
        _json(200, _item(lastUpdated="A")),
        _json(200, {"publishedItemIds": [], "errors": [{"itemId": "item-1",
                                                         "reason": "locked"}]}),
    ])
    adapter = get_adapter("webflow", transport=transport, config=_config())
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("item-1", expected_revision="A")
    assert excinfo.value.code == "partial"
    assert excinfo.value.uncertain is True
    # Pas de troisieme requete : l'adaptateur ne retente pas.
    assert len(transport.calls) == 2
