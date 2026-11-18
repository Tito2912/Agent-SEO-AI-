"""Adaptateur Ghost Admin : updated_at obligatoire, pas de newsletter, revision = updated_at."""
from __future__ import annotations

import json

import pytest

from backend.cms_content_adapters import AdapterError, ArticleDraft, TransportResponse, get_adapter


def _draft(**overrides):
    base = {"title": "Un titre", "html": "<p>Corps.</p>", "slug": "un-titre",
            "excerpt": "Resume.", "language": "fr", "author": "staff@exemple.fr"}
    base.update(overrides)
    return ArticleDraft(**base)


def _json(status, body):
    return TransportResponse(status=status, headers={"content-type": "application/json"},
                             body=json.dumps(body).encode("utf-8"))


def _ghost_post(**overrides):
    body = {"id": "abc123", "status": "draft", "url": "https://exemple.fr/a",
            "updated_at": "2026-11-17T10:00:00.000Z"}
    body.update(overrides)
    return {"posts": [body]}


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


def test_create_draft_disables_newsletter_and_uses_source_html():
    transport = _Transport([_json(201, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    article = adapter.create_draft(_draft())
    assert article.id == "abc123"
    assert article.revision == "2026-11-17T10:00:00.000Z"
    call = transport.calls[0]
    assert "?source=html" in call["path"]
    assert call["headers"]["Accept-Version"].startswith("v5")
    post = call["payload"]["posts"][0]
    assert post["send_email_when_published"] is False
    assert post["status"] == "draft"
    assert post["authors"] == [{"email": "staff@exemple.fr"}]


def test_update_draft_includes_updated_at_from_read():
    transport = _Transport([
        _json(200, _ghost_post(updated_at="2026-11-17T09:00:00.000Z")),
        _json(200, _ghost_post(updated_at="2026-11-17T10:00:00.000Z")),
    ])
    adapter = get_adapter("ghost", transport=transport, config={})
    article = adapter.update_draft("abc123", _draft(title="Autre"),
                                   expected_revision="2026-11-17T09:00:00.000Z")
    assert article.revision == "2026-11-17T10:00:00.000Z"
    write = transport.calls[1]
    assert write["method"] == "PUT"
    assert write["payload"]["posts"][0]["updated_at"] == "2026-11-17T09:00:00.000Z"


def test_publish_without_expected_revision_refused():
    transport = _Transport([_json(200, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("abc123")
    assert excinfo.value.code == "revision_required"


def test_publish_sends_status_published_with_updated_at():
    transport = _Transport([
        _json(200, _ghost_post(updated_at="rev-1")),
        _json(200, _ghost_post(status="published", url="https://exemple.fr/a",
                               updated_at="rev-2")),
    ])
    adapter = get_adapter("ghost", transport=transport, config={})
    article = adapter.publish("abc123", expected_revision="rev-1")
    assert article.status == "published"
    assert transport.calls[1]["payload"]["posts"][0] == {"status": "published", "updated_at": "rev-1"}


def test_422_is_mapped_to_conflict():
    transport = _Transport([_json(422, {"errors": [{"message": "updated_at obsolete"}]})])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("abc123")
    assert excinfo.value.code == "conflict"


def test_invalid_id_refused_before_network():
    transport = _Transport([])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("abc 123")  # espace interdit
    assert excinfo.value.code == "validation"
    assert transport.calls == []


def test_network_error_on_create_is_uncertain():
    transport = _Transport([TimeoutError("drop")])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "network"
    assert excinfo.value.uncertain is True


def test_create_defaults_to_draft_status():
    transport = _Transport([_json(201, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    adapter.create_draft(_draft())
    post = transport.calls[0]["payload"]["posts"][0]
    assert post["status"] == "draft"
    assert "published_at" not in post


def test_create_without_updated_at():
    transport = _Transport([_json(201, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    adapter.create_draft(_draft())
    post = transport.calls[0]["payload"]["posts"][0]
    assert "updated_at" not in post  # pas d'updated_at a la creation


def test_conflict_stops_before_write():
    transport = _Transport([_json(200, _ghost_post(updated_at="current"))])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("abc123", _draft(title="Autre"), expected_revision="stale")
    assert excinfo.value.code == "conflict"
    assert [c["method"] for c in transport.calls] == ["GET"]


def test_author_as_id_passes_through():
    transport = _Transport([_json(201, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    adapter.create_draft(_draft(author="641b2c18ef5a920001bcdef1"))
    authors = transport.calls[0]["payload"]["posts"][0]["authors"]
    assert authors == [{"id": "641b2c18ef5a920001bcdef1"}]


def test_404_on_read_is_not_found():
    transport = _Transport([TransportResponse(status=404, headers={}, body=b"{}")])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("abc123")
    assert excinfo.value.code == "not_found"


def test_server_error_on_write_is_uncertain():
    transport = _Transport([TransportResponse(status=502, headers={}, body=b"{}")])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "server"
    assert excinfo.value.uncertain is True


def test_path_is_relative_not_full_url():
    transport = _Transport([_json(201, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    adapter.create_draft(_draft())
    path = transport.calls[0]["path"]
    assert path.startswith("/ghost/api/admin/")
    assert "://" not in path


def test_scheduled_status_translation():
    transport = _Transport([_json(200, _ghost_post(status="scheduled"))])
    adapter = get_adapter("ghost", transport=transport, config={})
    article = adapter.read("abc123")
    assert article.status == "scheduled"
