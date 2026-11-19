"""Adaptateur Wix Blog v3 Draft Posts : Ricos subset, memberId obligatoire."""
from __future__ import annotations

import json

import pytest

from backend.cms_content_adapters import AdapterError, ArticleDraft, TransportResponse, get_adapter


def _draft(**overrides):
    base = {"title": "Un titre", "html": "<p>Un paragraphe <strong>riche</strong>.</p>",
            "slug": "un-titre", "excerpt": "Resume.", "language": "fr",
            "author": "member-xyz"}
    base.update(overrides)
    return ArticleDraft(**base)


def _json(status, body):
    return TransportResponse(status=status, headers={"content-type": "application/json"},
                             body=json.dumps(body).encode("utf-8"))


def _draft_post(**overrides):
    body = {"id": "draft-1", "title": "Un titre", "slug": "un-titre",
            "memberId": "member-xyz", "revision": "7"}
    body.update(overrides)
    return {"draftPost": body}


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


def test_create_draft_sends_publish_false_and_ricos_content():
    transport = _Transport([_json(201, _draft_post())])
    adapter = get_adapter("wix", transport=transport, config={})
    article = adapter.create_draft(_draft())
    assert article.status == "draft"
    assert article.revision == "7"
    body = transport.calls[0]["payload"]
    assert body["publish"] is False
    rich = body["draftPost"]["richContent"]
    assert rich["nodes"][0]["type"] == "PARAGRAPH"
    assert body["draftPost"]["memberId"] == "member-xyz"


def test_create_draft_requires_member_id():
    transport = _Transport([])
    adapter = get_adapter("wix", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft(author=""))
    assert excinfo.value.code == "author"


def test_create_draft_refuses_unsupported_html():
    transport = _Transport([])
    adapter = get_adapter("wix", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft(html="<p>x</p><table><tr><td>y</td></tr></table>"))
    assert excinfo.value.code == "validation"
    assert transport.calls == []


def test_publish_requires_expected_revision():
    transport = _Transport([_json(200, _draft_post())])
    adapter = get_adapter("wix", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("draft-1")
    assert excinfo.value.code == "revision_required"


def test_publish_uses_dedicated_endpoint():
    transport = _Transport([
        _json(200, _draft_post()),
        _json(200, {"post": {"id": "post-1", "revision": "8", "published": True,
                              "url": "https://exemple.fr/blog/un-titre"}}),
    ])
    adapter = get_adapter("wix", transport=transport, config={})
    article = adapter.publish("draft-1", expected_revision="7")
    assert article.status == "published"
    assert transport.calls[1]["path"].endswith("/publish")


def test_update_draft_sends_field_mask():
    transport = _Transport([
        _json(200, _draft_post()),
        _json(200, _draft_post(revision="8", title="Nouveau")),
    ])
    adapter = get_adapter("wix", transport=transport, config={})
    article = adapter.update_draft("draft-1", _draft(title="Nouveau"),
                                    expected_revision="7")
    assert article.revision == "8"
    patch = transport.calls[1]
    assert patch["method"] == "PATCH"
    assert "fieldMask" in patch["payload"]
    assert set(patch["payload"]["fieldMask"]["paths"]) >= {"title", "richContent", "memberId"}


def test_conflict_status_from_api_mapped_cleanly():
    transport = _Transport([_json(409, {"message": "revision mismatch"})])
    adapter = get_adapter("wix", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("draft-1")
    assert excinfo.value.code == "conflict"


def test_create_always_sends_publish_false():
    transport = _Transport([_json(201, _draft_post())])
    adapter = get_adapter("wix", transport=transport, config={})
    adapter.create_draft(_draft())
    body = transport.calls[0]["payload"]
    assert body["publish"] is False
    assert body["draftPost"]["memberId"] == "member-xyz"


def test_conflict_stops_before_write():
    transport = _Transport([_json(200, _draft_post(revision="current"))])
    adapter = get_adapter("wix", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("draft-1", _draft(title="Autre"), expected_revision="stale")
    assert excinfo.value.code == "conflict"
    assert [c["method"] for c in transport.calls] == ["GET"]


def test_ricos_supports_headings_lists_and_links():
    transport = _Transport([_json(201, _draft_post())])
    adapter = get_adapter("wix", transport=transport, config={})
    html = ('<h2>Titre</h2><p>Para <strong>gras</strong> et '
            '<a href="https://exemple.fr">lien</a>.</p>'
            '<ul><li>Un</li><li>Deux</li></ul>')
    adapter.create_draft(_draft(html=html))
    rich = transport.calls[0]["payload"]["draftPost"]["richContent"]
    types = [node["type"] for node in rich["nodes"]]
    assert types == ["HEADING", "PARAGRAPH", "BULLETED_LIST"]


@pytest.mark.parametrize("html", [
    "<p>x</p><script>alert(1)</script>",
    "<table><tr><td>x</td></tr></table>",
    "<p><iframe src='https://x'></iframe></p>",
    "<p><a href='javascript:x'>x</a></p>",
], ids=["script", "table", "iframe", "js-link"])
def test_unsupported_html_refuses_rather_than_silent_drop(html):
    transport = _Transport([])
    adapter = get_adapter("wix", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft(html=html))
    assert excinfo.value.code == "validation"
    assert transport.calls == []


def test_server_error_on_write_is_uncertain():
    transport = _Transport([TransportResponse(status=502, headers={}, body=b"{}")])
    adapter = get_adapter("wix", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "server"
    assert excinfo.value.uncertain is True


def test_path_is_relative_not_full_url():
    transport = _Transport([_json(201, _draft_post())])
    adapter = get_adapter("wix", transport=transport, config={})
    adapter.create_draft(_draft())
    assert transport.calls[0]["path"].startswith("/blog/v3/draft-posts")
    assert "://" not in transport.calls[0]["path"]


def test_update_passes_revision_and_field_mask():
    transport = _Transport([
        _json(200, _draft_post(revision="7")),
        _json(200, _draft_post(revision="8", title="Nouveau")),
    ])
    adapter = get_adapter("wix", transport=transport, config={})
    adapter.update_draft("draft-1", _draft(title="Nouveau"), expected_revision="7")
    patch = transport.calls[1]["payload"]
    assert patch["draftPost"]["revision"] == "7"
    assert set(patch["fieldMask"]["paths"]) >= {"title", "richContent", "memberId"}
