"""Adaptateur WordPress REST natif : contrat, revision, erreurs structurees."""
from __future__ import annotations

import json

import pytest

from backend.cms_content_adapters import AdapterError, ArticleDraft, TransportResponse, get_adapter


def _draft(**overrides):
    base = {"title": "Un titre", "html": "<p>Un paragraphe.</p>", "slug": "un-titre",
            "excerpt": "Resume.", "language": "fr", "author": "3"}
    base.update(overrides)
    return ArticleDraft(**base)


def _json(status, body):
    return TransportResponse(status=status, headers={"content-type": "application/json"},
                             body=json.dumps(body).encode("utf-8"))


class _Transport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, method, path, payload=None, headers=None):
        self.calls.append({"method": method, "path": path, "payload": payload,
                           "headers": dict(headers or {})})
        if not self.responses:
            raise AssertionError("Appel inattendu : %s %s" % (method, path))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _wp_response(**overrides):
    body = {"id": 42, "status": "draft", "link": "https://exemple.fr/?p=42",
            "modified_gmt": "2026-11-17T10:00:00"}
    body.update(overrides)
    return body


def test_create_draft_posts_with_draft_status_and_returns_remote():
    transport = _Transport([_json(201, _wp_response())])
    adapter = get_adapter("wordpress", transport=transport, config={})
    article = adapter.create_draft(_draft())
    assert article.id == "42"
    assert article.status == "draft"
    assert article.url == "https://exemple.fr/?p=42"
    assert article.revision == "2026-11-17T10:00:00"
    call = transport.calls[0]
    assert call["method"] == "POST"
    assert call["path"] == "/wp-json/wp/v2/posts"
    assert call["payload"]["status"] == "draft"
    assert call["payload"]["author"] == 3


def test_create_draft_rejects_non_integer_author():
    transport = _Transport([])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft(author="Alice"))
    assert excinfo.value.code == "author"
    assert transport.calls == []  # aucune requete envoyee


def test_create_draft_limits_image_width_without_changing_dimensions():
    transport = _Transport([_json(201, _wp_response())])
    adapter = get_adapter("wordpress", transport=transport, config={})
    image = '<img src="https://exemple.fr/photo.webp" width="1280" height="800" alt="Atelier" />'
    example = '<code>&lt;img src="example.webp"&gt;</code>'
    adapter.create_draft(_draft(html=f"<p>{image}</p>\n{example}"))
    assert transport.calls[0]["payload"]["content"] == (
        f'<p><img style="max-width:100%;height:auto"{image[4:]}</p>\n{example}'
    )


def test_publish_requires_expected_revision():
    transport = _Transport([_json(200, _wp_response())])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("42")
    assert excinfo.value.code == "revision_required"


def test_publish_refuses_revision_mismatch():
    transport = _Transport([_json(200, _wp_response(modified_gmt="2026-11-17T11:00:00"))])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("42", expected_revision="2026-11-17T10:00:00")
    assert excinfo.value.code == "conflict"


def test_publish_sends_status_publish_on_match():
    transport = _Transport([
        _json(200, _wp_response(modified_gmt="2026-11-17T10:00:00")),
        _json(200, _wp_response(status="publish", modified_gmt="2026-11-17T11:00:00")),
    ])
    adapter = get_adapter("wordpress", transport=transport, config={})
    article = adapter.publish("42", expected_revision="2026-11-17T10:00:00")
    assert article.status == "published"
    assert transport.calls[1]["method"] == "POST"
    assert transport.calls[1]["payload"] == {"status": "publish"}


def test_update_draft_requires_revision_and_sends_body():
    transport = _Transport([
        _json(200, _wp_response(modified_gmt="A")),
        _json(200, _wp_response(modified_gmt="B")),
    ])
    adapter = get_adapter("wordpress", transport=transport, config={})
    article = adapter.update_draft("42", _draft(title="Nouveau"), expected_revision="A")
    assert article.revision == "B"
    assert "status" not in transport.calls[1]["payload"]  # update_draft ne publie pas
    assert transport.calls[1]["payload"]["title"] == "Nouveau"


def test_bad_html_is_refused_before_network():
    transport = _Transport([])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft(html="<p>x<script>y</script></p>"))
    assert excinfo.value.code == "validation"
    assert transport.calls == []


def test_auth_error_is_mapped():
    transport = _Transport([TransportResponse(status=401, headers={}, body=b"{}")])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("42")
    assert excinfo.value.code == "auth"


def test_network_error_on_write_is_uncertain():
    transport = _Transport([ConnectionResetError("drop")])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "network"
    assert excinfo.value.uncertain is True


def test_invalid_remote_id_refused():
    transport = _Transport([])
    adapter = get_adapter("wordpress", transport=transport, config={})
    for bad in ("0", "-1", "abc", ""):
        with pytest.raises(AdapterError):
            adapter.read(bad)


def test_conflict_stops_before_write():
    transport = _Transport([_json(200, _wp_response(modified_gmt="current"))])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("42", _draft(title="Autre"), expected_revision="stale")
    assert excinfo.value.code == "conflict"
    assert [c["method"] for c in transport.calls] == ["GET"]  # pas d'ecriture


def test_404_on_read_is_not_found():
    transport = _Transport([TransportResponse(status=404, headers={}, body=b"{}")])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("42")
    assert excinfo.value.code == "not_found"


def test_rate_limit_is_mapped():
    transport = _Transport([TransportResponse(status=429, headers={}, body=b"{}")])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("42")
    assert excinfo.value.code == "limit"


def test_server_error_on_write_is_uncertain():
    transport = _Transport([TransportResponse(status=502, headers={}, body=b"{}")])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "server"
    assert excinfo.value.uncertain is True


def test_adapter_never_adds_authorization_header():
    """L'adaptateur ne manipule pas de secret : le transport injecte Authorization."""
    transport = _Transport([_json(201, _wp_response())])
    adapter = get_adapter("wordpress", transport=transport, config={})
    adapter.create_draft(_draft())
    headers = transport.calls[0]["headers"]
    assert "Authorization" not in headers
    assert "authorization" not in {k.lower() for k in headers}


def test_uncertain_from_transport_cmserror_is_preserved():
    """Si le transport leve une CmsError-like (uncertain=True), on conserve l'incertitude."""
    class _CmsErrorLike(Exception):
        def __init__(self, message):
            super().__init__(message)
            self.uncertain = True
            self.code = "redirection"
    transport = _Transport([_CmsErrorLike("WordPress redirige")])
    adapter = get_adapter("wordpress", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.uncertain is True
    assert excinfo.value.code == "redirection"


def test_path_is_relative_not_full_url():
    transport = _Transport([_json(201, _wp_response())])
    adapter = get_adapter("wordpress", transport=transport, config={})
    adapter.create_draft(_draft())
    path = transport.calls[0]["path"]
    assert path.startswith("/wp-json/")
    assert "://" not in path  # le transport ajoute https://domaine


def test_publish_returns_published_status():
    transport = _Transport([
        _json(200, _wp_response(modified_gmt="A")),
        _json(200, _wp_response(status="publish", modified_gmt="B")),
    ])
    adapter = get_adapter("wordpress", transport=transport, config={})
    article = adapter.publish("42", expected_revision="A")
    assert article.status == "published"


def test_scheduled_status_translation():
    transport = _Transport([_json(200, _wp_response(status="future"))])
    adapter = get_adapter("wordpress", transport=transport, config={})
    article = adapter.read("42")
    assert article.status == "scheduled"
