"""Adaptateur Ghost Admin : revision SHA-256 opaque, updated_at reel pour PUT, pas de newsletter.

Ghost 6.73 peut garder le meme `updated_at` apres une edition courte : la revision Noyaru
est donc un hash SHA-256 du snapshot renvoye, pas la date. Ces tests prouvent que :
- l'adaptateur detecte une edition a `updated_at` inchange (titre ou HTML) ;
- le hash est stable a l'ordre des cles JSON mais sensible aux espaces / statut / date ;
- le PUT envoie le vrai `updated_at` relu, jamais le hash ;
- une creation reste brouillon et une publication est toujours explicite ;
- une reponse illisible ou en erreur APRES ecriture reste incertaine (jamais de retry aveugle).

Tous les tests passent par un transport injecte ; aucun vrai service Ghost n'est joint.
"""
from __future__ import annotations

import hashlib
import json

import pytest

from backend.cms_content_adapters import AdapterError, ArticleDraft, TransportResponse, get_adapter


# Champs du snapshot que l'adaptateur hashe. Reproduit ici volontairement : les tests
# derivent le hash a partir des fixtures pour prouver le contrat sans importer l'implementation.
_REVISION_FIELDS = ("id", "title", "slug", "html", "custom_excerpt", "status", "updated_at")


def _revision(post):
    snapshot = {name: post.get(name) for name in _REVISION_FIELDS}
    serialized = json.dumps(
        snapshot, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _draft(**overrides):
    base = {"title": "Un titre", "html": "<p>Corps.</p>", "slug": "un-titre",
            "excerpt": "Resume.", "language": "fr", "author": "staff@exemple.fr"}
    base.update(overrides)
    return ArticleDraft(**base)


def _json_response(status, body):
    return TransportResponse(status=status, headers={"content-type": "application/json"},
                             body=json.dumps(body).encode("utf-8"))


def _ghost_post(**overrides):
    # Contrat HTML reel de Ghost >= 5 : paragraphes enveloppes par <p>, URL canonique,
    # `custom_excerpt` renseigne. Les IDs restent courts pour la lisibilite.
    body = {"id": "abc123", "status": "draft", "url": "https://exemple.fr/a",
            "title": "Un titre", "slug": "un-titre",
            "html": "<p>Corps.</p>", "custom_excerpt": "Resume.",
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


# ---- Revision opaque ---------------------------------------------------

def test_revision_is_opaque_sha256_hex_not_updated_at():
    transport = _Transport([_json_response(200, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    article = adapter.read("abc123")
    assert len(article.revision) == 64
    assert all(ch in "0123456789abcdef" for ch in article.revision)
    assert article.revision != "2026-11-17T10:00:00.000Z"


def test_revision_is_stable_regardless_of_response_key_order():
    initial = _ghost_post()
    # Meme contenu, cles renvoyees dans l'ordre inverse : la serialisation canonique
    # (sort_keys=True) doit produire exactement le meme hash.
    reversed_post = dict(reversed(list(initial["posts"][0].items())))
    transport = _Transport([
        _json_response(200, initial),
        _json_response(200, {"posts": [reversed_post]}),
    ])
    adapter = get_adapter("ghost", transport=transport, config={})
    assert adapter.read("abc123").revision == adapter.read("abc123").revision


def test_revision_is_sensitive_to_html_whitespace_status_and_date():
    base = _ghost_post()["posts"][0]
    with_extra_space = _ghost_post(html="<p>Corps. </p>")["posts"][0]
    published = _ghost_post(status="published")["posts"][0]
    later = _ghost_post(updated_at="2026-11-17T10:00:01.000Z")["posts"][0]
    assert _revision(base) != _revision(with_extra_space)
    assert _revision(base) != _revision(published)
    assert _revision(base) != _revision(later)


def test_revision_matches_canonical_formula_from_fixture():
    body = _ghost_post()
    transport = _Transport([_json_response(200, body)])
    adapter = get_adapter("ghost", transport=transport, config={})
    article = adapter.read("abc123")
    assert article.revision == _revision(body["posts"][0])


# ---- Edition a updated_at identique : conflit AVANT ecriture -----------

def test_edit_title_at_same_updated_at_triggers_conflict_before_put():
    initial = _ghost_post()
    edited = _ghost_post(title="Autre")  # updated_at identique, titre different
    transport = _Transport([
        _json_response(200, initial),
        _json_response(200, edited),
    ])
    adapter = get_adapter("ghost", transport=transport, config={})
    preview = adapter.read("abc123")
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("abc123", _draft(title="Final"),
                             expected_revision=preview.revision)
    assert excinfo.value.code == "conflict"
    assert excinfo.value.uncertain is False
    # Aucune PUT envoyee : la pre-relecture a detecte la divergence du hash.
    assert [c["method"] for c in transport.calls] == ["GET", "GET"]


def test_edit_html_space_at_same_updated_at_triggers_conflict_before_put():
    initial = _ghost_post()
    edited = _ghost_post(html="<p>Corps. </p>")  # espace ajoute, updated_at inchange
    transport = _Transport([
        _json_response(200, initial),
        _json_response(200, edited),
    ])
    adapter = get_adapter("ghost", transport=transport, config={})
    preview = adapter.read("abc123")
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("abc123", _draft(title="Final"),
                             expected_revision=preview.revision)
    assert excinfo.value.code == "conflict"
    assert [c["method"] for c in transport.calls] == ["GET", "GET"]


# ---- Ecritures : vrai updated_at, jamais le hash -----------------------

def test_put_payload_carries_real_updated_at_not_hash_on_update():
    pre_read = _ghost_post(updated_at="2026-11-17T09:00:00.000Z")
    after = _ghost_post(updated_at="2026-11-17T10:00:00.000Z")
    transport = _Transport([_json_response(200, pre_read), _json_response(200, after)])
    adapter = get_adapter("ghost", transport=transport, config={})
    preview_revision = _revision(pre_read["posts"][0])
    article = adapter.update_draft("abc123", _draft(title="Autre"),
                                   expected_revision=preview_revision)
    put_call = transport.calls[1]
    assert put_call["method"] == "PUT"
    assert put_call["payload"]["posts"][0]["updated_at"] == "2026-11-17T09:00:00.000Z"
    # La revision renvoyee reste un hash, jamais la date brute.
    assert article.revision != "2026-11-17T10:00:00.000Z"
    assert len(article.revision) == 64


def test_publish_sends_real_updated_at_not_hash():
    pre_read = _ghost_post(updated_at="2026-11-17T09:00:00.000Z")
    after = _ghost_post(status="published", updated_at="2026-11-17T10:00:00.000Z")
    transport = _Transport([_json_response(200, pre_read), _json_response(200, after)])
    adapter = get_adapter("ghost", transport=transport, config={})
    preview_revision = _revision(pre_read["posts"][0])
    article = adapter.publish("abc123", expected_revision=preview_revision)
    assert article.status == "published"
    put_call = transport.calls[1]
    assert put_call["method"] == "PUT"
    assert put_call["payload"]["posts"][0] == {
        "status": "published", "updated_at": "2026-11-17T09:00:00.000Z"}


def test_publish_without_expected_revision_refused_certain():
    transport = _Transport([_json_response(200, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("abc123")
    assert excinfo.value.code == "revision_required"
    assert excinfo.value.uncertain is False


# ---- Creation : brouillon, formats=html, sans newsletter ---------------

def test_create_draft_uses_source_and_formats_html_and_disables_newsletter():
    transport = _Transport([_json_response(201, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    article = adapter.create_draft(_draft())
    assert article.id == "abc123"
    assert len(article.revision) == 64
    call = transport.calls[0]
    assert "source=html" in call["path"]
    assert "formats=html" in call["path"]
    assert call["headers"]["Accept-Version"].startswith("v5")
    post = call["payload"]["posts"][0]
    assert post["send_email_when_published"] is False
    assert "email_segment" not in post
    assert post["status"] == "draft"
    assert post["authors"] == [{"email": "staff@exemple.fr"}]


def test_create_defaults_to_draft_without_updated_at_or_published_at():
    transport = _Transport([_json_response(201, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    adapter.create_draft(_draft())
    post = transport.calls[0]["payload"]["posts"][0]
    assert post["status"] == "draft"
    assert "published_at" not in post
    assert "updated_at" not in post  # pas d'updated_at fabrique a la creation


def test_get_read_requests_formats_html():
    transport = _Transport([_json_response(200, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    adapter.read("abc123")
    assert "formats=html" in transport.calls[0]["path"]


def test_put_requests_source_and_formats_html():
    pre = _ghost_post()
    transport = _Transport([_json_response(200, pre), _json_response(200, pre)])
    adapter = get_adapter("ghost", transport=transport, config={})
    adapter.update_draft("abc123", _draft(title="X"),
                         expected_revision=_revision(pre["posts"][0]))
    put_path = transport.calls[1]["path"]
    assert "source=html" in put_path
    assert "formats=html" in put_path


def test_author_as_id_passes_through_without_inventing_fields():
    transport = _Transport([_json_response(201, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    adapter.create_draft(_draft(author="641b2c18ef5a920001bcdef1"))
    authors = transport.calls[0]["payload"]["posts"][0]["authors"]
    assert authors == [{"id": "641b2c18ef5a920001bcdef1"}]


# ---- Refus avant ecriture : restent certains ---------------------------

def test_invalid_id_refused_before_network_certain():
    transport = _Transport([])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("abc 123")  # espace interdit
    assert excinfo.value.code == "validation"
    assert excinfo.value.uncertain is False
    assert transport.calls == []


def test_422_on_pre_read_is_conflict_certain():
    transport = _Transport([_json_response(422, {"errors": [{"message": "updated_at obsolete"}]})])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("abc123")
    assert excinfo.value.code == "conflict"
    assert excinfo.value.uncertain is False


def test_404_on_read_is_not_found_certain():
    transport = _Transport([TransportResponse(status=404, headers={}, body=b"{}")])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("abc123")
    assert excinfo.value.code == "not_found"
    assert excinfo.value.uncertain is False


def test_stale_revision_stops_before_put():
    transport = _Transport([_json_response(200, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("abc123", _draft(title="Autre"),
                             expected_revision="hash-perime")
    assert excinfo.value.code == "conflict"
    assert excinfo.value.uncertain is False
    assert [c["method"] for c in transport.calls] == ["GET"]


# ---- Incertitudes apres ecriture ---------------------------------------

def test_network_error_on_create_is_uncertain():
    transport = _Transport([TimeoutError("drop")])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "network"
    assert excinfo.value.uncertain is True


def test_server_error_on_create_is_uncertain():
    transport = _Transport([TransportResponse(status=502, headers={}, body=b"{}")])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "server"
    assert excinfo.value.uncertain is True


def test_unreadable_json_on_create_is_uncertain():
    # 2xx mais corps non JSON : le POST a pu reussir cote Ghost. On remonte uncertain=True
    # pour que l'appelant verifie manuellement et ne declenche jamais une creation aveugle.
    transport = _Transport([TransportResponse(
        status=201, headers={"content-type": "application/json"}, body=b"<html></html>")])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(_draft())
    assert excinfo.value.code == "unreadable"
    assert excinfo.value.uncertain is True


def test_unreadable_json_on_put_is_uncertain():
    pre = _ghost_post()
    transport = _Transport([
        _json_response(200, pre),
        TransportResponse(status=200,
                           headers={"content-type": "application/json"},
                           body=b"not-json"),
    ])
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("abc123", _draft(title="Autre"),
                             expected_revision=_revision(pre["posts"][0]))
    assert excinfo.value.code == "unreadable"
    assert excinfo.value.uncertain is True


# ---- Divers ------------------------------------------------------------

def test_path_is_relative_not_full_url():
    transport = _Transport([_json_response(201, _ghost_post())])
    adapter = get_adapter("ghost", transport=transport, config={})
    adapter.create_draft(_draft())
    path = transport.calls[0]["path"]
    assert path.startswith("/ghost/api/admin/")
    assert "://" not in path


def test_scheduled_status_translation():
    transport = _Transport([_json_response(200, _ghost_post(status="scheduled"))])
    adapter = get_adapter("ghost", transport=transport, config={})
    article = adapter.read("abc123")
    assert article.status == "scheduled"


@pytest.mark.parametrize("body", [
    {"posts": []}, _ghost_post(id=""), _ghost_post(updated_at=""),
], ids=["empty-posts", "missing-id", "missing-date"])
@pytest.mark.parametrize("operation", ["create", "update", "publish", "read"])
def test_incomplete_success_response_keeps_write_uncertain(body, operation):
    pre = _ghost_post()
    responses = [_json_response(200, body)]
    if operation in {"update", "publish"}:
        responses.insert(0, _json_response(200, pre))
    transport = _Transport(responses)
    adapter = get_adapter("ghost", transport=transport, config={})
    with pytest.raises(AdapterError) as caught:
        if operation == "create":
            adapter.create_draft(_draft())
        elif operation == "update":
            adapter.update_draft("abc123", _draft(),
                                 expected_revision=_revision(pre["posts"][0]))
        elif operation == "publish":
            adapter.publish("abc123", expected_revision=_revision(pre["posts"][0]))
        else:
            adapter.read("abc123")
    assert caught.value.code == "unreadable"
    assert caught.value.uncertain is (operation != "read")
    assert sum(call["method"] != "GET" for call in transport.calls) == (operation != "read")
