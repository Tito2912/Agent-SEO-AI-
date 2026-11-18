"""Adaptateur PrestaShop Webservice (pages CMS) : XML echappe, active=0 par defaut."""
from __future__ import annotations

import pytest

from backend.cms_content_adapters import AdapterError, ArticleDraft, TransportResponse, get_adapter


def _draft(**overrides):
    base = {"title": "Un titre & essai", "html": "<p>Corps simple.</p>",
            "slug": "un-titre-essai", "excerpt": "Resume & synthese.",
            "language": "fr", "author": ""}
    base.update(overrides)
    return ArticleDraft(**base)


def _xml(body: str, status: int = 200) -> TransportResponse:
    return TransportResponse(status=status, headers={"content-type": "application/xml"},
                             body=body.encode("utf-8"))


def _content_xml(cms_id: str = "12", active: int = 0, date_upd: str = "2026-11-17 10:00:00") -> str:
    return (f'<?xml version="1.0" encoding="UTF-8"?>'
            f'<prestashop><content>'
            f'<id>{cms_id}</id><active>{active}</active>'
            f'<date_upd>{date_upd}</date_upd>'
            f'</content></prestashop>')


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


def test_create_draft_sends_xml_with_escaped_values_and_active_zero():
    transport = _Transport([_xml(_content_xml(), status=201)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    article = adapter.create_draft(_draft())
    assert article.status == "draft"
    assert article.revision == "2026-11-17 10:00:00"
    xml = transport.calls[0]["payload"].decode("utf-8")
    assert "<active>0</active>" in xml
    # L'esperluette du titre est echappee ; l'HTML du corps aussi (contenu dans <content>).
    assert "Un titre &amp; essai" in xml
    assert "&lt;p&gt;Corps simple.&lt;/p&gt;" in xml
    assert "<script" not in xml


def test_publish_toggles_active_true_after_revision_check():
    transport = _Transport([
        _xml(_content_xml(date_upd="old")),
        _xml(_content_xml(active=1, date_upd="new")),
    ])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    article = adapter.publish("12", expected_revision="old")
    assert article.status == "published"
    body = transport.calls[1]["payload"].decode("utf-8")
    assert "<active>1</active>" in body
    assert "<id>12</id>" in body


def test_update_draft_requires_revision_and_patches():
    transport = _Transport([
        _xml(_content_xml(date_upd="old")),
        _xml(_content_xml(date_upd="new")),
    ])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    article = adapter.update_draft("12", _draft(title="Autre"), expected_revision="old")
    assert article.revision == "new"


def test_publish_without_revision_refused():
    transport = _Transport([_xml(_content_xml())])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.publish("12")
    assert excinfo.value.code == "revision_required"


def test_bad_remote_id_refused_before_network():
    transport = _Transport([])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    for bad in ("0", "-1", "abc", "1.2"):
        with pytest.raises(AdapterError):
            adapter.read(bad)
    assert transport.calls == []


def test_oversize_html_refused_before_network():
    transport = _Transport([])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    huge = ArticleDraft(title="t", html="x" * 1_100_000, slug="s")
    with pytest.raises(AdapterError) as excinfo:
        adapter.create_draft(huge)
    assert excinfo.value.code == "validation"
    assert transport.calls == []


def test_unparseable_response_is_unreadable():
    transport = _Transport([_xml("<not-xml", status=200)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("12")
    assert excinfo.value.code == "unreadable"


def test_capabilities_do_not_declare_blog_or_media():
    adapter = get_adapter("prestashop", transport=lambda *a, **k: None,
                          config={"id_lang": "1", "id_shop": "1"})
    assert adapter.capabilities.upload_images is False
    assert "PAS de blog" in adapter.capabilities.notes


def test_doctype_response_is_refused_before_parsing():
    poisoned = (
        '<?xml version="1.0"?>'
        '<!DOCTYPE lolz [<!ENTITY lol "lol"><!ENTITY lol2 "&lol;&lol;">]>'
        '<prestashop><content><id>12</id><active>0</active>'
        '<date_upd>2026-11-17 10:00:00</date_upd></content></prestashop>'
    )
    transport = _Transport([_xml(poisoned, status=200)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("12")
    assert excinfo.value.code == "unreadable"


def test_entity_declaration_without_doctype_is_refused():
    poisoned = (
        '<?xml version="1.0"?>'
        '<!ENTITY ext SYSTEM "file:///etc/passwd">'
        '<prestashop><content><id>12</id><active>0</active>'
        '<date_upd>2026-11-17 10:00:00</date_upd></content></prestashop>'
    )
    transport = _Transport([_xml(poisoned, status=200)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("12")
    assert excinfo.value.code == "unreadable"


def test_malformed_xml_is_unreadable_not_crash():
    transport = _Transport([_xml("<prestashop><content><id>12", status=200)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.read("12")
    assert excinfo.value.code == "unreadable"


def test_update_draft_uses_patch_method():
    transport = _Transport([
        _xml(_content_xml(date_upd="old")),
        _xml(_content_xml(date_upd="new")),
    ])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    adapter.update_draft("12", _draft(title="Nouveau"), expected_revision="old")
    assert transport.calls[0]["method"] == "GET"
    assert transport.calls[1]["method"] == "PATCH"
    # L'activation ne se fait pas via update_draft : active reste 0.
    patch_body = transport.calls[1]["payload"].decode("utf-8")
    assert "<active>0</active>" in patch_body


def test_publish_uses_patch_with_only_id_and_active():
    transport = _Transport([
        _xml(_content_xml(date_upd="old")),
        _xml(_content_xml(active=1, date_upd="new")),
    ])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    adapter.publish("12", expected_revision="old")
    assert transport.calls[1]["method"] == "PATCH"
    body = transport.calls[1]["payload"].decode("utf-8")
    # L'activation PATCH envoie uniquement id + active, pas le contenu des champs texte.
    assert "<id>12</id>" in body
    assert "<active>1</active>" in body
    assert "<meta_title>" not in body
    assert "<content>" in body  # l'enveloppe <content> reste presente


def test_conflict_stops_before_write():
    transport = _Transport([_xml(_content_xml(date_upd="current"))])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("12", _draft(title="Nouveau"), expected_revision="stale")
    assert excinfo.value.code == "conflict"
    # Une seule requete (le read) : l'ecriture n'a pas ete envoyee.
    assert len(transport.calls) == 1
    assert transport.calls[0]["method"] == "GET"


def test_create_uses_post_and_active_zero():
    transport = _Transport([_xml(_content_xml(), status=201)])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    adapter.create_draft(_draft())
    assert transport.calls[0]["method"] == "POST"
    assert "id_shop=1" in transport.calls[0]["path"]
    body = transport.calls[0]["payload"].decode("utf-8")
    assert body.startswith("<?xml")
    assert '<language id="1">' in body
    # Pas d'identifiant inclus dans le POST de creation : le CMS attribue l'id.
    assert "<id>" not in body


def test_id_shop_scopes_every_path():
    transport = _Transport([
        _xml(_content_xml(date_upd="old")),
        _xml(_content_xml(date_upd="new")),
    ])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "2", "id_shop": "3"})
    adapter.update_draft("12", _draft(title="Autre"), expected_revision="old")
    for call in transport.calls:
        assert "id_shop=3" in call["path"]
    patch_body = transport.calls[1]["payload"].decode("utf-8")
    assert '<language id="2">' in patch_body


def test_server_error_on_write_is_uncertain():
    transport = _Transport([
        _xml(_content_xml(date_upd="old")),
        _xml("<prestashop></prestashop>", status=503),
    ])
    adapter = get_adapter("prestashop", transport=transport,
                          config={"id_lang": "1", "id_shop": "1"})
    with pytest.raises(AdapterError) as excinfo:
        adapter.update_draft("12", _draft(title="Autre"), expected_revision="old")
    assert excinfo.value.code == "server"
    assert excinfo.value.uncertain is True
