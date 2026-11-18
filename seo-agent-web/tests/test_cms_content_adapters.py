"""Contrat partage des adaptateurs editoriaux : types, fabrique, assainissement HTML, Ricos.

Aucun test ne touche a un CMS reel. Les transports sont des callables en memoire ; les
donnees sont fictives (slug, titre, auteur). Chaque test valide un invariant du contrat :
fabrique, validation d'entree, interdiction d'ecriture ambigüe.
"""
from __future__ import annotations

import dataclasses

import pytest

from backend.cms_content_adapters import (
    AdapterError,
    ArticleDraft,
    ArticleImage,
    Capabilities,
    PLATFORMS,
    RemoteArticle,
    TransportResponse,
    get_adapter,
)
from backend.cms_content_adapters.base import (
    check_revision,
    require_author,
    require_config,
    require_draft_fields,
    wrap_transport_errors,
)
from backend.cms_content_adapters.html_safe import DEFAULT_TAGS, validate_html
from backend.cms_content_adapters.ricos import html_to_ricos


# ---- Fabrique et catalogue --------------------------------------------------

def test_platforms_catalog_contains_expected_names():
    assert set(PLATFORMS) == {"wordpress", "ghost", "shopify", "webflow", "wix", "prestashop"}


def test_get_adapter_refuses_unknown_platform():
    with pytest.raises(AdapterError) as excinfo:
        get_adapter("blogger", transport=lambda *a, **k: None, config={})
    assert excinfo.value.code == "unsupported"


@pytest.mark.parametrize("platform,config", [
    ("wordpress", {}),
    ("ghost", {}),
    ("wix", {}),
    ("shopify", {"blog_id": "gid://shopify/Blog/1"}),
    ("webflow", {"collection_id": "abc", "field_mapping": {"title": "t", "slug": "s", "body": "b"}}),
    ("prestashop", {"id_lang": "1", "id_shop": "1"}),
], ids=["wp", "ghost", "wix", "shopify", "webflow", "prestashop"])
def test_adapters_instantiate_without_network(platform, config):
    """Pas d'appel reseau au chargement : le transport n'est jamais appele."""
    called = []
    def transport(*a, **k):
        called.append(True)
        raise AssertionError("transport ne doit pas etre appele a l'instanciation")
    adapter = get_adapter(platform, transport=transport, config=config)
    assert adapter.platform == platform
    assert called == []
    caps = adapter.capabilities
    assert isinstance(caps, Capabilities)
    # Une capacite declaree True doit correspondre a une methode exposee.
    for name in ("create_draft", "read", "publish", "update_draft"):
        assert hasattr(adapter, name)


def test_shopify_init_rejects_bad_blog_id():
    with pytest.raises(AdapterError) as excinfo:
        get_adapter("shopify", transport=lambda *a, **k: None, config={"blog_id": "1"})
    assert excinfo.value.code == "mapping"


def test_webflow_init_rejects_missing_field_mapping():
    with pytest.raises(AdapterError) as excinfo:
        get_adapter("webflow", transport=lambda *a, **k: None,
                    config={"collection_id": "abc", "field_mapping": {"title": "t"}})
    assert excinfo.value.code == "mapping"


def test_prestashop_init_rejects_bad_id_lang():
    with pytest.raises(AdapterError) as excinfo:
        get_adapter("prestashop", transport=lambda *a, **k: None,
                    config={"id_lang": "abc", "id_shop": "1"})
    assert excinfo.value.code == "mapping"


# ---- Dataclasses frozen -----------------------------------------------------

def test_article_draft_is_frozen():
    draft = ArticleDraft(title="t", html="<p>x</p>", slug="s")
    with pytest.raises(dataclasses.FrozenInstanceError):
        draft.title = "autre"  # type: ignore[misc]


def test_remote_article_is_frozen():
    article = RemoteArticle(id="1", status="draft", url="", revision="rev")
    with pytest.raises(dataclasses.FrozenInstanceError):
        article.status = "published"  # type: ignore[misc]


def test_article_image_defaults():
    image = ArticleImage(url="https://cdn.example/a.jpg")
    assert image.alt == "" and image.caption == ""


# ---- Validation du brouillon et des identifiants ---------------------------

@pytest.mark.parametrize("field,value", [
    ("title", ""),
    ("slug", ""),
    ("title", "x" * 401),
    ("slug", "x" * 201),
], ids=["title-vide", "slug-vide", "title-long", "slug-long"])
def test_require_draft_fields_rejects_bad_input(field, value):
    base = {"title": "t", "html": "<p>x</p>", "slug": "s"}
    base[field] = value
    with pytest.raises(AdapterError) as excinfo:
        require_draft_fields(ArticleDraft(**base))
    assert excinfo.value.code == "validation"


def test_require_author_refuses_empty():
    with pytest.raises(AdapterError) as excinfo:
        require_author(ArticleDraft(title="t", html="<p>x</p>", slug="s"), "Test")
    assert excinfo.value.code == "author"


def test_require_config_lists_missing_fields():
    with pytest.raises(AdapterError) as excinfo:
        require_config({"a": " "}, ("a", "b"))
    assert excinfo.value.code == "mapping"
    assert set(excinfo.value.details["missing"]) == {"a", "b"}


# ---- Revision obligatoire ---------------------------------------------------

def test_check_revision_requires_expected():
    with pytest.raises(AdapterError) as excinfo:
        check_revision(None, "current")
    assert excinfo.value.code == "revision_required"


def test_check_revision_refuses_mismatch():
    with pytest.raises(AdapterError) as excinfo:
        check_revision("old", "current")
    assert excinfo.value.code == "conflict"
    assert excinfo.value.details == {"expected": "old", "current": "current"}


def test_check_revision_accepts_match():
    check_revision("same", "same")  # ne doit rien lever


# ---- wrap_transport_errors --------------------------------------------------

def test_wrap_transport_errors_marks_writes_as_uncertain():
    def boom():
        raise RuntimeError("socket dropped")
    with pytest.raises(AdapterError) as excinfo:
        wrap_transport_errors(boom, write=True, cms="X")
    assert excinfo.value.code == "network"
    assert excinfo.value.uncertain is True


def test_wrap_transport_errors_passes_adapter_errors_unchanged():
    def inner():
        raise AdapterError("deja structuree", code="refused")
    with pytest.raises(AdapterError) as excinfo:
        wrap_transport_errors(inner, write=False, cms="X")
    assert excinfo.value.code == "refused"


def test_wrap_transport_errors_preserves_uncertain_from_cmserror():
    """L'incertitude du transport (CmsError-like) ne doit pas etre perdue en remontant."""
    class _CmsErrorLike(Exception):
        def __init__(self, message, uncertain, code):
            super().__init__(message)
            self.uncertain = uncertain
            self.code = code

    def inner():
        raise _CmsErrorLike("redirection avec donnees ecrites", uncertain=True,
                             code="redirection")
    with pytest.raises(AdapterError) as excinfo:
        wrap_transport_errors(inner, write=False, cms="X")
    assert excinfo.value.uncertain is True
    assert excinfo.value.code == "redirection"


def test_wrap_transport_errors_write_forces_uncertain_when_transport_silent():
    """Un transport sans attribut uncertain : une ecriture reste uncertain=True."""
    def inner():
        raise RuntimeError("boom")
    with pytest.raises(AdapterError) as excinfo:
        wrap_transport_errors(inner, write=True, cms="X")
    assert excinfo.value.uncertain is True


# ---- Assainissement HTML ----------------------------------------------------

def test_validate_html_accepts_editorial_subset():
    validate_html("<h2>Titre</h2><p>Un <strong>texte</strong> avec "
                  "<a href=\"https://exemple.fr\">lien</a>.</p>")


@pytest.mark.parametrize("html,reason", [
    ("<script>bad</script>", "script"),
    ("<iframe src='https://x'></iframe>", "iframe"),
    ("<p onclick='x'>x</p>", "onclick"),
    ("<a href='javascript:alert(1)'>x</a>", "javascript"),
    ("<img src='data:image/png;base64,AAA'>", "data-url"),
], ids=["script", "iframe", "onclick", "js-href", "data-url"])
def test_validate_html_rejects_dangerous_content(html, reason):
    with pytest.raises(AdapterError) as excinfo:
        validate_html(html)
    assert excinfo.value.code == "validation"


def test_default_tags_do_not_include_script_or_iframe():
    assert "script" not in DEFAULT_TAGS
    assert "iframe" not in DEFAULT_TAGS


# ---- Conversion Ricos (Wix) -------------------------------------------------

def test_html_to_ricos_supports_paragraph_and_decorations():
    doc = html_to_ricos("<p>Un <strong>mot</strong> <em>clef</em>.</p>")
    paragraph = doc["nodes"][0]
    assert paragraph["type"] == "PARAGRAPH"
    children = paragraph["nodes"]
    assert all(child["type"] == "TEXT" for child in children)
    joined = "".join(child["textData"]["text"] for child in children)
    assert joined == "Un mot clef."
    decorations = [child["textData"]["decorations"] for child in children]
    assert any({"type": "BOLD"} in decos for decos in decorations)
    assert any({"type": "ITALIC"} in decos for decos in decorations)


def test_html_to_ricos_supports_link_with_safe_url():
    doc = html_to_ricos('<p><a href="https://exemple.fr">texte</a></p>')
    text_node = doc["nodes"][0]["nodes"][0]
    link = [d for d in text_node["textData"]["decorations"] if d["type"] == "LINK"]
    assert link and link[0]["linkData"]["link"]["url"] == "https://exemple.fr"


def test_html_to_ricos_supports_list():
    doc = html_to_ricos("<ul><li>Un</li><li>Deux</li></ul>")
    assert doc["nodes"][0]["type"] == "BULLETED_LIST"
    assert [n["type"] for n in doc["nodes"][0]["nodes"]] == ["LIST_ITEM", "LIST_ITEM"]


def test_html_to_ricos_supports_heading_levels():
    doc = html_to_ricos("<h3>Sous titre</h3>")
    assert doc["nodes"][0]["type"] == "HEADING"
    assert doc["nodes"][0]["headingData"]["level"] == 3


@pytest.mark.parametrize("html", [
    "<table><tr><td>x</td></tr></table>",
    "<p>x <script>y</script></p>",
    '<p><a href="javascript:x">x</a></p>',
    "<p><img src='data:image/png;base64,A'></p>",
    "<p>x<img src='https://exemple.fr/i.jpg'></p>",
], ids=["table", "script", "js-link", "data-img", "inline-img"])
def test_html_to_ricos_refuses_unsupported(html):
    with pytest.raises(AdapterError) as excinfo:
        html_to_ricos(html)
    assert excinfo.value.code == "validation"
    assert "refused" in excinfo.value.details


# ---- TransportResponse helper ----------------------------------------------

def test_transport_response_json_parses_ok():
    response = TransportResponse(status=200, headers={}, body=b'{"a": 1}')
    assert response.json() == {"a": 1}


def test_transport_response_json_fails_on_non_json():
    response = TransportResponse(status=200, headers={}, body=b"<html>")
    with pytest.raises(AdapterError) as excinfo:
        response.json()
    assert excinfo.value.code == "unreadable"


def test_transport_response_refuses_oversize():
    from backend.cms_content_adapters.base import MAX_BODY
    huge = b"x" * (MAX_BODY + 1)
    response = TransportResponse(status=200, headers={}, body=huge)
    with pytest.raises(AdapterError) as excinfo:
        response.json()
    assert excinfo.value.code == "unreadable"
