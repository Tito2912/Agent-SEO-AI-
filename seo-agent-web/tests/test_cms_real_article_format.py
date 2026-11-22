"""The actual editor rendering must validate before any image reaches a CMS."""
from types import SimpleNamespace
import io
import os
import tempfile

os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="noyaru-html-data-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="noyaru-html-runs-"))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")

from bs4 import BeautifulSoup
from PIL import Image
import pytest

from backend import app as m, cms_publication_routes as routes, cms_publication as workflow
from backend.cms_content_adapters import get_adapter, AdapterError


@pytest.fixture()
def rendered():
    output = io.BytesIO()
    Image.new("RGB", (32, 24), "#88aa88").save(output, "WEBP")
    note = {"versions": [{"code": "fr", "langue": "Français", "titre": "Réparer son vélo | Atelier", "h1": "Réparer son vélo",
        "slug": "reparer-velo", "markdown": "![Décor](images/velo.webp)\n\n## Choisir une chaîne\n\nUn contrôle régulier.\n\n![La chaîne](images/velo.webp)\n\n| Pièce | Action |\n|:---|---:|\n| Chaîne | Nettoyer |\n"}],
        "images": {"fichiers": [{"nom": "velo.webp", "largeur": 32, "hauteur": 24}]}}
    fake = SimpleNamespace(_note_d_article=lambda *a: note, _versions_servies=m._versions_servies,
                           _dimensions=m._dimensions, _image_d_article=lambda *a: output.getvalue())
    return fake, note


def adapter(platform):
    config = {"webflow": {"collection_id": "collection-1", "field_mapping": {"title": "name", "slug": "slug", "body": "body"}},
              "prestashop": {"id_lang": "1", "id_shop": "1"}, "shopify": {"blog_id": "gid://shopify/Blog/1"}}.get(platform, {})
    return get_adapter(platform, config=config, transport=lambda *a, **kw: pytest.fail("Validation must never call the CMS"))


@pytest.mark.parametrize("platform", ["wordpress", "ghost", "prestashop", "webflow", "shopify"])
def test_real_editor_headings_image_and_aligned_table_are_publishable_without_content_loss(rendered, platform):
    fake, note = rendered
    before = m._versions_servies(note["versions"], m._dimensions(note["images"]))[0]["html"]
    assert 'loading="lazy"' in before and 'decoding="async"' in before and '<h2 id=' in before
    uploads = []
    def upload(name, data):
        uploads.append(name)
        return "https://atelier.example/media/" + name
    target = adapter(platform)
    draft = routes.source_draft(fake, None, "atelier", "task", "fr", "1", resolve_image=upload,
                                validate=target.validate_draft, platform=platform)
    soup = BeautifulSoup(draft.html, "html.parser")
    assert draft.title == "Réparer son vélo" and not soup.find("h1")
    assert soup.find("h2")["id"] and soup.find("img", alt="La chaîne")["alt"] == "La chaîne"
    assert soup.find("th")["align"] == "left" and soup.find("td", align="right").text == "Nettoyer"
    assert not soup.select('[style], [loading], [decoding]')
    assert uploads == ["velo.webp", "velo.webp"] and "media-placeholder" not in draft.html
    target.validate_draft(draft)


def test_wix_refuses_the_real_table_before_uploading_any_media(rendered):
    fake, _note = rendered
    with pytest.raises(AdapterError):
        routes.source_draft(fake, None, "atelier", "task", "fr", "member-1",
            resolve_image=lambda *a: pytest.fail("Unsupported article must not upload"),
            validate=adapter("wix").validate_draft, platform="wix")


def test_wix_accepts_a_real_heading_without_a_table_or_anchor_link(rendered):
    fake, note = rendered
    note["versions"][0]["markdown"] = "## Choisir une chaîne\n\nTexte simple."
    draft = routes.source_draft(fake, None, "atelier", "task", "fr", "member-1", validate=adapter("wix").validate_draft, platform="wix")
    assert "Choisir une chaîne" in draft.html and "<h1" not in draft.html


def test_invalid_second_image_is_refused_before_the_first_upload(rendered):
    fake, note = rendered
    note["versions"][0]["markdown"] += "\n![Autre](images/cassee.webp)"
    note["images"]["fichiers"].append({"nom": "cassee.webp", "largeur": 32, "hauteur": 24})
    image = fake._image_d_article
    fake._image_d_article = lambda *a: b"not-an-image" if a[-1] == "cassee.webp" else image(*a)
    from backend.cms_commun import CmsError
    with pytest.raises(CmsError):
        routes.source_draft(fake, None, "atelier", "task", "fr", "", resolve_image=lambda *a: pytest.fail("No partial upload"))


def test_unsafe_attributes_are_still_refused_instead_of_silently_removed(rendered):
    fake, _note = rendered
    fake._versions_servies = lambda *a: [{"code": "fr", "titre": "Sujet", "slug": "sujet", "html": '<h1>Sujet</h1><p onclick="alert(1)">Texte</p>'}]
    with pytest.raises(workflow.PublicationError):
        routes.source_draft(fake, None, "atelier", "task", "fr", "", resolve_image=lambda *a: pytest.fail("No upload"))
