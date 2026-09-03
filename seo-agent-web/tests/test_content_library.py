"""The content collections, checked the way a template would consume them.

The previous incarnation of this content was a 600-line Python literal with no test at all: a
missing key or a bad slug was found in production, by a visitor. These assertions are cheap and
they are the whole reason the content now lives in files.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import content_library as cl  # noqa: E402


def test_every_page_parses() -> None:
    """A page skipped at load is invisible in production but silent. Not here."""
    cl.reload()
    assert cl.LOAD_ERRORS == []


def test_both_collections_are_populated() -> None:
    assert len(cl.docs_pages()) >= 10
    assert len(cl.blog_pages()) >= 6


def test_slugs_are_unique_across_collections() -> None:
    # Not strictly required — the two live under different prefixes — but a shared slug means
    # two pages compete for the same name in `related:`, which resolves to whichever loads first.
    slugs = [p["slug"] for p in cl.all_pages()]
    assert len(slugs) == len(set(slugs))


@pytest.mark.parametrize("field", ["title", "description", "updated_at", "reading_time", "kind"])
def test_required_fields_are_never_empty(field: str) -> None:
    for page in cl.all_pages():
        assert page[field], f"{page['source']} has an empty {field}"


def test_descriptions_fit_a_meta_description() -> None:
    """These strings are rendered into <meta name="description">, so length is functional."""
    for page in cl.all_pages():
        length = len(page["description"])
        assert 60 <= length <= 200, f"{page['source']}: description is {length} characters"


def test_related_slugs_all_resolve() -> None:
    known = {p["slug"] for p in cl.all_pages()}
    for page in cl.all_pages():
        unknown = [slug for slug in page["related"] if slug not in known]
        assert not unknown, f"{page['source']} links to missing pages: {unknown}"


def test_internal_links_in_bodies_resolve() -> None:
    """A /docs/... or /ressources-seo/... href in prose must point at a page that exists."""
    import re

    known_paths = {p["path"] for p in cl.all_pages()}
    pattern = re.compile(r'href="(/(?:docs|ressources-seo)/[a-z0-9-]+)"')
    for page in cl.all_pages():
        for href in pattern.findall(page["body_html"]):
            assert href in known_paths, f"{page['source']} links to {href}, which does not exist"


def test_docs_are_ordered_and_sectioned() -> None:
    sections = cl.docs_sections()
    assert sections, "docs must be grouped into at least one section"
    for section in sections:
        orders = [p["order"] for p in section["pages"]]
        assert orders == sorted(orders), f"section {section['name']} is out of order"


def test_blog_is_newest_first() -> None:
    dates = [p["published_at"] for p in cl.blog_pages()]
    assert dates == sorted(dates, reverse=True)


def test_featured_articles_fill_up_to_the_limit() -> None:
    picked = cl.featured_articles(3)
    assert len(picked) == 3
    assert len({p["slug"] for p in picked}) == 3


def test_headings_carry_anchors() -> None:
    """The on-page table of contents links to #ids the body must actually contain."""
    for page in cl.all_pages():
        for heading in page["headings"]:
            assert heading["id"]
            assert f'id="{heading["id"]}"' in page["body_html"]


def test_tokens_are_substituted_everywhere_they_appear() -> None:
    tokens = {"app_name": "Noyaru", "support_email": "hello@example.com"}
    for page in cl.all_pages():
        resolved = cl.resolve(page, tokens)
        for field in ("title", "meta_title", "description", "cta"):
            assert "{{app_name}}" not in resolved[field]
        assert "{{app_name}}" not in resolved["body_html"]
        for item in resolved["faq"]:
            assert "{{app_name}}" not in item["question"]
            assert "{{app_name}}" not in item["answer"]


def test_unknown_token_is_left_visible() -> None:
    """Blanking an unknown token would hide the bug; leaving it makes someone fix the page."""
    assert cl.substitute("reste {{inconnu}} ici", {"app_name": "X"}) == "reste {{inconnu}} ici"


def test_token_values_are_escaped() -> None:
    out = cl.substitute("<p>{{app_name}}</p>", {"app_name": "<script>x</script>"})
    assert "<script>" not in out
    assert "&lt;script&gt;" in out


def test_resolve_does_not_mutate_the_cache() -> None:
    page = cl.docs_pages()[0]
    before = page["title"]
    cl.resolve(page, {"app_name": "Something Else"})
    assert cl.docs_pages()[0]["title"] == before


def test_tables_are_wrapped_for_small_screens() -> None:
    """Every table gets its own scroller, or a wide one makes the whole page scroll sideways."""
    for page in cl.all_pages():
        body = page["body_html"]
        opens = body.count("<table>")
        assert body.count('<div class="content-table-wrap"><table>') == opens
        assert body.count("</table></div>") == body.count("</table>")


def test_sitemap_entries_cover_every_page() -> None:
    entries = list(cl.sitemap_entries())
    assert len(entries) == len(cl.all_pages())
    for entry in entries:
        assert entry["path"].startswith("/")
        assert len(entry["lastmod"]) == 10


def test_the_six_original_blog_urls_still_exist() -> None:
    """These slugs were live and are in the sitemap. Losing one is a 404 for Google."""
    for slug in (
        "audit-seo-technique-checklist-priorites",
        "connecter-google-search-console-audit-mensuel",
        "corriger-title-meta-description-grande-echelle",
        "frequence-crawl-seo-site-vitrine-ecommerce-blog",
        "netlinking-opportunites-backlinks-sans-spam",
        "core-web-vitals-lire-signaux-seo",
    ):
        assert cl.get_article(slug) is not None, f"{slug} disappeared"


def test_unknown_slug_returns_none() -> None:
    assert cl.get_doc("il-ny-a-pas-de-page-ici") is None
    assert cl.get_article("il-ny-a-pas-de-page-ici") is None
