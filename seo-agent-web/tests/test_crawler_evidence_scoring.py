"""End-to-end check that the scorer EMITS the evidence the corrector expects.

test_crawler_evidence.py covers the writer in isolation; this file feeds real PageData
through _score_issues, which is where a wrong assumption about a field actually bites. It
exists because `PageData.h1` is a list of every h1 on the page, not a string: reading it as
one raised AttributeError inside _score_issues and took the WHOLE crawl down for any site
with a duplicated h1 — a unit test on the writer could never have caught that.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_scoring_tests", REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_scoring_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

BASE = "https://site.test"


def _page(url: str, **kw: Any) -> Any:
    """A healthy 200 HTML page, overridden field by field for the defect under test."""
    defaults: dict[str, Any] = {
        "url": url,
        "final_url": url,
        "status_code": 200,
        "content_type": "text/html; charset=utf-8",
        "title": "Un titre de page correct",
        "meta_description": "Une meta description de longueur raisonnable pour ne pas declencher les regles de longueur.",
        "canonical": url,
        "lang": "fr",
        "h1": ["Titre principal"],
        "h1_tag_count": 1,
        "title_tag_count": 1,
    }
    defaults.update(kw)
    return seo_audit.PageData(**defaults)


def _evidence(issues: dict[str, Any], key: str) -> dict[str, Any] | None:
    block = issues.get(key)
    return block.get("evidence") if isinstance(block, dict) else None


def test_duplicate_h1_lists_every_heading_without_crashing_the_crawl() -> None:
    pages = [
        _page(f"{BASE}/", h1=["Premier titre", "Second titre en trop"], h1_tag_count=2),
        _page(f"{BASE}/ok"),
    ]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    assert issues["multiple_h1"]["count"] == 1
    ev = _evidence(issues, "multiple_h1")
    assert ev is not None and ev["kind"] == "page_values"
    assert ev["items"] == [
        {"page": f"{BASE}/", "field": "2 balises <h1>", "value": "Premier titre | Second titre en trop"}
    ]


def test_empty_h1_tags_still_produce_usable_evidence() -> None:
    pages = [_page(f"{BASE}/", h1=["", "  "], h1_tag_count=2)]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    assert _evidence(issues, "multiple_h1")["items"][0]["value"] == "h1 vides"


def test_canonical_pointing_at_a_redirect_yields_the_final_url() -> None:
    pages = [
        _page(f"{BASE}/", canonical=f"{BASE}/home-old"),
        # The canonical target: a 301 that lands on /home.
        _page(f"{BASE}/home-old", final_url=f"{BASE}/home", redirect_statuses=[301], canonical=f"{BASE}/home"),
        _page(f"{BASE}/home"),
    ]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    ev = _evidence(issues, "canonical_points_to_redirect")
    assert ev is not None and ev["kind"] == "url_pairs"
    assert ev["items"] == [{"page": f"{BASE}/", "from": f"{BASE}/home-old", "to": f"{BASE}/home"}]


def test_canonical_pointing_at_a_redirect_to_a_404_yields_no_pair() -> None:
    pages = [
        _page(f"{BASE}/", canonical=f"{BASE}/gone-old"),
        _page(f"{BASE}/gone-old", final_url=f"{BASE}/gone", redirect_statuses=[301], status_code=404),
    ]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    # The issue still fires, but repointing the canonical onto a 404 would be worse than
    # leaving it — so the corrector gets nothing to rewrite.
    assert issues["canonical_points_to_redirect"]["count"] == 1
    assert _evidence(issues, "canonical_points_to_redirect") is None


def test_incomplete_open_graph_names_only_the_absent_tags() -> None:
    pages = [_page(f"{BASE}/", og_title="Present", og_description="Aussi present")]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    ev = _evidence(issues, "open_graph_tags_incomplete")
    assert ev is not None
    # og:title and og:description are present and must not be listed as missing.
    assert ev["items"][0]["value"] == "og:image, og:url, og:type"


def test_invalid_html_lang_reports_the_offending_value() -> None:
    pages = [_page(f"{BASE}/", lang="en_US")]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    ev = _evidence(issues, "html_lang_attribute_invalid")
    assert ev is not None
    assert ev["items"] == [{"page": f"{BASE}/", "field": "lang invalide", "value": "en_US"}]


def test_hreflang_to_a_non_canonical_target_yields_the_real_canonical() -> None:
    pages = [
        _page(
            f"{BASE}/es-source", lang="es",
            hreflang={"es": f"{BASE}/es-dup", "en": f"{BASE}/", "x-default": f"{BASE}/"},
        ),
        # The hreflang target declares a different canonical: that is where it should point.
        _page(f"{BASE}/es-dup", lang="es", canonical=f"{BASE}/es"),
        _page(f"{BASE}/es", lang="es"),
        _page(f"{BASE}/", hreflang={"en": f"{BASE}/", "es": f"{BASE}/es", "x-default": f"{BASE}/"}),
    ]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    ev = _evidence(issues, "hreflang_to_non_canonical")
    assert ev is not None and ev["kind"] == "url_pairs"
    assert {"page": f"{BASE}/es-source", "from": f"{BASE}/es-dup", "to": f"{BASE}/es"} in ev["items"]


def test_invalid_hreflang_annotations_are_named_one_by_one() -> None:
    pages = [
        _page(f"{BASE}/", hreflang={"en_US": f"{BASE}/en", "fr": "", "es": f"{BASE}/es", "x-default": f"{BASE}/"}),
        _page(f"{BASE}/es", lang="es"),
    ]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    ev = _evidence(issues, "hreflang_annotation_invalid")
    assert ev is not None
    value = ev["items"][0]["value"]
    assert "en_US" in value and "fr" in value
    # The valid annotation is not reported as broken.
    assert "«es»" not in value


def test_redirect_3xx_separates_deliberate_canonicalisation_from_a_self_loop() -> None:
    pages = [
        _page(f"{BASE}/"),
        # The site's own http->https canonicalisation: deliberate, must never be "fixed".
        _page("http://site.test/", final_url=f"{BASE}/", redirect_statuses=[301]),
        # A URL that redirects to ITSELF: a genuine config bug the corrector can repair.
        _page(f"{BASE}/loops", final_url=f"{BASE}/loops", redirect_statuses=[301, 301],
              redirect_chain=[f"{BASE}/loops"], error="TooManyRedirects: exceeded 30 redirects"),
    ]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    ev = _evidence(issues, "redirect_3xx")
    assert ev is not None and ev["kind"] == "page_values"
    by_page = {i["page"]: i["field"] for i in ev["items"]}
    assert by_page["http://site.test/"] == "canonicalisation attendue"
    assert by_page[f"{BASE}/loops"] == "boucle: redirige vers elle-meme"


def test_a_clean_site_produces_no_evidence_at_all() -> None:
    pages = [_page(f"{BASE}/"), _page(f"{BASE}/a"), _page(f"{BASE}/b")]

    issues = seo_audit._score_issues(pages, base_url=BASE)

    assert not [k for k, v in issues.items() if isinstance(v, dict) and "evidence" in v]
