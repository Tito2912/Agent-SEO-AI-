from __future__ import annotations

import copy
from types import SimpleNamespace

import pytest

from backend import app as m
from ops.gauntlet import hreflang_cycle as bench

PREVIEW = "https://deploy-preview-1--fixture.netlify.app/"
PROD = "https://fixture.netlify.app/"


def test_projection_is_explicit_immutable_and_preserves_real_noindex():
    raw = {"pages": [
        {"url": PREVIEW + "A/?Q=1", "x_robots_tag": "noindex", "meta_robots": "noindex, follow",
         "hreflang": {"fr": PROD + "A/?Q=1", "en": PREVIEW + "en/"}},
        {"url": PREVIEW + "b", "x_robots_tag": "noindex, nofollow"},
        {"url": "https://external.test/", "x_robots_tag": "noindex"}],
        "meta": {"base_url": PREVIEW}, "canonical_host_alias": "fixture.netlify.app"}
    original = copy.deepcopy(raw)
    output, removed = bench.project(raw, PREVIEW, PROD)
    assert raw == original
    assert output["pages"][0]["url"] == PROD + "A/?Q=1"
    assert output["pages"][0]["hreflang"] == {"fr": PROD + "A/?Q=1", "en": PROD + "en/"}
    assert output["pages"][0]["meta_robots"] == "noindex, follow"
    assert output["pages"][1]["x_robots_tag"] == "noindex, nofollow"
    assert output["pages"][2]["x_robots_tag"] == "noindex"
    assert removed == [PREVIEW + "A/?Q=1"]
    assert "counterfactual" in output["validation_mode"]


def test_redirect_to_another_host_keeps_its_real_noindex_header():
    output, removed = bench.project({"pages": [{"url": PREVIEW + "redirect",
                                               "final_url": PROD + "destination",
                                               "x_robots_tag": "noindex"}]}, PREVIEW, PROD)
    assert output["pages"][0]["x_robots_tag"] == "noindex"
    assert removed == []


@pytest.mark.parametrize("preview,production", [(PROD, PROD), ("http://preview.test/", PROD),
                                               (PREVIEW, "http://fixture.test/")])
def test_bad_projection_origins_are_rejected(preview, production):
    with pytest.raises(ValueError):
        bench.project({}, preview, production)


@pytest.mark.parametrize("stack", ["hugo", "nuxt"])
def test_seed_page_is_a_genuine_mechanical_positive_control(stack):
    source = next(v for k, v in bench.seed_pages(stack).items() if "-en." in k)
    site = f"https://noyaru-stack-{stack}.netlify.app"
    item = {"page": site + "/gauntlet/qa-reciprocal-en/", "field": "fr",
            "value": site + "/gauntlet/qa-reciprocal-fr/"}
    patched, count = m._add_reciprocal_hreflang(source, [item])
    assert count == 1
    assert m._add_reciprocal_hreflang(patched, [item]) == (patched, 0)
    assert m._unbalanced_delimiters(patched) == ""


def test_rescore_uses_observed_pages_and_the_real_scorer(tmp_path):
    pages = []
    urls = {c: PROD + c + "/" for c in ("fr", "en", "de")}
    for code, peers in {"fr": ("fr", "en"), "en": ("en", "de"), "de": ("de", "en")}.items():
        pages.append({"url": PREVIEW + code + "/", "final_url": PREVIEW + code + "/",
                      "status_code": 200, "content_type": "text/html", "x_robots_tag": "noindex",
                      "canonical": urls[code], "lang": code,
                      "hreflang": {p: urls[p] for p in peers}})
    path = tmp_path / "raw.json"
    bench.live.save(path, {"meta": {"base_url": PREVIEW}, "pages": pages})
    sitemap = (f'<urlset xmlns="{bench.NS}">' + "".join(f"<url><loc>{u}</loc></url>" for u in urls.values())
               + "</urlset>").encode()
    before = bench.rescore(path, PREVIEW, PROD, sitemap, tmp_path / "before")
    assert before["issues"][bench.KEY]["count"] == 1
    assert before["issues"][bench.KEY]["evidence"]["items"] == [
        {"page": urls["en"], "field": "fr", "value": urls["fr"]}]
    pages[1]["hreflang"]["fr"] = urls["fr"]
    bench.live.save(path, {"meta": {"base_url": PREVIEW}, "pages": pages})
    after = bench.rescore(path, PREVIEW, PROD, sitemap, tmp_path / "after")
    assert after["issues"][bench.KEY]["count"] == 0
    assert len(after["pages"]) == 3


def test_a_sitemap_index_cannot_silently_disable_the_positive_control(tmp_path):
    with pytest.raises(ValueError, match="URL set"):
        bench.rescore(tmp_path / "missing.json", PREVIEW, PROD,
                      f'<sitemapindex xmlns="{bench.NS}"/>'.encode(), tmp_path / "out")


def test_nonfixture_stack_is_rejected_before_network_or_disk(tmp_path):
    with pytest.raises(ValueError, match="Hugo/Nuxt"):
        bench.cycle(None, "unused", "customer", tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_changed_seed_is_refused_before_writes_and_saved(tmp_path, monkeypatch):
    module = SimpleNamespace(_github_api_get=lambda *a, **kw: {"object": {"sha": "changed"}},
                             _github_ref_api_path=lambda *a: "/".join(a))
    monkeypatch.setattr(bench.live, "close_prs", lambda *a: [])
    with pytest.raises(ValueError, match="seed head"):
        bench.cycle(module, "unused", "hugo", tmp_path)
    result = bench.live.load(tmp_path / "hugo/cycle.json")
    assert result["status"] == "failed" and result["error_type"] == "ValueError"
    assert result["pull_requests"] == []


@pytest.mark.parametrize("stack", ["hugo", "nuxt"])
def test_navigation_exposes_all_controls_as_preview_relative_links(stack):
    source = "Fixture index\n" if stack == "hugo" else '<ul><li><a href="/existing/">Existing</a></li></ul>'
    output = bench.seed_navigation(stack, source)
    for code in ("fr", "en", "de"):
        assert f"/gauntlet/qa-reciprocal-{code}/" in output
    assert "https://" not in output
    assert source.split("</ul>")[0] in output


@pytest.mark.parametrize("source", ["<main>Different index</main>", "<ul></ul><ul></ul>"])
def test_ambiguous_fixture_navigation_is_refused(source):
    with pytest.raises(ValueError, match="safely extended"):
        bench.seed_navigation("nuxt", source)
