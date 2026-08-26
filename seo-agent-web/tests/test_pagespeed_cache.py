"""Reusing a PageSpeed measurement instead of re-buying it.

PageSpeed is the scarcest resource in the product and the only one that is not ours: the API
allows 25 000 queries/day per key, ONE key is shared by every customer, and 50 URLs per crawl
caps the whole platform at ~500 crawls/day. Nothing was cached, so a customer crawling weekly
re-measured the same 50 unchanged URLs every single time.

The cache is keyed by a content signature rather than by age alone, so these tests are mostly
about the one thing that must never happen: serving a stale measurement for a page that has
actually changed.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_for_pagespeed_tests",
    REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py",
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_for_pagespeed_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)


def _page(**over):
    base = dict(
        url="https://x.test/p",
        final_url="https://x.test/p",
        status_code=200,
        content_type="text/html",
        title="Un titre de page",
        meta_description="Une description de page de longueur raisonnable pour le test.",
        text_word_count=850,
        images_total=12,
        script_urls=["https://x.test/app.abc123.js"],
        css_urls=["https://x.test/site.def456.css"],
    )
    base.update(over)
    return seo_audit.PageData(**base)


def _sig(**over) -> str:
    return seo_audit._pagespeed_signature([_page(**over)], "mobile")


# --- what invalidates a measurement --------------------------------------------------------

def test_an_unchanged_page_keeps_its_signature() -> None:
    assert _sig() == _sig()


@pytest.mark.parametrize(
    "field,value",
    [
        ("script_urls", ["https://x.test/app.NEWHASH.js"]),  # a new bundle changes load time
        ("css_urls", ["https://x.test/site.NEWHASH.css"]),
        ("images_total", 40),
        ("title", "Un titre completement different"),
        ("text_word_count", 4000),
    ],
)
def test_a_page_that_renders_differently_gets_a_new_signature(field: str, value) -> None:
    assert _sig(**{field: value}) != _sig(), f"{field} changed but the cache would serve a stale score"


def test_the_strategy_is_part_of_the_signature() -> None:
    # Mobile and desktop are different measurements of the same URL.
    page = [_page()]
    assert seo_audit._pagespeed_signature(page, "mobile") != seo_audit._pagespeed_signature(page, "desktop")


def test_script_order_does_not_invalidate_a_measurement() -> None:
    # The parser's ordering is not stable; only the SET of assets affects load time.
    a = _sig(script_urls=["https://x.test/a.js", "https://x.test/b.js"])
    b = _sig(script_urls=["https://x.test/b.js", "https://x.test/a.js"])
    assert a == b


def test_no_page_means_no_signature_and_therefore_no_cache() -> None:
    assert seo_audit._pagespeed_signature([], "mobile") == ""


# --- serving a hit --------------------------------------------------------------------------

def _entry(signature: str, *, age_days: float = 0.0, summary=None) -> dict:
    return {
        "signature": signature,
        "fetched_ts": time.time() - age_days * 86400.0,
        "fetched_at": "2026-08-26T00:00:00Z",
        "summary": summary if summary is not None else {"performance_score": 87},
    }


def test_a_fresh_matching_entry_is_reused() -> None:
    sig = _sig()
    hit = seo_audit._pagespeed_cache_hit(_entry(sig), signature=sig, now=time.time())
    assert hit == {"performance_score": 87}


def test_a_changed_page_is_never_served_from_cache() -> None:
    assert seo_audit._pagespeed_cache_hit(_entry(_sig()), signature=_sig(images_total=40), now=time.time()) is None


def test_an_entry_past_its_ttl_is_refetched() -> None:
    # A CDN change or a swapped third-party script moves performance without moving the
    # signature, so age alone still has to expire an entry.
    sig = _sig()
    old = _entry(sig, age_days=seo_audit._PAGESPEED_CACHE_TTL_DAYS + 1)
    assert seo_audit._pagespeed_cache_hit(old, signature=sig, now=time.time()) is None


def test_an_entry_from_the_future_is_not_trusted() -> None:
    # Clock skew between containers must not pin a measurement forever.
    sig = _sig()
    future = _entry(sig, age_days=-5)
    assert seo_audit._pagespeed_cache_hit(future, signature=sig, now=time.time()) is None


@pytest.mark.parametrize("entry", [None, {}, "nope", {"signature": "x"}, {"signature": "x", "summary": "nope"}])
def test_a_malformed_entry_is_a_miss_not_a_crash(entry) -> None:
    assert seo_audit._pagespeed_cache_hit(entry, signature="x", now=time.time()) is None


def test_an_empty_signature_can_never_hit() -> None:
    assert seo_audit._pagespeed_cache_hit(_entry(""), signature="", now=time.time()) is None


# --- the file on disk -----------------------------------------------------------------------

def test_a_cache_survives_a_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "pagespeed-cache.json"
    entries = {"https://x.test/p": _entry("sig")}
    seo_audit._pagespeed_cache_save(str(path), entries)
    assert seo_audit._pagespeed_cache_load(str(path))["https://x.test/p"]["signature"] == "sig"


def test_a_corrupt_cache_is_a_cold_start_not_a_failed_crawl(tmp_path: Path) -> None:
    path = tmp_path / "pagespeed-cache.json"
    path.write_text("{ this is not json", encoding="utf-8")
    assert seo_audit._pagespeed_cache_load(str(path)) == {}


def test_a_missing_cache_is_a_cold_start(tmp_path: Path) -> None:
    assert seo_audit._pagespeed_cache_load(str(tmp_path / "absent.json")) == {}
    assert seo_audit._pagespeed_cache_load(None) == {}


def test_saving_leaves_no_half_written_file(tmp_path: Path) -> None:
    # A crawl killed by its timeout must not corrupt the cache for the next one.
    path = tmp_path / "pagespeed-cache.json"
    seo_audit._pagespeed_cache_save(str(path), {"u": _entry("sig")})
    assert not list(tmp_path.glob("*.tmp")), "a temp file was left behind"
    json.loads(path.read_text(encoding="utf-8"))


def test_the_cache_is_trimmed_and_keeps_the_newest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(seo_audit, "_PAGESPEED_CACHE_MAX_ENTRIES", 3)
    path = tmp_path / "pagespeed-cache.json"
    entries = {}
    for i in range(10):
        e = _entry(f"sig{i}")
        e["fetched_at"] = f"2026-08-{i + 10:02d}T00:00:00Z"
        entries[f"https://x.test/{i}"] = e
    seo_audit._pagespeed_cache_save(str(path), entries)
    kept = seo_audit._pagespeed_cache_load(str(path))
    assert len(kept) == 3
    assert set(kept) == {"https://x.test/9", "https://x.test/8", "https://x.test/7"}


def test_saving_to_an_impossible_path_does_not_break_the_crawl(tmp_path: Path) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file", encoding="utf-8")
    seo_audit._pagespeed_cache_save(str(blocker / "nested" / "cache.json"), {"u": _entry("s")})


# --- the whole runner, with the API faked --------------------------------------------------

def _config(cache_path: Path, **over):
    base = dict(
        base_url="https://x.test/",
        max_pages=10,
        max_sitemap_urls=100,
        timeout_s=15.0,
        workers=2,
        user_agent="SEOAutopilot/1.0",
        ignore_robots=True,
        allow_subdomains=False,
        include_re=None,
        exclude_re=None,
        sitemap_urls=[],
        output_dir=str(cache_path.parent),
        check_resources=False,
        max_resources=0,
        pagespeed_enabled=True,
        pagespeed_api_key="fake-key",
        pagespeed_cache_path=str(cache_path),
        pagespeed_max_urls=50,
        pagespeed_workers=2,
    )
    base.update(over)
    return seo_audit.CrawlConfig(**base)


def _site(n: int = 5):
    return [
        _page(url=f"https://x.test/p{i}", final_url=f"https://x.test/p{i}", title=f"Page {i}")
        for i in range(n)
    ]


@pytest.fixture()
def counted_api(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []

    def fake_fetch(url, **_kw):
        calls.append(url)
        return {"lighthouseResult": {}}, None

    monkeypatch.setattr(seo_audit, "_pagespeed_fetch", fake_fetch)
    monkeypatch.setattr(
        seo_audit, "_pagespeed_extract_summary",
        lambda payload, strategy: {"strategy": strategy, "performance_score": 91},
    )
    return calls


def test_a_second_crawl_of_an_unchanged_site_calls_the_api_zero_times(
    tmp_path: Path, counted_api
) -> None:
    # This is the entire point: 25 000 API queries/day shared by every customer, and a weekly
    # crawler was re-buying the same 50 measurements every week.
    cache = tmp_path / "pagespeed-cache.json"
    cfg = _config(cache)

    first = seo_audit._run_pagespeed(_site(), cfg)
    assert len(counted_api) == 5
    assert first["tested"] == 5 and first["cached"] == 0

    counted_api.clear()
    second = seo_audit._run_pagespeed(_site(), cfg)
    assert counted_api == [], "the API was called again for pages that had not changed"
    assert second["cached"] == 5 and second["tested"] == 0
    assert second["requested"] == 5


def test_the_cached_score_actually_reaches_the_pages(tmp_path: Path, counted_api) -> None:
    cfg = _config(tmp_path / "pagespeed-cache.json")
    seo_audit._run_pagespeed(_site(), cfg)
    pages = _site()
    seo_audit._run_pagespeed(pages, cfg)
    assert all(p.pagespeed == {"strategy": "mobile", "performance_score": 91} for p in pages)


def test_only_the_pages_that_changed_are_re_measured(tmp_path: Path, counted_api) -> None:
    cfg = _config(tmp_path / "pagespeed-cache.json")
    seo_audit._run_pagespeed(_site(), cfg)
    counted_api.clear()

    changed = _site()
    changed[2] = _page(
        url="https://x.test/p2", final_url="https://x.test/p2",
        script_urls=["https://x.test/app.REBUILT.js"],
    )
    result = seo_audit._run_pagespeed(changed, cfg)
    assert counted_api == ["https://x.test/p2"]
    assert result["cached"] == 4 and result["tested"] == 1


def test_a_failed_measurement_is_not_cached(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Caching an error would keep serving it for the whole TTL.
    monkeypatch.setattr(seo_audit, "_pagespeed_fetch", lambda url, **_kw: (None, "HTTP 500"))
    cfg = _config(tmp_path / "pagespeed-cache.json")
    result = seo_audit._run_pagespeed(_site(2), cfg)
    assert result["errors"] == 2
    assert seo_audit._pagespeed_cache_load(cfg.pagespeed_cache_path) == {}


def test_without_a_cache_path_nothing_changes(tmp_path: Path, counted_api) -> None:
    cfg = _config(tmp_path / "unused.json", pagespeed_cache_path=None)
    seo_audit._run_pagespeed(_site(3), cfg)
    counted_api.clear()
    seo_audit._run_pagespeed(_site(3), cfg)
    assert len(counted_api) == 3, "with no cache configured every crawl must still measure"
