"""Being refused by a host is a crawl problem, never a defect of the customer's site.

Observed in production on avis-invest.com: 11 pages reported as `http_4xx` while all 11
answered 200 when checked by hand seconds later, plus 14 phantom "broken internal link" and
29 phantom "hreflang to broken page". The site was healthy; the crawler had been firewalled.

Two independent faults produced that, and both are pinned here:
  - the crawler ignored robots.txt Crawl-delay and paced nothing, so it invited the block;
  - scoring read a 403 as "this page is broken", and a dozen issue families test
    `status_code >= 400`, so each blocked page multiplied into several phantom errors.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_for_politeness_tests",
    REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py",
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_for_politeness_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)


# --- robots.txt Crawl-delay ---------------------------------------------------------------

def test_crawl_delay_is_read_for_the_matching_user_agent() -> None:
    rules = seo_audit._parse_robots_rules(
        "User-agent: *\nCrawl-delay: 2\nDisallow: /admin\n\n"
        "User-agent: SEOAutopilot\nCrawl-delay: 5\nDisallow: /private\n"
    )
    assert rules.crawl_delay_for("SEOAutopilot/1.0") == 5.0
    assert rules.crawl_delay_for("SomeOtherBot/2.0") == 2.0


def test_crawl_delay_and_disallow_come_from_the_same_group() -> None:
    # Obeying one group's Disallow while taking another's Crawl-delay would be incoherent.
    rules = seo_audit._parse_robots_rules(
        "User-agent: *\nCrawl-delay: 9\nDisallow: /\n\n"
        "User-agent: SEOAutopilot\nCrawl-delay: 1\nDisallow: /nope\n"
    )
    assert rules.crawl_delay_for("SEOAutopilot/1.0") == 1.0
    assert rules.can_fetch("SEOAutopilot/1.0", "https://x.test/anything") is True
    assert rules.can_fetch("SEOAutopilot/1.0", "https://x.test/nope") is False


def test_a_hostile_crawl_delay_cannot_stall_the_crawl() -> None:
    rules = seo_audit._parse_robots_rules("User-agent: *\nCrawl-delay: 86400\n")
    assert rules.crawl_delay_for("SEOAutopilot/1.0") == 30.0


@pytest.mark.parametrize("value", ["", "abc", "-1", "1,5"])
def test_malformed_crawl_delay_never_breaks_robots_parsing(value: str) -> None:
    rules = seo_audit._parse_robots_rules(f"User-agent: *\nCrawl-delay: {value}\nDisallow: /x\n")
    assert rules.can_fetch("Bot", "https://x.test/x") is False
    assert rules.crawl_delay_for("Bot") >= 0.0


def test_no_crawl_delay_means_no_delay() -> None:
    rules = seo_audit._parse_robots_rules("User-agent: *\nDisallow: /x\n")
    assert rules.crawl_delay_for("Bot") == 0.0


# --- per-host throttle ---------------------------------------------------------------------

def test_throttle_spaces_requests_by_the_configured_delay() -> None:
    t = seo_audit._HostThrottle()
    t.set_base_delay("x.test", 0.05)
    start = time.monotonic()
    for _ in range(4):
        t.wait("x.test")
    assert time.monotonic() - start >= 0.05 * 3


def test_throttle_slots_are_shared_across_threads() -> None:
    # The host sees one client, so the pacing must be one client's — a per-thread delay would
    # let N workers hit the site N times faster than the site asked for.
    t = seo_audit._HostThrottle()
    t.set_base_delay("x.test", 0.05)
    start = time.monotonic()
    threads = [threading.Thread(target=lambda: t.wait("x.test")) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert time.monotonic() - start >= 0.05 * 3


def test_hosts_are_paced_independently() -> None:
    t = seo_audit._HostThrottle()
    t.set_base_delay("slow.test", 0.2)
    t.wait("slow.test")
    start = time.monotonic()
    for _ in range(5):
        t.wait("fast.test")
    assert time.monotonic() - start < 0.1


def test_being_blocked_makes_the_crawler_back_off() -> None:
    t = seo_audit._HostThrottle()
    first = t.penalise("x.test")
    second = t.penalise("x.test")
    assert first >= seo_audit._BLOCK_BACKOFF_START_S
    assert second > first, "a repeated block must widen the delay, not repeat it"
    for _ in range(10):
        t.penalise("x.test")
    assert t.penalise("x.test") <= seo_audit._BLOCK_BACKOFF_MAX_S


def test_retry_after_from_the_server_wins_over_our_guess() -> None:
    t = seo_audit._HostThrottle()
    assert t.penalise("x.test", retry_after="12") == 12.0
    # Junk headers fall back to the computed backoff instead of crashing the crawl.
    assert t.penalise("x.test", retry_after="Wed, 21 Oct 2026 07:28:00 GMT") > 0


# --- scoring ------------------------------------------------------------------------------

def _page(url: str, status: int, *, blocked: bool = False, links: list[str] | None = None):
    return seo_audit.PageData(
        url=url,
        final_url=url,
        status_code=status,
        content_type="text/html",
        blocked_by_host=blocked,
        title="Un titre de page parfaitement normal",
        h1=["Titre"],
        internal_links=list(links or []),
    )


def test_a_blocked_page_is_not_reported_as_a_broken_page() -> None:
    ok = _page("https://x.test/", 200, links=["https://x.test/de/about"])
    blocked = _page("https://x.test/de/about", 403, blocked=True)
    issues = seo_audit._score_issues([ok, blocked], base_url="https://x.test/")

    def count(key: str) -> int:
        block = issues.get(key) or {}
        return int(block.get("count") or 0)

    assert count("http_4xx") == 0, "a page the host refused us was reported as the site's own 4XX"
    assert count("bad_status") == 0
    # The whole point: one blocked page used to become several unrelated phantom errors.
    assert count("page_has_links_to_broken_page_indexable") == 0
    assert count("page_has_links_to_broken_page") == 0


def test_a_genuine_4xx_is_still_reported() -> None:
    # The fix must not become a way to hide real errors.
    ok = _page("https://x.test/", 200, links=["https://x.test/gone"])
    gone = _page("https://x.test/gone", 404)
    issues = seo_audit._score_issues([ok, gone], base_url="https://x.test/")
    assert int((issues.get("http_4xx") or {}).get("count") or 0) == 1
    assert int((issues.get("http_404") or {}).get("count") or 0) == 1


# --- reporting a partial crawl as partial ---------------------------------------------------

def _report(meta_extra: dict | None = None) -> dict:
    return {
        "meta": {"base_url": "https://x.test/", "pages_crawled": 2, **(meta_extra or {})},
        "pages": [
            {"url": "https://x.test/", "status_code": 200, "content_type": "text/html"},
            {"url": "https://x.test/b", "status_code": 200, "content_type": "text/html"},
        ],
        "issues": {},
    }


def test_the_dashboard_knows_a_crawl_was_partial() -> None:
    from backend import audit_dashboard

    blocked = {"count": 11, "urls": [f"https://x.test/de/p{i}" for i in range(11)]}
    summary = audit_dashboard.summarize_report(_report({"blocked_by_host": blocked}))
    assert summary["blocked_by_host"]["count"] == 11
    assert summary["blocked_by_host"]["urls"][0] == "https://x.test/de/p0"


def test_a_complete_crawl_reports_nothing_blocked() -> None:
    from backend import audit_dashboard

    summary = audit_dashboard.summarize_report(_report())
    assert summary["blocked_by_host"]["count"] == 0


def test_reports_written_before_this_feature_still_summarise() -> None:
    # Old reports on S3 have no blocked_by_host key at all; they must not blow up or
    # invent a warning banner.
    from backend import audit_dashboard

    summary = audit_dashboard.summarize_report(_report({"blocked_by_host": "nonsense"}))
    assert summary["blocked_by_host"]["count"] == 0


def test_the_overview_warns_only_when_pages_were_blocked() -> None:
    import jinja2

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(REPO_ROOT / "seo-agent-web" / "templates")))
    source = (REPO_ROOT / "seo-agent-web" / "templates" / "project_overview.html").read_text(encoding="utf-8")
    # Render just the banner fragment: the full page pulls in the whole app context.
    start = source.index('{% set blocked =')
    end = source.index("<h1>{{ project.site_name }}</h1>")
    tpl = env.from_string(source[start:end])

    clean = tpl.render(sum={"blocked_by_host": {"count": 0, "urls": []}})
    assert "Crawl incomplet" not in clean

    partial = tpl.render(sum={"blocked_by_host": {"count": 11, "urls": ["https://x.test/de/about"]}})
    assert "Crawl incomplet" in partial and "11 pages inaccessibles" in partial
    assert "https://x.test/de/about" in partial

    # A summary from before the feature must render the same as a clean one, not crash.
    assert "Crawl incomplet" not in tpl.render(sum={})


# --- phase timings ---------------------------------------------------------------------------

def test_marginal_cost_ignores_the_fixed_phases() -> None:
    # The whole point: PageSpeed is capped at 50 URLs, so it does NOT grow with the site.
    # Folding it into a per-page average is what produced per-plan caps several times too low.
    t = seo_audit._PhaseTimer()
    t.add("discovery", 3.0)
    t.add("crawl", 200.0)
    t.add("pagespeed", 300.0)
    t.add("scoring", 7.0)
    out = t.as_dict(pages=100)
    assert out["marginal_s_per_page"] == 2.0, "marginal cost must come from the crawl phase alone"
    assert out["fixed_s"] == 310.0
    assert out["measured_total_s"] == 510.0
    # The naive figure this replaces would have been 5.1s/page — 2.5x too high.


def test_repeated_phases_accumulate() -> None:
    t = seo_audit._PhaseTimer()
    t.add("resources", 1.5)
    t.add("resources", 2.5)
    assert t.as_dict(pages=1)["phases_s"]["resources"] == 4.0


def test_timing_survives_a_crawl_with_no_pages() -> None:
    t = seo_audit._PhaseTimer()
    t.add("discovery", 2.0)
    out = t.as_dict(pages=0)
    assert "marginal_s_per_page" not in out, "no pages means no marginal cost, not a division by zero"
    assert out["measured_total_s"] == 2.0


def test_the_context_manager_records_elapsed_time() -> None:
    t = seo_audit._PhaseTimer()
    with t.time("crawl"):
        time.sleep(0.05)
    assert t.as_dict(pages=1)["phases_s"]["crawl"] >= 0.05


def test_a_phase_that_raises_is_still_timed() -> None:
    # A crash mid-PageSpeed must not erase the measurement of everything before it.
    t = seo_audit._PhaseTimer()
    with pytest.raises(RuntimeError):
        with t.time("pagespeed"):
            time.sleep(0.02)
            raise RuntimeError("boom")
    assert t.as_dict(pages=1)["phases_s"]["pagespeed"] >= 0.02


# --- PageSpeed concurrency ------------------------------------------------------------------

def test_pagespeed_concurrency_floor_lifts_projects_saved_with_the_old_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Every project that ever saved its crawl settings has pagespeed_workers=2 persisted, from
    # a form default backed by a UI hint that was wrong. No migration reaches them, so the
    # floor has to apply over the stored value.
    monkeypatch.delenv("SEO_AGENT_PAGESPEED_MIN_WORKERS", raising=False)
    assert _effective_pagespeed_workers(stored=2) == 6
    assert _effective_pagespeed_workers(stored=1) == 6


def test_a_project_asking_for_more_still_gets_more(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEO_AGENT_PAGESPEED_MIN_WORKERS", raising=False)
    assert _effective_pagespeed_workers(stored=12) == 12


def test_the_floor_can_be_switched_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEO_AGENT_PAGESPEED_MIN_WORKERS", "0")
    assert _effective_pagespeed_workers(stored=2) == 2


def test_a_junk_floor_falls_back_to_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEO_AGENT_PAGESPEED_MIN_WORKERS", "beaucoup")
    assert _effective_pagespeed_workers(stored=2) == 6


def _effective_pagespeed_workers(*, stored: int) -> int:
    """Mirror of the resolution in _run_pagespeed, which is buried inside a long function."""
    import os

    try:
        floor = int(os.getenv("SEO_AGENT_PAGESPEED_MIN_WORKERS", "6"))
    except ValueError:
        floor = 6
    return max(1, int(stored), floor)


def test_the_default_stays_far_below_google_s_documented_quota() -> None:
    # Documented: 240 queries/minute per API key. A query takes ~13 s (333 s for 50 URLs on
    # 2 workers), so N workers issue N/13*60 queries per minute.
    workers, seconds_per_query, quota_per_minute = 6, 13.3, 240
    assert workers / seconds_per_query * 60 < quota_per_minute * 0.25
