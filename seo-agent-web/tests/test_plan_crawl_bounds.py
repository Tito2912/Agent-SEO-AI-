"""Per-plan crawl bounds: what a plan may ask for must be what a slot can deliver.

The platform's scarce resource is worker slot-time, not pages. Before these bounds existed
the crawl form offered 200 000 pages to every plan including Free, so a user could queue a
crawl that provably could not finish, hold a worker slot until the timeout killed it, get the
whole reservation refunded, and retry — free for them, hours of CPU for us.

The invariant below is the reason the numbers are what they are: each plan's page cap must
still complete inside that plan's own timeout at the measured crawl cost.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-plan-crawl-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))

from backend import billing  # noqa: E402

# From meta.timings on a real Render crawl (avis-invest.com, 84 pages, 2026-08-25):
# pagespeed 333.3s | crawl 139.4s | resources 2.1s | discovery 1.3s | scoring 0.6s.
# Only the crawl phase scales with the site.
FIXED_S = 337.0
PER_PAGE_S = 1.66
# PageData costs a measured 11.3 KB of live RAM per page (was 41.2 before shared URL strings
# and a slots record per link); 16.4 KB keeps the same prudence margin as the old 60.
RAM_KB_PER_PAGE = 16.4
WORKER_CONCURRENCY = 2  # SEO_AGENT_WORKER_CONCURRENCY on the Render worker
# 2 GB - one Chromium per concurrent job (400 MB) - 300 MB runtime, split across jobs.
RAM_BUDGET_MB_PER_JOB = (2048 - 400 * WORKER_CONCURRENCY - 300) / WORKER_CONCURRENCY
PLANS = ("free", "solo", "pro", "business")


def _duration_s(pages: int) -> float:
    return FIXED_S + pages * PER_PAGE_S


def test_every_plan_cap_completes_inside_its_own_timeout():
    for plan in PLANS:
        cfg = billing.crawl_config_for_plan(plan)
        pages = cfg["max_pages_per_crawl"]
        timeout = cfg["job_timeout_s"]
        assert _duration_s(pages) < timeout, (
            f"{plan}: {pages} pages needs {_duration_s(pages):.0f}s but the job is killed at {timeout}s"
        )


def test_caps_leave_headroom_for_sites_slower_than_the_reference():
    # A cap sized to 100% of the timeout would fail on any site slower than the reference.
    # No lower bound here: business is bound by MEMORY, not time, and legitimately leaves
    # half its timeout unused.
    for plan in PLANS:
        cfg = billing.crawl_config_for_plan(plan)
        used = _duration_s(cfg["max_pages_per_crawl"]) / cfg["job_timeout_s"]
        assert used <= 0.85, f"{plan}: cap uses {used:.0%} of its timeout"


def test_no_plan_can_ask_for_more_pages_than_fit_in_RAM():
    # Two concurrent crawls each hold their own page set. A cap the box cannot hold does not
    # produce a slow crawl, it produces an OOM-killed worker — and this box has been killed
    # for exactly that before.
    for plan in PLANS:
        pages = billing.crawl_config_for_plan(plan)["max_pages_per_crawl"]
        needed_mb = pages * RAM_KB_PER_PAGE / 1024
        assert needed_mb <= RAM_BUDGET_MB_PER_JOB, (
            f"{plan}: {pages} pages need {needed_mb:.0f} MB but a job has "
            f"{RAM_BUDGET_MB_PER_JOB:.0f} MB"
        )


def test_the_fixed_cost_is_not_charged_to_every_page():
    # Guards the mistake this table was rebuilt to correct: deriving caps from a whole crawl's
    # duration divided by its page count folds PageSpeed's fixed cost into every page.
    naive_per_page = (FIXED_S + 84 * PER_PAGE_S) / 84
    assert naive_per_page > PER_PAGE_S * 3, "the reference crawl no longer illustrates the trap"
    for plan in PLANS:
        cfg = billing.crawl_config_for_plan(plan)
        naive_cap = int(cfg["job_timeout_s"] * 0.8 / naive_per_page)
        assert cfg["max_pages_per_crawl"] > naive_cap, (
            f"{plan}: cap looks like it was derived from the average, not the marginal cost"
        )


def test_bounds_increase_with_the_plan():
    pages = [billing.crawl_config_for_plan(p)["max_pages_per_crawl"] for p in PLANS]
    timeouts = [billing.crawl_config_for_plan(p)["job_timeout_s"] for p in PLANS]
    assert pages == sorted(pages) and len(set(pages)) == len(pages)
    assert timeouts == sorted(timeouts) and len(set(timeouts)) == len(timeouts)


def test_unknown_plan_falls_back_to_the_most_restrictive_bounds():
    free = billing.crawl_config_for_plan("free")
    for bogus in ("", "enterprise", "PRO ", None):
        assert billing.crawl_config_for_plan(bogus) == free or bogus == "PRO "
    # "PRO " is normalised (stripped + lowercased), so it resolves to the real pro plan.
    assert billing.crawl_config_for_plan("PRO ") == billing.crawl_config_for_plan("pro")


def test_admin_override_can_retune_bounds_without_a_deploy(monkeypatch):
    monkeypatch.setenv(
        "PLAN_CONFIG_JSON",
        json.dumps({"pro": {"crawl": {"max_pages_per_crawl": 2_500, "job_timeout_s": 20_000}}}),
    )
    cfg = billing.crawl_config_for_plan("pro")
    assert cfg == {"max_pages_per_crawl": 2_500, "job_timeout_s": 20_000}
    # Untouched plans keep their defaults.
    assert billing.crawl_config_for_plan("solo")["max_pages_per_crawl"] == 3_000


def test_override_ignores_junk_and_non_positive_values(monkeypatch):
    monkeypatch.setenv(
        "PLAN_CONFIG_JSON",
        json.dumps({"business": {"crawl": {"max_pages_per_crawl": 0, "job_timeout_s": "8h"}}}),
    )
    cfg = billing.crawl_config_for_plan("business")
    assert cfg["max_pages_per_crawl"] == 13_000
    assert cfg["job_timeout_s"] == 28_800


def test_monthly_quota_stays_reachable_within_the_per_crawl_cap():
    # A monthly page quota you cannot spend is a lie on the pricing page: each plan must be
    # able to reach its quota with a believable number of crawls across its allowed projects.
    cat = billing.plan_catalog()
    for plan in ("solo", "pro", "business"):
        limits = cat[plan]["limits"]
        cap = billing.crawl_config_for_plan(plan)["max_pages_per_crawl"]
        crawls_needed = limits["pages_crawled_month"] / cap
        per_project = crawls_needed / limits["projects"]
        assert per_project <= 40, (
            f"{plan}: reaching {limits['pages_crawled_month']} pages needs {per_project:.0f} "
            f"crawls per project per month at {cap} pages each"
        )
