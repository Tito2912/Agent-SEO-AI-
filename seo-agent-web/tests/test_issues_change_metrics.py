"""A successful correction must not read as a regression the agent caused.

Walking the whole customer path on voiceoverstudioai.com (2026-08-28) ended with a merged PR
that fixed a canonical, a re-crawl that confirmed it, and an anomaly table showing:

    Canonical URL changed    Indexability    info    1    +1

The `+1` is the fix being detected, not a defect: `canonical_url_changed` compares each page's
canonical to the PREVIOUS crawl, so repairing one necessarily raises it on the next run. Shown
in the same table, the same column and the same red as real regressions, it tells a customer who
cannot read the code that the agent broke something the moment it repaired something.

Ahrefs marks these `[Δ]` and shows "—" rather than an Actual-issues count, so counting them as
anomalies was also a parity break. The sibling metrics (`title_tag_changed`,
`meta_description_changed`, `h1_tag_changed`, `word_count_changed`) already had their emission
zeroed in seo_audit.py for exactly this reason; these three still emit because the movement is
worth seeing — just not as a defect.

These tests render the real page through the real handler for a non-admin account, in the state
that produced the trap: two crawls, one canonical changed between them.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-deltas-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import audit_dashboard as dash  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import Project, User  # noqa: E402

BEFORE_TS = "20260828-090000"
AFTER_TS = "20260828-115319"


def _write_report(runs_dir: Path, slug: str, timestamp: str, issues: dict) -> None:
    path = dash.report_path(runs_dir, slug, timestamp)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "meta": {
            "base_url": "https://site-du-client.fr/",
            "pages_crawled": 2,
            "max_pages": 150,
        },
        "issues": issues,
        "pages": [
            {"url": "https://site-du-client.fr/blog", "status_code": 200},
            {"url": "https://site-du-client.fr/", "status_code": 200},
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _block(urls: list[str]) -> dict:
    return {"count": len(urls), "examples": [{"url": u} for u in urls]}


@pytest.fixture()
def customer_who_just_merged_a_correction():
    """A non-admin account with two crawls: the correction landed between them.

    The earlier crawl carries the defect, the later one carries the change metric the fix
    produced — exactly the shape that made a repair look like a regression.
    """
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    slug = f"site-du-client-{tag}"

    with app_module.DB.session() as db:
        user = User(
            email=f"client-{tag}@exemple.fr",
            password_hash=auth.hash_password("x" * 12),
            is_admin=False,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        db.add(Project(
            owner_user_id=uid,
            slug=slug,
            site_name="site-du-client.fr",
            base_url="https://site-du-client.fr/",
        ))
        db.commit()

    # Runs are stored per user (`<runs>/<user_id>/<slug>/…`), so the fixture writes where the
    # handler will actually look — a shared-root fixture renders a 404 that hides the real test.
    runs_dir = app_module._runs_dir_for_user(uid)

    _write_report(
        runs_dir,
        slug,
        BEFORE_TS,
        {
            "canonical_points_to_redirect": _block(["https://site-du-client.fr/blog"]),
            "title_too_long": _block(["https://site-du-client.fr/"]),
        },
    )
    _write_report(
        runs_dir,
        slug,
        AFTER_TS,
        {
            # The defect is gone; the change metric appeared in its place.
            "canonical_url_changed": _block(["https://site-du-client.fr/blog"]),
            "title_too_long": _block(["https://site-du-client.fr/"]),
        },
    )

    token = auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"])
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, token)
    return client, slug, runs_dir


def test_a_change_metric_is_not_counted_as_an_anomaly(customer_who_just_merged_a_correction) -> None:
    _client, slug, runs_dir = customer_who_just_merged_a_correction
    summary = dash.project_overview(runs_dir, slug, timestamp=AFTER_TS, compare_to=None)["current"]["summary"]

    assert summary["issues_total"] == 1, (
        "the anomaly total counted a between-crawl change metric: a customer who fixes their "
        f"site would see the total refuse to drop (issues_total={summary['issues_total']})"
    )
    assert "Indexability" not in summary["issues_by_category"], (
        "a change metric inflated a category count"
    )
    assert all(not r["is_delta"] for r in summary["top_issues"]), (
        "a change metric was listed as work to do"
    )


def test_the_change_metric_is_still_reported_just_not_as_a_defect(
    customer_who_just_merged_a_correction,
) -> None:
    # Suppressing it would lose a signal Ahrefs does show. It stays in the payload, flagged.
    _client, slug, runs_dir = customer_who_just_merged_a_correction
    summary = dash.project_overview(runs_dir, slug, timestamp=AFTER_TS, compare_to=None)["current"]["summary"]
    rows = {r["key"]: r for r in summary["issues"]}

    assert "canonical_url_changed" in rows, "the change metric was dropped instead of separated"
    assert rows["canonical_url_changed"]["is_delta"] is True
    assert rows["title_too_long"]["is_delta"] is False, "a real defect was mistaken for a delta"


def test_a_change_metric_carries_no_evolution_arrow(customer_who_just_merged_a_correction) -> None:
    """The `+1` was the whole problem: for a change metric the count IS the change."""
    _client, slug, runs_dir = customer_who_just_merged_a_correction
    summary = dash.project_overview(
        runs_dir, slug, timestamp=AFTER_TS, compare_to=BEFORE_TS
    )["current"]["summary"]
    rows = {r["key"]: r for r in summary["issues"]}

    assert rows["canonical_url_changed"]["change"] is None, (
        "a repaired canonical still rendered as '+1 introduced' next to real regressions"
    )


def test_the_anomaly_table_does_not_list_the_change_metric(
    customer_who_just_merged_a_correction,
) -> None:
    """Rendered through the real handler: a fragment test cannot see a template scoping bug."""
    client, slug, _runs_dir = customer_who_just_merged_a_correction
    response = client.get(f"/projects/{slug}/issues?crawl={AFTER_TS}")

    assert response.status_code == 200
    body = response.text
    assert "Changements depuis le crawl précédent" in body, (
        "the change metric vanished from the page entirely instead of moving to its own section"
    )
    assert "canonical_url_changed" in body

    anomalies = body.split("Changements depuis le crawl précédent")[0]
    assert "canonical_url_changed" not in anomalies, (
        "the change metric is still listed among the anomalies"
    )
    assert "title_too_long" in anomalies, "the real defect left the anomaly table"


def test_a_site_whose_only_row_is_a_change_metric_reads_as_clean(
    customer_who_just_merged_a_correction,
) -> None:
    """The end state a corrected site reaches: nothing left but the trace of the repair."""
    _client, slug, runs_dir = customer_who_just_merged_a_correction
    ts = "20260828-160000"
    _write_report(
        runs_dir, slug, ts,
        {"canonical_url_changed": _block(["https://site-du-client.fr/blog"])},
    )
    summary = dash.project_overview(runs_dir, slug, timestamp=ts, compare_to=None)["current"]["summary"]

    assert summary["issues_total"] == 0, (
        "a site with zero defects still reported anomalies because of the repair's own trace"
    )
