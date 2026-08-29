"""Following a keyword over time means storing it, and storing it has two traps.

Search Console keeps roughly sixteen months and offers no per-project history, so "management"
cannot be a live view — it needs rows. Two tables rather than one: the keyword is a DECISION
(this query matters, this page should win it), a snapshot is an OBSERVATION. Keeping them apart
is what lets the target page be corrected without rewriting the past.

The two properties worth defending, both of which are silent when broken:

  * adding the same query twice is the same decision, not two;
  * re-measuring a day already recorded must OVERWRITE, or a nightly refresh run twice doubles
    the curve and every trend drawn from it is wrong.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from datetime import date
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-kw-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from sqlalchemy import select  # noqa: E402
from sqlalchemy.exc import IntegrityError  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth  # noqa: E402
from backend.models import (  # noqa: E402
    Project, TrackedKeyword, TrackedKeywordSnapshot, User,
)


@pytest.fixture()
def project():
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        user = User(email=f"client-{tag}@exemple.fr",
                    password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        proj = Project(owner_user_id=str(user.id), slug=f"site-{tag}",
                       site_name="site.fr", base_url="https://site.fr/")
        db.add(proj)
        db.commit()
        db.refresh(proj)
        return str(proj.id), str(user.id)


def _add_keyword(project_id, user_id, query="kling ai", **kw):
    with app_module.DB.session() as db:
        row = TrackedKeyword(project_id=project_id, user_id=user_id, query=query, **kw)
        db.add(row)
        db.commit()
        db.refresh(row)
        return str(row.id)


def test_a_keyword_is_stored_with_its_target_and_its_origin(project) -> None:
    project_id, user_id = project
    kid = _add_keyword(project_id, user_id, target_url="https://site.fr/guide",
                       source="gsc_opportunity")
    with app_module.DB.session() as db:
        row = db.get(TrackedKeyword, kid)
        assert row.query == "kling ai"
        assert row.target_url == "https://site.fr/guide"
        # Worth keeping: a suggestion the customer chose to track is the product working, and
        # that is measurable later.
        assert row.source == "gsc_opportunity"
        assert row.status == "tracked"


def test_the_same_query_cannot_be_tracked_twice_on_one_project(project) -> None:
    project_id, user_id = project
    _add_keyword(project_id, user_id, query="pictory ai pricing")
    with pytest.raises(IntegrityError):
        _add_keyword(project_id, user_id, query="pictory ai pricing")


def test_two_projects_may_track_the_same_query(project) -> None:
    # The constraint is per project, not global: two customers wanting the same keyword is
    # ordinary, and a global unique index would have made the second one fail.
    project_id, user_id = project
    _add_keyword(project_id, user_id, query="kling ai")
    with app_module.DB.session() as db:
        other = Project(owner_user_id=user_id, slug=f"autre-{uuid.uuid4().hex[:8]}",
                        site_name="autre.fr", base_url="https://autre.fr/")
        db.add(other)
        db.commit()
        db.refresh(other)
        other_id = str(other.id)
    _add_keyword(other_id, user_id, query="kling ai")  # must not raise


def _snapshot(keyword_id, day, **values):
    with app_module.DB.session() as db:
        db.add(TrackedKeywordSnapshot(keyword_id=keyword_id, captured_on=day, **values))
        db.commit()


def test_a_day_can_only_be_recorded_once(project) -> None:
    """The trap: a refresh run twice would double the curve, silently."""
    project_id, user_id = project
    kid = _add_keyword(project_id, user_id)
    _snapshot(kid, date(2026, 8, 29), clicks=9, impressions=6462, ctr=0.0014, position=6.6)
    with pytest.raises(IntegrityError):
        _snapshot(kid, date(2026, 8, 29), clicks=9, impressions=6462, ctr=0.0014, position=6.6)


def test_history_accumulates_across_days(project) -> None:
    project_id, user_id = project
    kid = _add_keyword(project_id, user_id)
    _snapshot(kid, date(2026, 8, 28), clicks=4, impressions=6000, ctr=0.0007, position=7.4)
    _snapshot(kid, date(2026, 8, 29), clicks=9, impressions=6462, ctr=0.0014, position=6.6)
    with app_module.DB.session() as db:
        rows = db.scalars(
            select(TrackedKeywordSnapshot)
            .where(TrackedKeywordSnapshot.keyword_id == kid)
            .order_by(TrackedKeywordSnapshot.captured_on)
        ).all()
    assert [r.position for r in rows] == [7.4, 6.6], "the trend a customer is paying to see"


def test_the_measurements_are_stored_as_given(project) -> None:
    """Position is an average over the window and CTR is that window's ratio, so recomputing
    either from the other two later would quietly invent a number."""
    project_id, user_id = project
    kid = _add_keyword(project_id, user_id)
    _snapshot(kid, date(2026, 8, 29), clicks=9, impressions=6462, ctr=0.0014, position=6.6,
              page="https://site.fr/guide-kling-ai-fr")
    with app_module.DB.session() as db:
        row = db.scalars(select(TrackedKeywordSnapshot)
                         .where(TrackedKeywordSnapshot.keyword_id == kid)).one()
    assert (row.clicks, row.impressions) == (9, 6462)
    assert row.ctr == pytest.approx(0.0014)
    assert row.position == pytest.approx(6.6)
    assert row.page == "https://site.fr/guide-kling-ai-fr"


def test_dropping_a_keyword_takes_its_history_with_it(project) -> None:
    # Orphan snapshots would be counted by any later aggregate and belong to nothing.
    project_id, user_id = project
    kid = _add_keyword(project_id, user_id)
    _snapshot(kid, date(2026, 8, 29), clicks=1, impressions=100, ctr=0.01, position=8.0)
    with app_module.DB.session() as db:
        db.delete(db.get(TrackedKeyword, kid))
        db.commit()
    with app_module.DB.session() as db:
        left = db.scalars(select(TrackedKeywordSnapshot)
                          .where(TrackedKeywordSnapshot.keyword_id == kid)).all()
    assert left == []
