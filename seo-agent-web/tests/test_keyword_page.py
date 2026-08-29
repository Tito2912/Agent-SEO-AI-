"""The keywords page, in the states a customer actually passes through.

The engine and the storage existed before this page did, and nothing showed them — a feature
nobody can see is a feature that does not exist. These render the real page through the real
handler for a non-admin account, with Search Console stubbed, because the states that matter are
the ones a customer hits: not connected, connected with nothing to report, and connected with
work to do.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-kwpage-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import Project, TrackedKeyword, User  # noqa: E402

# The real account's shape: page-one rankings, thousands of impressions, almost no clicks.
LIVE_ROWS = [
    {"keyword": "kling ai", "clicks": 9, "impressions": 6462, "ctr": 0.0014,
     "position": 6.6, "page": "https://site.fr/guide-kling-ai-fr"},
    {"keyword": "pictory ai pricing", "clicks": 0, "impressions": 481, "ctr": 0.0,
     "position": 7.9, "page": "https://site.fr/blog/pictory"},
]


@pytest.fixture()
def customer():
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    slug = f"site-{tag}"
    with app_module.DB.session() as db:
        user = User(email=f"client-{tag}@exemple.fr",
                    password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        proj = Project(owner_user_id=uid, slug=slug, site_name="site.fr",
                       base_url="https://site.fr/")
        db.add(proj)
        db.commit()
        db.refresh(proj)
        pid = str(proj.id)
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug, pid


@pytest.fixture()
def gsc(monkeypatch):
    """Stub Search Console. The page's job is what it does with rows, not how it gets them."""
    def _install(payload):
        monkeypatch.setattr(app_module, "_fetch_gsc_live_items", lambda **kw: payload)
    return _install


def _csrf_post(client, path, data, form_page):
    html = client.get(form_page).text
    token = re.search(r'name="_csrf"\s+value="([^"]*)"', html)
    assert token, "no CSRF token on the page"
    return client.post(path, data={**data, "_csrf": token.group(1)}, follow_redirects=False)


def test_a_project_without_search_console_is_told_so(customer, gsc) -> None:
    """Not connected and nothing to report are different answers; an empty table conflates them."""
    gsc({"ok": False, "enabled": True, "source": "gsc", "reason": "missing_credentials"})
    client, slug, _ = customer
    body = client.get(f"/projects/{slug}/keywords/opportunities").text
    assert "Search Console n'est pas connecté" in body
    assert "Paramètres de crawl" in body, "the page must say where to go next"


def test_a_connected_project_with_nothing_to_report_says_that_too(customer, gsc) -> None:
    gsc({"ok": True, "items": []})
    client, slug, _ = customer
    body = client.get(f"/projects/{slug}/keywords/opportunities").text
    assert "Aucune requête ne remplit les critères" in body
    assert "Search Console n'est pas connecté" not in body


def test_the_opportunities_name_the_query_and_the_page(customer, gsc) -> None:
    gsc({"ok": True, "items": LIVE_ROWS})
    client, slug, _ = customer
    body = client.get(f"/projects/{slug}/keywords/opportunities").text
    assert "kling ai" in body and "guide-kling-ai-fr" in body
    assert "Beaucoup moins cliquée que la normale" in body
    assert "Vue, jamais cliquée" in body


def test_no_gain_is_claimed_when_there_is_nothing_to_compare_against(customer, gsc) -> None:
    # The account has no top-3 query, so the estimate is withheld and the page says why.
    gsc({"ok": True, "items": LIVE_ROWS})
    client, slug, _ = customer
    body = client.get(f"/projects/{slug}/keywords/opportunities").text
    assert "Aucune estimation de gain" in body
    assert "clics par période si ces" not in body


def test_tracking_an_opportunity_keeps_its_page_as_the_target(customer, gsc) -> None:
    gsc({"ok": True, "items": LIVE_ROWS})
    client, slug, pid = customer
    page = f"/projects/{slug}/keywords/opportunities"
    response = _csrf_post(client, f"/projects/{slug}/keywords/track",
                          {"query": "kling ai", "target_url": "https://site.fr/guide-kling-ai-fr",
                           "source": "gsc_opportunity"}, page)
    assert response.status_code == 303
    with app_module.DB.session() as db:
        row = db.scalar(select(TrackedKeyword).where(TrackedKeyword.project_id == pid))
    assert row.query == "kling ai"
    assert row.target_url == "https://site.fr/guide-kling-ai-fr"
    assert row.source == "gsc_opportunity"
    assert "suivie" in client.get(page).text


def test_tracking_the_same_query_twice_is_not_an_error(customer, gsc) -> None:
    """It is the same decision. Letting the unique constraint surface as a 500 would be absurd
    for a button a customer may double-click."""
    gsc({"ok": True, "items": LIVE_ROWS})
    client, slug, pid = customer
    page = f"/projects/{slug}/keywords/opportunities"
    _csrf_post(client, f"/projects/{slug}/keywords/track", {"query": "kling ai"}, page)
    second = _csrf_post(client, f"/projects/{slug}/keywords/track",
                        {"query": "kling ai", "target_url": "https://site.fr/autre"}, page)
    assert second.status_code == 303
    with app_module.DB.session() as db:
        rows = db.scalars(select(TrackedKeyword).where(TrackedKeyword.project_id == pid)).all()
    assert len(rows) == 1, "the same query was tracked twice"
    assert rows[0].target_url == "https://site.fr/autre", "the second click must refresh the target"


def test_an_empty_query_is_ignored_rather_than_stored(customer, gsc) -> None:
    gsc({"ok": True, "items": []})
    client, slug, pid = customer
    page = f"/projects/{slug}/keywords/opportunities"
    assert _csrf_post(client, f"/projects/{slug}/keywords/track", {"query": "   "}, page).status_code == 303
    with app_module.DB.session() as db:
        assert db.scalars(select(TrackedKeyword).where(TrackedKeyword.project_id == pid)).all() == []


def test_a_keyword_can_be_removed(customer, gsc) -> None:
    gsc({"ok": True, "items": LIVE_ROWS})
    client, slug, pid = customer
    page = f"/projects/{slug}/keywords/opportunities"
    _csrf_post(client, f"/projects/{slug}/keywords/track", {"query": "kling ai"}, page)
    with app_module.DB.session() as db:
        kid = str(db.scalar(select(TrackedKeyword).where(TrackedKeyword.project_id == pid)).id)
    assert _csrf_post(client, f"/projects/{slug}/keywords/untrack", {"keyword_id": kid}, page).status_code == 303
    with app_module.DB.session() as db:
        assert db.scalars(select(TrackedKeyword).where(TrackedKeyword.project_id == pid)).all() == []


def test_another_account_cannot_remove_a_keyword_by_its_id(customer, gsc) -> None:
    """An id alone must never be enough: the row is scoped to the project in the URL, whose
    ownership the handler already checked."""
    gsc({"ok": True, "items": LIVE_ROWS})
    client, slug, pid = customer
    page = f"/projects/{slug}/keywords/opportunities"
    _csrf_post(client, f"/projects/{slug}/keywords/track", {"query": "kling ai"}, page)
    with app_module.DB.session() as db:
        kid = str(db.scalar(select(TrackedKeyword).where(TrackedKeyword.project_id == pid)).id)

    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        other = User(email=f"autre-{tag}@exemple.fr",
                     password_hash=auth.hash_password("y" * 12), is_admin=False)
        db.add(other)
        db.commit()
        db.refresh(other)
        foreign_slug = f"pas-a-moi-{tag}"
        db.add(Project(owner_user_id=str(other.id), slug=foreign_slug,
                       site_name="pas-a-moi.fr", base_url="https://pas-a-moi.fr/"))
        db.commit()
        other_id = str(other.id)

    intruder = TestClient(app)
    intruder.cookies.set(auth.SESSION_COOKIE_NAME,
                         auth.make_session_token(user_id=other_id,
                                                 secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    intruder.post(f"/projects/{foreign_slug}/keywords/untrack",
                  data={"keyword_id": kid}, follow_redirects=False)
    with app_module.DB.session() as db:
        assert db.get(TrackedKeyword, kid) is not None, "another account deleted the row"
