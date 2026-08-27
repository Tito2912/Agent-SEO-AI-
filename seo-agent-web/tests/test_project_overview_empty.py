"""The project page before anything has been crawled — the first page a customer ever opens.

A new account creates a project and clicks Démarrer. The crawl starts, the handler redirects
to /projects/<slug>, and that page returned 500. The cause was a banner added the day before,
placed ABOVE the `{% if project.current %}` guard that defines `sum`:

    jinja2.exceptions.UndefinedError: 'sum' is undefined

So every project that had never completed a crawl broke — which is every project a new
customer has just created, on the one screen the whole onboarding funnels into.

The test written for that banner rendered only a FRAGMENT of the template, with `sum` handed
to it explicitly, so it could not see a scoping bug. These tests render the real page through
the real handler, in the states a customer actually passes through.
"""

from __future__ import annotations

import os
import sys
import uuid
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-overview-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import Project, User  # noqa: E402


@pytest.fixture()
def customer_with_an_uncrawled_project():
    """A NON-admin account, because the admin path skips code the customer path runs."""
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]  # each test gets its own account: emails are unique
    slug = f"site-tout-neuf-{tag}"
    with app_module.DB.session() as db:
        user = User(email=f"client-{tag}@exemple.fr", password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        db.add(Project(
            owner_user_id=uid,
            slug=slug,
            site_name="site-tout-neuf.fr",
            base_url="https://site-tout-neuf.fr/",
        ))
        db.commit()
    token = auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"])
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME, token)
    return client, slug


def test_a_project_that_has_never_been_crawled_renders(customer_with_an_uncrawled_project) -> None:
    client, slug = customer_with_an_uncrawled_project
    response = client.get(f"/projects/{slug}", follow_redirects=False)
    assert response.status_code == 200, (
        "the first page of the onboarding failed for a project with no crawl"
    )
    assert "site-tout-neuf.fr" in response.text


def test_the_incomplete_crawl_banner_stays_out_of_the_way_before_any_crawl(
    customer_with_an_uncrawled_project,
) -> None:
    # Nothing has been crawled, so nothing can have been blocked. The banner must be absent —
    # not merely harmless.
    client, slug = customer_with_an_uncrawled_project
    response = client.get(f"/projects/{slug}")
    assert "Crawl incomplet" not in response.text


def test_a_project_belonging_to_someone_else_is_not_served(
    customer_with_an_uncrawled_project,
) -> None:
    # The same walk-through confirmed ownership isolation by accident; pin it on purpose.
    client, _slug = customer_with_an_uncrawled_project
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        other = User(email=f"autre-{tag}@exemple.fr", password_hash=auth.hash_password("y" * 12), is_admin=False)
        db.add(other)
        db.commit()
        db.refresh(other)
        foreign_slug = f"pas-a-moi-{tag}"
        db.add(Project(
            owner_user_id=str(other.id),
            slug=foreign_slug,
            site_name="pas-a-moi.fr",
            base_url="https://pas-a-moi.fr/",
        ))
        db.commit()

    response = client.get(f"/projects/{foreign_slug}", follow_redirects=False)
    assert response.status_code in (303, 400, 403, 404), (
        f"another account's project was served with {response.status_code}"
    )
    assert "pas-a-moi.fr" not in response.text
