"""The competitors screen, in the states a customer actually passes through.

The engine (`backend/competitors.py`) was validated on two live sites; nothing showed it. These
drive the real handlers for a real non-admin account, because the states that matter are the
ones a customer hits and they lead to different actions:

  * a plan below Pro — the owner's gating decision, one step above the backlink Opportunités;
  * Pro, no rival added;
  * a rival added but never analysed (which is NOT "analysed and nothing found");
  * a rival analysed, but no crawl of the customer's OWN site to compare against;
  * both sides present — covered subjects get a retarget button, uncovered ones never do.

The rival crawl itself is not run here: it is a subprocess against somebody else's site. What is
tested is everything around it — who may ask for it, what is queued, and what the page says.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-competitors-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

import re  # noqa: E402

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import CompetitorSite, JobRecord, Project, User  # noqa: E402

# Two real shapes, from the pair the engine was validated on: a rival guide and the page here
# that answers the same subject, plus a rival subject nothing here covers.
RIVAL_PAGES = [
    {"url": "https://rival.fr/blog/kling-ai-prix-2026", "status_code": 200,
     "title": "Kling AI prix 2026 : crédits, plans et coût réel", "h1": ["Kling AI prix 2026"]},
    {"url": "https://rival.fr/blog/heygen-avatars-2026", "status_code": 200,
     "title": "HeyGen avatars 2026 : test complet", "h1": ["HeyGen avatars 2026"]},
]
OWN_PAGES = [
    {"url": "https://site.fr/blog/kling-ai-prix-2026", "status_code": 200,
     "title": "Kling AI prix 2026 : crédits et plans", "h1": ["Kling AI prix 2026"]},
    {"url": "https://site.fr/", "status_code": 200, "title": "Accueil", "h1": ["Accueil"]},
]


@pytest.fixture()
def customer(monkeypatch):
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
                       base_url="https://site.fr/",
                       settings={"github_repo": "client/site.fr", "github_branch": "main"})
        db.add(proj)
        db.commit()
        db.refresh(proj)
        pid = str(proj.id)
    plans = {"key": "pro"}
    monkeypatch.setattr(app_module.billing, "effective_plan_key", lambda db, **kw: plans["key"])
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug, pid, uid, plans


@pytest.fixture()
def own_crawl(monkeypatch):
    """The customer's own pages, as the latest crawl report would give them."""
    def _install(pages):
        monkeypatch.setattr(app_module.dash, "list_project_crawls", lambda *a, **kw: ["20260829-101010"])
        monkeypatch.setattr(app_module.dash, "load_report_json", lambda *a, **kw: {"pages": pages})
    return _install


def _csrf_post(client, path, data, form_page):
    html = client.get(form_page).text
    token = re.search(r'name="_csrf"\s+value="([^"]*)"', html)
    assert token, "no CSRF token on the page"
    return client.post(path, data={**data, "_csrf": token.group(1)}, follow_redirects=False)


def _add_ready_rival(pid, uid, pages=RIVAL_PAGES, *, status="ready", domain="rival.fr"):
    from datetime import datetime, timezone
    with app_module.DB.session() as db:
        row = CompetitorSite(project_id=pid, user_id=uid, domain=domain,
                             base_url=f"https://{domain}", status=status,
                             pages=pages, pages_count=len(pages),
                             last_crawled_at=datetime.now(timezone.utc) if status == "ready" else None)
        db.add(row)
        db.commit()
        db.refresh(row)
        return str(row.id)


# ── who may open it ───────────────────────────────────────────────────────────────────────────

def test_a_solo_account_is_told_the_feature_is_pro(customer) -> None:
    """One step above the backlink Opportunités gate, deliberately: a rival crawl spends worker
    time on a site that is not the customer's."""
    client, slug, _pid, _uid, plans = customer
    plans["key"] = "solo"
    body = client.get(f"/projects/{slug}/competitors").text
    assert "Réservé aux plans Pro" in body
    assert "Ajouter un concurrent" not in body


def test_a_solo_account_cannot_add_a_rival_through_the_endpoint(customer) -> None:
    client, slug, pid, _uid, plans = customer
    page = f"/projects/{slug}/competitors"
    client.get(page)          # the session's CSRF cookie, before the plan drops
    plans["key"] = "solo"
    resp = client.post(f"{page}/add",
                       data={"url": "rival.fr",
                             "_csrf": client.cookies.get(app_module._CSRF_COOKIE_NAME, "")},
                       follow_redirects=False)
    assert resp.status_code == 303
    with app_module.DB.session() as db:
        assert db.scalars(select(CompetitorSite).where(CompetitorSite.project_id == pid)).all() == []


# ── the empty states, which are three different answers ───────────────────────────────────────

def test_a_pro_account_with_no_rival_says_what_to_do(customer) -> None:
    client, slug, _pid, _uid, _plans = customer
    body = client.get(f"/projects/{slug}/competitors").text
    assert "Aucun concurrent suivi" in body
    assert "sans acheter de" in body, "the page must say the data is not bought from a vendor"


def test_a_rival_never_analysed_is_not_a_rival_with_nothing_to_say(customer) -> None:
    client, slug, pid, uid, _plans = customer
    _add_ready_rival(pid, uid, pages=[], status="new")
    body = client.get(f"/projects/{slug}/competitors").text
    assert "Jamais analysé" in body
    assert "Aucun sujet exploitable" not in body


def test_without_a_crawl_of_your_own_site_the_comparison_says_so(customer, monkeypatch) -> None:
    """The comparison needs both sides. Showing an empty subject table would read as "your
    rivals publish nothing"."""
    client, slug, pid, uid, _plans = customer
    monkeypatch.setattr(app_module.dash, "list_project_crawls", lambda *a, **kw: [])
    _add_ready_rival(pid, uid)
    body = client.get(f"/projects/{slug}/competitors").text
    assert "Il manque ton propre crawl" in body


# ── the comparison ────────────────────────────────────────────────────────────────────────────

def test_a_covered_subject_names_the_page_and_offers_the_retarget(customer, own_crawl) -> None:
    client, slug, pid, uid, _plans = customer
    own_crawl(OWN_PAGES)
    _add_ready_rival(pid, uid)
    body = client.get(f"/projects/{slug}/competitors").text
    assert "kling-ai-prix-2026" in body
    assert "data-keyword-pr" in body, "a covered subject must offer the rewrite"
    assert 'data-url="https://site.fr/blog/kling-ai-prix-2026"' in body


def test_an_uncovered_subject_is_reported_and_never_actionable(customer, own_crawl) -> None:
    """Putting a keyword into a page that does not cover the subject is stuffing, and a
    corrector that does it makes the site worse. Owner's rule: retarget first, create later."""
    client, slug, pid, uid, _plans = customer
    own_crawl(OWN_PAGES)
    _add_ready_rival(pid, uid)
    body = client.get(f"/projects/{slug}/competitors").text
    assert "Aucune page sur ce sujet" in body
    heygen_row = body.split("HeyGen")[1].split("</tr>")[0]
    assert "data-keyword-pr" not in heygen_row, "an uncovered subject was made actionable"


def test_the_home_page_is_never_offered_as_the_page_to_retarget(customer, own_crawl) -> None:
    """It borrows a little of every topic: on the real pair it matched five rival articles at
    exactly the floor."""
    client, slug, pid, uid, _plans = customer
    own_crawl(OWN_PAGES)
    _add_ready_rival(pid, uid)
    body = client.get(f"/projects/{slug}/competitors").text
    assert 'data-url="https://site.fr/"' not in body


# ── adding, refusing, queueing ────────────────────────────────────────────────────────────────

def test_a_rival_is_added_by_domain_however_it_is_typed(customer) -> None:
    client, slug, pid, _uid, _plans = customer
    page = f"/projects/{slug}/competitors"
    assert _csrf_post(client, f"{page}/add", {"url": "https://www.rival.fr/blog"}, page).status_code == 303
    with app_module.DB.session() as db:
        row = db.scalar(select(CompetitorSite).where(CompetitorSite.project_id == pid))
    assert row.domain == "rival.fr", "www and the path must not create a second entry"


def test_your_own_site_is_refused_as_a_competitor(customer) -> None:
    client, slug, pid, _uid, _plans = customer
    page = f"/projects/{slug}/competitors"
    resp = _csrf_post(client, f"{page}/add", {"url": "https://site.fr"}, page)
    assert resp.status_code == 303 and "propre+site" in resp.headers["location"].replace("%20", "+")
    with app_module.DB.session() as db:
        assert db.scalars(select(CompetitorSite).where(CompetitorSite.project_id == pid)).all() == []


def test_a_private_host_is_refused_like_any_crawl_target(customer) -> None:
    client, slug, pid, _uid, _plans = customer
    page = f"/projects/{slug}/competitors"
    _csrf_post(client, f"{page}/add", {"url": "http://127.0.0.1:8000"}, page)
    with app_module.DB.session() as db:
        assert db.scalars(select(CompetitorSite).where(CompetitorSite.project_id == pid)).all() == []


def test_the_same_domain_twice_is_not_an_error_and_not_a_duplicate(customer) -> None:
    client, slug, pid, _uid, _plans = customer
    page = f"/projects/{slug}/competitors"
    _csrf_post(client, f"{page}/add", {"url": "rival.fr"}, page)
    assert _csrf_post(client, f"{page}/add", {"url": "www.rival.fr"}, page).status_code == 303
    with app_module.DB.session() as db:
        rows = db.scalars(select(CompetitorSite).where(CompetitorSite.project_id == pid)).all()
    assert len(rows) == 1


def test_the_number_of_rivals_is_bounded_because_each_one_is_a_crawl(customer) -> None:
    client, slug, pid, _uid, _plans = customer
    page = f"/projects/{slug}/competitors"
    for i in range(app_module._COMPETITOR_MAX_PER_PROJECT + 2):
        _csrf_post(client, f"{page}/add", {"url": f"rival{i}.fr"}, page)
    with app_module.DB.session() as db:
        rows = db.scalars(select(CompetitorSite).where(CompetitorSite.project_id == pid)).all()
    assert len(rows) == app_module._COMPETITOR_MAX_PER_PROJECT


def test_analysing_queues_one_bounded_crawl(customer) -> None:
    client, slug, pid, uid, _plans = customer
    cid = _add_ready_rival(pid, uid, pages=[], status="new")
    page = f"/projects/{slug}/competitors"
    assert _csrf_post(client, f"{page}/analyze", {"competitor_id": cid}, page).status_code == 303
    with app_module.DB.session() as db:
        job = db.scalars(select(JobRecord)).all()[-1]
        row = db.get(CompetitorSite, cid)
        status = row.status
    assert job.result["type"] == "competitor" and job.result["competitor_id"] == cid
    assert status == "crawling", "the page must show the crawl is running"


def test_a_second_analyse_while_one_runs_is_refused(customer) -> None:
    """A queued crawl the customer cannot see is a button they press again."""
    client, slug, pid, uid, _plans = customer
    cid = _add_ready_rival(pid, uid, pages=[], status="crawling")
    page = f"/projects/{slug}/competitors"
    before = _job_count()
    _csrf_post(client, f"{page}/analyze", {"competitor_id": cid}, page)
    assert _job_count() == before


def test_another_account_cannot_remove_a_rival_by_its_id(customer) -> None:
    client, slug, pid, uid, _plans = customer
    cid = _add_ready_rival(pid, uid)
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        other = User(email=f"autre-{tag}@exemple.fr",
                     password_hash=auth.hash_password("y" * 12), is_admin=False)
        db.add(other)
        db.commit()
        db.refresh(other)
        other_id = str(other.id)
        foreign = f"pas-a-moi-{tag}"
        db.add(Project(owner_user_id=other_id, slug=foreign, site_name="pas-a-moi.fr",
                       base_url="https://pas-a-moi.fr/"))
        db.commit()
    intruder = TestClient(app)
    intruder.cookies.set(auth.SESSION_COOKIE_NAME,
                         auth.make_session_token(user_id=other_id, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    intruder.get(f"/projects/{foreign}/competitors")
    intruder.post(f"/projects/{foreign}/competitors/remove",
                  data={"competitor_id": cid,
                        "_csrf": intruder.cookies.get(app_module._CSRF_COOKIE_NAME, "")},
                  follow_redirects=False)
    with app_module.DB.session() as db:
        assert db.get(CompetitorSite, cid) is not None, "another account deleted this row"


def _job_count() -> int:
    with app_module.DB.session() as db:
        return len(db.scalars(select(JobRecord)).all())


# ── the monthly refresh ───────────────────────────────────────────────────────────────────────

def test_the_monthly_refresh_only_touches_what_has_gone_stale(customer) -> None:
    from datetime import datetime, timedelta, timezone

    client, slug, pid, uid, _plans = customer
    fresh = _add_ready_rival(pid, uid)
    stale = _add_ready_rival(pid, uid, domain="vieux-rival.fr")
    with app_module.DB.session() as db:
        db.get(CompetitorSite, stale).last_crawled_at = datetime.now(timezone.utc) - timedelta(
            days=app_module._COMPETITOR_REFRESH_DAYS + 1)
        db.commit()

    resp = client.post("/cron/refresh-competitors",
                       headers={"Authorization": "Bearer test-cron-secret"})
    assert resp.status_code == 200
    # Membership, not equality: this cron is global by design, and the tests above leave their
    # own never-crawled rivals behind — which are legitimately stale.
    assert "vieux-rival.fr" in resp.json()["domains"]
    with app_module.DB.session() as db:
        assert db.get(CompetitorSite, fresh).status == "ready", "a fresh rival was re-crawled"
        assert db.get(CompetitorSite, stale).status == "crawling"


def test_the_refresh_refuses_an_unsigned_call(customer) -> None:
    client, _slug, _pid, _uid, _plans = customer
    assert client.post("/cron/refresh-competitors").status_code == 401


def test_a_downgraded_account_stops_being_refreshed(customer) -> None:
    """The crawl costs worker time either way, and it would feed a page the customer can no
    longer open."""
    from datetime import datetime, timedelta, timezone

    client, _slug, pid, uid, plans = customer
    cid = _add_ready_rival(pid, uid)
    with app_module.DB.session() as db:
        db.get(CompetitorSite, cid).last_crawled_at = datetime.now(timezone.utc) - timedelta(days=90)
        db.commit()
    plans["key"] = "solo"
    resp = client.post("/cron/refresh-competitors",
                       headers={"Authorization": "Bearer test-cron-secret"})
    assert resp.json()["queued"] == 0


# ── what the crawl keeps ──────────────────────────────────────────────────────────────────────

def test_only_the_fields_the_engine_reads_are_stored() -> None:
    """A rival's report describes a site we are not auditing: keeping its anomalies would store
    findings nobody will ever act on, about somebody else's site.

    `lang` earns its place — the first real run paired a German rival page with an English page
    here, and the language is what refuses that. Everything else stays out.
    """
    report = {"pages": [
        {"url": "https://rival.fr/a", "title": "A", "h1": ["A"], "status_code": 200, "lang": "de",
         "meta_description": "…", "internal_links": [1, 2, 3], "canonical": "…"},
        {"url": "https://rival.fr/dead", "error": "timeout"},
    ]}
    kept = app_module._competitor_pages_from_report(report)
    assert kept == [{"url": "https://rival.fr/a", "title": "A", "h1": ["A"], "lang": "de",
                     "status_code": 200}]


def test_a_refusal_is_actually_shown_to_the_customer(customer) -> None:
    """The refusals redirect with a message in the query string, which the handler must accept
    and the template must render. Miss either and the page redraws saying nothing — the customer
    reads that as "the button is broken"."""
    client, slug, _pid, _uid, _plans = customer
    page = f"/projects/{slug}/competitors"
    resp = _csrf_post(client, f"{page}/add", {"url": "https://site.fr"}, page)
    assert resp.status_code == 303
    body = client.get(resp.headers["location"]).text
    # Assert on text that survives HTML escaping: Jinja writes the apostrophe as &#39;, so
    # asserting on "C'est ton propre site" fails against a page that says exactly that.
    assert "ton propre site" in body and 'class="alert error"' in body
