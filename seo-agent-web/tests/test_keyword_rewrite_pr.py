"""The "Réécrire (PR)" button on the keywords page, walked as the customer walks it.

This endpoint is the one that closes the loop: Search Console says a page is seen on a query and
never clicked, and the answer is a pull request rewriting the two lines a searcher reads. It is
also the one that opens a pull request on a CUSTOMER's repository from a page that has no crawl
behind it, so what these tests defend is mostly what it REFUSES to do:

* a page on another host is not this repo's page;
* a repo whose route map cannot name the page's source file gets no PR at all, rather than a
  patch aimed at whatever an AI file picker would have proposed;
* a shared layout is never the target — a title written there becomes every page's title;
* the values are written by the model, so the PR says so, the correction is billed, and the
  project's "Full Access" mode does not merge it.

GitHub and the model are stubbed: what is under test is the decision-making, not the transport.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-kwpr-"))
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
from backend import auth, billing  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import IssueTask, Project, User  # noqa: E402

PAGE = "https://site.fr/guide-kling-ai-fr"
QUERY = "kling ai"

# A Next.js App Router page as a real one is written: the values are literals in the file, and a
# layout sits beside it holding the values that belong to every page.
PAGE_SOURCE = """export const metadata = {
  title: "Guide",
  description: "Un guide.",
};

export default function Page() { return <article>Kling AI</article>; }
"""

TREE = [
    "app/layout.tsx",
    "app/page.tsx",
    "app/guide-kling-ai-fr/page.tsx",
    "package.json",
    "next.config.js",
]


@pytest.fixture()
def customer(monkeypatch):
    """A non-admin account on a paid plan, with a repo connected, exactly like the real one."""
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
                       settings={"github_repo": "client/site.fr", "github_branch": "main",
                                 "github_mode": "review"})
        db.add(proj)
        db.commit()
        db.refresh(proj)
        pid = str(proj.id)
    # A Pro subscription: the gate is part of the path, and a free account must not reach GitHub.
    monkeypatch.setattr(app_module, "_plan_correction_cfg", lambda user: {
        "plan": "pro", "model": "claude-sonnet-4-6", "max_files": 20, "unlimited": False,
    })
    monkeypatch.setattr(app_module, "_effective_user_connection_value",
                        lambda **kw: ("ghp_test_token", "user"))
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug, pid, uid


@pytest.fixture()
def github(monkeypatch):
    """Stub the GitHub API and record every write, so the test can read what was committed."""
    calls: dict[str, list] = {"put": [], "post": [], "tree": list(TREE)}

    def _get(path, **kw):
        if "/git/trees/" in path:
            return {"tree": [{"path": p, "type": "blob"} for p in calls["tree"]]}
        if "/git/ref/" in path or "/git/refs/heads/" in path:
            return {"object": {"sha": "base-sha"}}
        if "/contents/" in path:
            import base64
            return {"content": base64.b64encode(PAGE_SOURCE.encode()).decode(), "sha": "file-sha"}
        raise AssertionError(f"unexpected GET {path}")

    def _post(path, **kw):
        calls["post"].append((path, kw.get("json_body") or {}))
        if path.endswith("/pulls"):
            return {"html_url": "https://github.com/client/site.fr/pull/42", "number": 42}
        return {"ok": True}

    def _put(path, **kw):
        import base64
        body = kw.get("json_body") or {}
        calls["put"].append((path, base64.b64decode(body.get("content", "")).decode()))
        return {"content": {"sha": "new-sha"}}

    monkeypatch.setattr(app_module, "_github_api_get", _get)
    monkeypatch.setattr(app_module, "_github_api_post", _post)
    monkeypatch.setattr(app_module, "_github_api_put", _put)
    monkeypatch.setattr(app_module, "_github_pr_is_open", lambda *a, **kw: True)
    return calls


@pytest.fixture()
def model(monkeypatch):
    """The model answers with a value; the code is what must place it and measure it."""
    seen: list[dict] = []

    def _ai(*, system, user_msg, **kw):
        seen.append({"system": system, "user": json.loads(user_msg), "model": kw.get("model_override")})
        field = "titre" if "titre" in system else "description"
        if field == "titre":
            return {"value": "Kling AI : le guide complet, prix et alternatives"}
        return {"value": "Tout sur Kling AI : ce que fait l'outil, ce qu'il coûte et quand il vaut mieux prendre autre chose."}

    monkeypatch.setattr(app_module, "_correction_ai_json", _ai)
    return seen


def _post(client, slug, **body):
    """A JSON POST as the page makes it: the CSRF token travels in the header, from the cookie."""
    client.get(f"/projects/{slug}/keywords/opportunities")
    token = client.cookies.get(app_module._CSRF_COOKIE_NAME, "")
    return client.post(f"/api/projects/{slug}/keywords/rewrite-pr", json=body,
                       headers={app_module._CSRF_HEADER_NAME: token})


# ── what it does ──────────────────────────────────────────────────────────────────────────────

def test_a_pull_request_rewrites_the_flagged_page_and_only_it(customer, github, model) -> None:
    client, slug, _pid, _uid = customer
    r = _post(client, slug, query=QUERY, url=PAGE)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] and data["pr_url"].endswith("/42")

    written = dict(github["put"])
    paths = [p.split("/contents/")[1] for p in written]
    assert paths == ["app/guide-kling-ai-fr/page.tsx"], "the page's own source, and nothing else"
    new = list(written.values())[0]
    assert "Kling AI : le guide complet" in new, "the title the model wrote is in the file"
    assert 'title: "Guide"' not in new, "the old value is gone, not duplicated"
    assert "export default function Page()" in new, "the rest of the file is untouched"


def test_the_layout_is_never_the_target(customer, github, model) -> None:
    """A title written into a shared layout becomes EVERY page's title.

    Measured honestly: today this holds because `route_files` answers with per-page sources only,
    not because of the `is_shared_path` filter next to it — reverting that filter leaves this test
    green. The filter stays as a second layer and this stays as the outcome check, so a future
    widening of the route map is caught here rather than in a customer's repository.
    """
    client, slug, _pid, _uid = customer
    _post(client, slug, query=QUERY, url=PAGE)
    assert not any("layout" in p for p, _ in github["put"]), "a shared file was patched"


def test_the_pr_says_the_model_wrote_it_and_is_never_auto_merged(customer, github, model) -> None:
    """The values are editorial. Badging them "correctif mécanique" would invite a blind merge,
    and auto-merging them would skip the human entirely."""
    client, slug, pid, _uid = customer
    with app_module.DB.session() as db:
        proj = db.get(Project, pid)
        proj.settings = {**proj.settings, "github_mode": "auto"}  # Full Access
        db.commit()
    _post(client, slug, query=QUERY, url=PAGE)
    pr_body = next(b["body"] for p, b in github["post"] if p.endswith("/pulls"))
    assert "rédigé par le **modèle**" in pr_body or "rédigé par le" in pr_body
    assert "Correctif mécanique" not in pr_body
    assert not any("/merge" in p for p, _ in github["put"]), "a model-written PR was auto-merged"


def test_the_correction_is_billed_once_per_file(customer, github, model) -> None:
    """One file written by the model, one correction — the unit the anomaly corrector uses. A
    bounded rewrite that still calls the model is not free to produce."""
    client, slug, _pid, uid = customer
    _post(client, slug, query=QUERY, url=PAGE)
    with app_module.DB.session() as db:
        used = billing.usage_sum(db, user_id=uid, metric="ai_corrections_month")
    assert used == 1, f"expected 1 correction billed, got {used}"


def test_the_plans_model_is_the_one_used(customer, github, model) -> None:
    client, slug, _pid, _uid = customer
    _post(client, slug, query=QUERY, url=PAGE)
    assert model and all(c["model"] == "claude-sonnet-4-6" for c in model)


def test_the_task_is_tracked_without_pretending_a_crawl_saw_it(customer, github, model) -> None:
    client, slug, pid, _uid = customer
    _post(client, slug, query=QUERY, url=PAGE)
    with app_module.DB.session() as db:
        task = db.scalar(select(IssueTask).where(IssueTask.project_id == pid))
    assert task.issue_key == app_module._KEYWORD_REWRITE_KEY
    assert task.url == PAGE and task.status == "in_progress"
    assert task.crawl_ts == "", "no crawl produced this correction"
    assert json.loads(task.note)["query"] == QUERY


# ── what it refuses ───────────────────────────────────────────────────────────────────────────

def test_a_page_on_another_host_is_refused(customer, github, model) -> None:
    """The URL arrives through the browser and this endpoint writes to a repository."""
    client, slug, _pid, _uid = customer
    r = _post(client, slug, query=QUERY, url="https://concurrent.fr/leur-page")
    assert r.status_code == 400
    assert not github["put"], "a foreign page reached the patcher"


def test_a_page_the_route_map_cannot_resolve_gets_no_pr(customer, github, model) -> None:
    """Measured, not assumed: hand this endpoint the corrector's ordinary resolution chain and it
    opens a PR anyway — because for an unknown key the hardcoded candidate list leads with
    `app/layout.tsx`, then `app/page.tsx`. Either would put one page's title somewhere it does
    not belong, and no crawl evidence exists here to catch it. So: the route map answers, or
    nothing happens."""
    client, slug, _pid, _uid = customer
    r = _post(client, slug, query=QUERY, url="https://site.fr/une-page-qui-n-existe-pas")
    assert r.status_code == 422
    assert "n'a pas pu être identifié" in r.json()["error"]
    assert not github["post"], "a branch was created for a fix that could not be made"


def test_a_free_account_is_sent_to_billing_before_any_repo_call(customer, github, monkeypatch) -> None:
    client, slug, _pid, _uid = customer
    monkeypatch.setattr(app_module, "_plan_correction_cfg", lambda user: {
        "plan": "free", "model": "", "max_files": 0, "unlimited": False})
    r = _post(client, slug, query=QUERY, url=PAGE)
    assert r.status_code == 402 and r.json()["billing_url"] == "/billing"
    assert not github["put"]


def test_a_second_click_while_a_pr_is_open_is_refused_with_its_link(customer, github, model) -> None:
    """Two queries can point at the same page; the second PR would touch the same two lines."""
    client, slug, _pid, _uid = customer
    assert _post(client, slug, query=QUERY, url=PAGE).status_code == 200
    r = _post(client, slug, query="kling ai avis", url=PAGE)
    assert r.status_code == 409
    assert r.json()["pr_url"].endswith("/42")


def test_a_page_whose_values_are_assembled_is_refused_not_guessed_at(customer, github, model, monkeypatch) -> None:
    """No AI fallback on this path: handing the whole file to a free-form patch is how a page's
    logic gets rewritten to satisfy a keyword."""
    import base64

    assembled = "export const metadata = { title: `${post.title} | Marque` };\n"

    def _get(path, **kw):
        if "/git/trees/" in path:
            return {"tree": [{"path": p, "type": "blob"} for p in TREE]}
        if "/git/ref" in path:
            return {"object": {"sha": "base-sha"}}
        return {"content": base64.b64encode(assembled.encode()).decode(), "sha": "file-sha"}

    monkeypatch.setattr(app_module, "_github_api_get", _get)
    client, slug, _pid, _uid = customer
    r = _post(client, slug, query=QUERY, url=PAGE)
    assert r.status_code == 422
    assert "assemblés" in r.json()["error"]
    assert not github["put"], "a value that is not written in the file was rewritten anyway"


def test_another_account_cannot_open_a_pr_on_this_project(customer, github, model) -> None:
    _client, slug, _pid, _uid = customer
    tag = uuid.uuid4().hex[:8]
    with app_module.DB.session() as db:
        other = User(email=f"autre-{tag}@exemple.fr",
                     password_hash=auth.hash_password("y" * 12), is_admin=False)
        db.add(other)
        db.commit()
        db.refresh(other)
        other_id = str(other.id)
    intruder = TestClient(app)
    intruder.cookies.set(auth.SESSION_COOKIE_NAME,
                         auth.make_session_token(user_id=other_id, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    r = _post(intruder, slug, query=QUERY, url=PAGE)
    assert r.status_code == 404
    assert not github["put"]


def test_a_crawl_never_declares_a_snippet_rewrite_verified(customer, github, model) -> None:
    """The post-crawl verification asks "does the fresh crawl still flag this?". No crawler emits
    the keyword key, so the answer would always be "no" — and the task would be badged
    "✓ Vérifié résolu" for a rewrite whose only real verdict is weeks of Search Console clicks.
    A confident wrong reading is worse than none.
    """
    client, slug, pid, _uid = customer
    _post(client, slug, query=QUERY, url=PAGE)
    with app_module.DB.session() as db:
        task = db.scalar(select(IssueTask).where(IssueTask.project_id == pid))
        task.status = "done"          # as it would be once the customer merged the PR
        db.commit()

    app_module._verify_corrections_after_crawl(slug, {
        "meta": {"timestamp": "20260830-101010"},
        "issues": {"title_too_long_indexable": {"count": 0, "pages": []}},
    })

    with app_module.DB.session() as db:
        note = json.loads(db.scalar(select(IssueTask).where(IssueTask.project_id == pid)).note)
    assert "verify" not in note, "a crawl claimed to have verified a keyword rewrite"
