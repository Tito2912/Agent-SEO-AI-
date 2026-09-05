"""One anomaly, nine stacks, three different right answers.

`served_html_lang_mismatch` is the first family where the correct fix is not the same shape
everywhere. Measured on the nine fixtures: every stack writes `<html lang>` in exactly ONE file,
and eight of them can make that file depend on the page. Only Next.js App Router cannot — its
root layout never receives the route — and even there a locale segment puts the source fix back
within reach.

That is why the post-build script exists, and why it must stay the exception: adding a build step
to a project that can fix itself at the source would be a worse correction, not a safer one. The
first pull request this family produced proved the other half of the lesson — it reached for
`headers()` in a static export and broke the customer's build.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402
from backend import repo_index  # noqa: E402

FIXTURES = WEB_ROOT / "tests" / "fixtures"


def _paths(stack: str) -> list[str]:
    root = FIXTURES / stack
    return [str(p.relative_to(root)).replace("\\", "/") for p in root.rglob("*")
            if p.is_file() and "node_modules" not in str(p)]


@pytest.mark.parametrize(
    "stack, expected",
    [
        # The attribute is in the page itself: no model, no tokens, no build step.
        ("static-html", "deterministic"),
        # Seven frameworks whose single lang-writing file CAN know the page.
        ("jekyll", "source"), ("hugo", "source"), ("astro", "source"),
        ("gatsby", "source"), ("nuxt", "source"), ("next-pages", "source"),
        ("sveltekit", "source"),
        # The one exception, and the only reason the post-build script exists.
        ("next-app", "postbuild"),
    ],
)
def test_each_stack_gets_the_fix_its_framework_allows(stack: str, expected: str) -> None:
    paths = _paths(stack)
    if not paths:  # pragma: no cover - fixtures ship with the tests
        pytest.skip(f"{stack} fixture missing")
    assert app_module._served_lang_strategy(paths) == expected


def test_a_locale_segment_puts_the_source_fix_back_within_reach() -> None:
    """`app/[lang]/layout.tsx` DOES receive the route. Sending that project to a build step
    would be gratuitous."""
    assert app_module._served_lang_strategy(
        ["app/[lang]/layout.tsx", "app/[lang]/page.tsx", "package.json", "next.config.mjs"]
    ) == "source"


def test_every_supported_stack_has_an_idiom() -> None:
    """The project's own rule, learned from Gatsby being handed Next.js's API: a stack gets its
    routes AND its idiom, or neither. A family that targets a file without saying how that file
    can know the page is the same mistake in a new place."""
    for stack in ("static-html", "jekyll", "hugo", "astro", "gatsby", "nuxt", "next-pages",
                  "sveltekit", "next-app"):
        idiom = app_module._SERVED_LANG_STACK_IDIOM.get(stack, "")
        assert idiom and len(idiom) > 40, f"{stack} has no idiom for this family"


def test_the_hint_carries_the_idiom_of_the_repository_it_is_for() -> None:
    prep = app_module._prepare_issue_fix(
        issue_key="served_html_lang_mismatch", issues={}, impacted=[],
        all_paths=_paths("jekyll"), site_name="site.fr",
        owner="o", repo_name="r", branch="main", token="t",
    )
    assert "page.lang" in prep["extra_hint"], "Jekyll's own idiom is missing from the hint"
    assert "hreflang" in prep["extra_hint"], "the base instruction was dropped"


# ── the hand-written case, where no model is needed at all ────────────────────────────────────

PAGE = ('<!doctype html>\n<html lang="en">\n<head>\n'
        '<link rel="canonical" href="https://site.fr/index-fr" />\n'
        '<link rel="alternate" hreflang="fr" href="https://site.fr/index-fr" />\n'
        '<link rel="alternate" hreflang="en" href="https://site.fr/" />\n'
        '<link rel="alternate" hreflang="x-default" href="https://site.fr/" />\n'
        '</head><body><a href="/" title="accueil">x</a></body></html>')


def test_the_page_says_its_own_language_and_the_rewrite_reads_it_back() -> None:
    fixed, count = app_module._rewrite_served_lang(PAGE)
    assert count == 1
    assert '<html lang="fr">' in fixed
    # Nothing else moves — including the hreflang, which is the side telling the truth.
    assert fixed.replace('<html lang="fr">', '<html lang="en">') == PAGE


def test_a_page_already_serving_its_own_language_is_untouched() -> None:
    assert app_module._rewrite_served_lang(PAGE.replace('lang="en"', 'lang="fr"'))[1] == 0


def test_a_region_variant_is_not_a_mismatch() -> None:
    page = PAGE.replace('lang="en"', 'lang="fr-CA"')
    assert app_module._rewrite_served_lang(page)[1] == 0


def test_a_page_that_declares_nothing_is_left_exactly_as_it_is() -> None:
    for silent in ('<html lang="en"><head></head></html>',
                   PAGE.replace('rel="canonical"', 'rel="preload"')):
        assert app_module._rewrite_served_lang(silent) == (silent, 0)


def test_x_default_is_never_taken_for_a_language() -> None:
    page = PAGE.replace('hreflang="fr" href="https://site.fr/index-fr"',
                        'hreflang="x-default" href="https://site.fr/index-fr"')
    assert app_module._rewrite_served_lang(page)[1] == 0


def test_the_hand_written_stack_is_fixed_without_a_model(monkeypatch) -> None:
    """`rewriter_is_ai` stays False here: no call, nothing billed, and the pull request may
    honestly carry the mechanical badge."""
    prep = app_module._prepare_issue_fix(
        issue_key="served_html_lang_mismatch", issues={}, impacted=[],
        all_paths=_paths("static-html"), site_name="site.fr",
        owner="o", repo_name="r", branch="main", token="t",
    )
    assert prep["link_rewriter"] is not None
    assert prep["rewriter_is_ai"] is False
    assert prep["rewriter_ai_fallback"] is False
    assert prep["link_rewriter"](PAGE)[1] == 1


def test_the_framework_stacks_get_no_deterministic_rewriter() -> None:
    """There is nothing mechanical to do in a template: what changes is the EXPRESSION that
    computes the language, which only the model can write."""
    prep = app_module._prepare_issue_fix(
        issue_key="served_html_lang_mismatch", issues={}, impacted=[],
        all_paths=_paths("hugo"), site_name="site.fr",
        owner="o", repo_name="r", branch="main", token="t",
    )
    assert prep["link_rewriter"] is None


def test_the_hand_written_stack_targets_its_pages_not_a_shared_file() -> None:
    """The exclusion that keeps this family off page targeting exists for templates. On a
    hand-written site it would leave the family with nothing to fix at all."""
    paths = ["index.html", "a-propos.html", "blog.html"]
    index = repo_index.build_repo_index(paths)
    targets = app_module._resolve_issue_targets(
        all_paths=paths, index=index, issue_key="served_html_lang_mismatch", issue_label="x",
        impacted_urls=["https://s.fr/a-propos"], located=[], max_files=8,
        ai_map=lambda: [], ai_pick=lambda: [],
    )
    assert "a-propos.html" in targets


# ── the guarantee that matters most: a build step only where it is unavoidable ─────────────────
#
# Everything above tests the DECISION. This tests that the endpoint acts on it — the revert that
# sends every stack to the post-build script passed all of the above, which is exactly the kind
# of hole a decision table can hide.

import base64  # noqa: E402
import json  # noqa: E402
import tempfile  # noqa: E402
import uuid  # noqa: E402

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-servedlang-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import Project, User  # noqa: E402

JEKYLL_LAYOUT = ('<!doctype html>\n<html lang="fr">\n<head>{{ page.title }}</head>\n'
                 "<body>{{ content }}</body></html>\n")


@pytest.fixture()
def jekyll_customer(monkeypatch):
    """A Pro customer whose repository is a Jekyll site — a stack that CAN fix itself."""
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    slug = f"jek-{tag}"
    with app_module.DB.session() as db:
        user = User(email=f"c-{tag}@exemple.fr", password_hash=auth.hash_password("x" * 12),
                    is_admin=False)
        db.add(user); db.commit(); db.refresh(user)
        uid = str(user.id)
        db.add(Project(owner_user_id=uid, slug=slug, site_name="site.fr",
                       base_url="https://site.fr/",
                       settings={"github_repo": "client/site.fr", "github_branch": "main",
                                 "github_mode": "review"}))
        db.commit()
    monkeypatch.setattr(app_module, "_plan_correction_cfg", lambda user: {
        "plan": "pro", "model": "claude-sonnet-4-6", "max_files": 20, "unlimited": False})
    monkeypatch.setattr(app_module, "_effective_user_connection_value",
                        lambda **kw: ("ghp_test", "user"))
    monkeypatch.setattr(app_module, "_open_pr_for_issue", lambda **kw: "")
    monkeypatch.setattr(app_module.dash, "list_project_crawls", lambda *a, **kw: ["20260904-101010"])
    monkeypatch.setattr(app_module.dash, "load_report_json", lambda *a, **kw: {
        "issues": {"served_html_lang_mismatch": {"count": 1, "pages": ["https://site.fr/a-propos"]}}})
    monkeypatch.setattr(app_module.dash, "extract_impacted_pages",
                        lambda key, block: {"https://site.fr/a-propos"})

    tree = ["_config.yml", "_layouts/default.html", "a-propos.html", "index.html"]
    written: dict[str, str] = {}

    def _get(path, **kw):
        if "/git/trees/" in path:
            return {"tree": [{"path": p, "type": "blob"} for p in tree]}
        if "/git/ref" in path:
            return {"object": {"sha": "base"}}
        return {"content": base64.b64encode(JEKYLL_LAYOUT.encode()).decode(), "sha": "sha"}

    def _put(path, **kw):
        body = kw.get("json_body") or {}
        written[path.split("/contents/")[1]] = base64.b64decode(body["content"]).decode()
        return {"content": {"sha": "new"}}

    monkeypatch.setattr(app_module, "_github_api_get", _get)
    monkeypatch.setattr(app_module, "_github_api_put", _put)
    monkeypatch.setattr(app_module, "_github_api_post", lambda path, **kw: (
        {"html_url": "https://github.com/client/site.fr/pull/1", "number": 1}
        if path.endswith("/pulls") else {"ok": True}))
    monkeypatch.setattr(app_module, "_correction_ai_json", lambda **kw: {
        "patched_content": JEKYLL_LAYOUT.replace('lang="fr"', 'lang="{{ page.lang }}"')})

    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug, written


def test_a_stack_that_can_fix_itself_never_gets_a_build_step(jekyll_customer) -> None:
    client, slug, written = jekyll_customer
    client.get(f"/projects/{slug}/issues")
    token = client.cookies.get(app_module._CSRF_COOKIE_NAME, "")
    resp = client.post(
        f"/api/projects/{slug}/issues/served_html_lang_mismatch/deep-fix",
        json={"url": "", "crawl_ts": "20260904-101010"},
        headers={app_module._CSRF_HEADER_NAME: token},
    )
    assert resp.status_code == 200, resp.text
    assert "scripts/fix-html-lang.mjs" not in written, (
        "a Jekyll site was given a post-build script it does not need")
    assert "package.json" not in written, "its build was chained onto for nothing"
    assert any(p.endswith("default.html") for p in written), written


@pytest.fixture()
def next_app_customer(jekyll_customer, monkeypatch):
    """The same customer, on the one stack that has no source fix — so the build step is real."""
    client, slug, written = jekyll_customer
    tree = ["package.json", "app/layout.tsx", "app/page.tsx", "next.config.js"]

    def _get(path, **kw):
        if "/git/trees/" in path:
            return {"tree": [{"path": q, "type": "blob"} for q in tree]}
        if "/git/ref" in path:
            return {"object": {"sha": "base"}}
        if path.endswith("/package.json"):
            return {"content": base64.b64encode(
                json.dumps({"name": "s", "scripts": {"build": "next build"}}).encode()).decode(),
                "sha": "pkg"}
        return {"content": base64.b64encode(b"<html lang=\"fr\"></html>").decode(), "sha": "sha"}

    monkeypatch.setattr(app_module, "_github_api_get", _get)
    bodies: list[str] = []

    def _post(path, **kw):
        if path.endswith("/pulls"):
            bodies.append((kw.get("json_body") or {}).get("body", ""))
            return {"html_url": "https://github.com/client/site.fr/pull/9", "number": 9}
        return {"ok": True}

    monkeypatch.setattr(app_module, "_github_api_post", _post)
    return client, slug, written, bodies


def test_the_pull_request_names_the_anomaly_it_actually_fixed(next_app_customer) -> None:
    """It went out wrong once, on a real customer repository: the post-build language fix was
    announced under the heading "Correction config (boucle de redirection)" — the title of the
    only family that used to write there. A correction the reader mistrusts is a correction that
    does not get merged."""
    client, slug, written, bodies = next_app_customer
    client.get(f"/projects/{slug}/issues")
    token = client.cookies.get(app_module._CSRF_COOKIE_NAME, "")
    resp = client.post(
        f"/api/projects/{slug}/issues/served_html_lang_mismatch/deep-fix",
        json={"url": "", "crawl_ts": "20260904-101010"},
        headers={app_module._CSRF_HEADER_NAME: token},
    )
    assert resp.status_code == 200, resp.text
    assert "scripts/fix-html-lang.mjs" in written, "this stack does need the build step"
    assert bodies, "no pull request was opened"
    body = bodies[0]
    assert "langue du html servi" in body.lower(), body
    assert "boucle de redirection" not in body, (
        "the language fix is announced as a redirect fix")
