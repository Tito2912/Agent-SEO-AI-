"""Real transactions, fake GitHub HTTP; no messages, AI or public writes in this suite."""

import base64
import copy
from datetime import timedelta
import json
from urllib.parse import unquote
import uuid

import pytest
from requests.structures import CaseInsensitiveDict
from bs4 import BeautifulSoup

from backend import app as m, own_blog as b, own_blog_content as c, own_writing as w
from backend import telegram_approval_adapters as adapters
from backend.models import OwnSeoDraft, OwnSeoWorkspace
from test_own_writing import writing as writing, authorize, output
from test_own_seo import own as own, NOW
from test_customer_activation import lab as lab, client, user
from test_telegram_approvals import bot as bot

REAL_CONNECTION = b.connection


class Response:
    def __init__(self, data=None, status=200, headers=None, raw=None):
        self.data, self.status_code, self.headers, self.raw = (
            data,
            status,
            CaseInsensitiveDict(headers or {}),
            raw,
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def iter_content(self, size):
        yield self.raw if self.raw is not None else json.dumps(self.data).encode()


class Remote:
    def __init__(self):
        self.base = "a" * 40
        self.branches = {"main": self.base}
        self.commits = {self.base: {"parents": [], "files": {}}}
        self.writes = []
        self.reads = []
        self.pr = None
        self.pending = False
        self.failure = False
        self.timeout = None
        self.after = False
        self.omit_step = None
        self.run_id = 20
        self.extra = False
        self.deny = False
        self.request_hook = None
        self.protection = None
        self.rules = []

    def commit(self, parent, files, other=None):
        sha = str(len(self.commits) + 1).zfill(40)
        self.commits[sha] = {
            "parents": [parent] + ([other] if other else []),
            "files": copy.deepcopy(files),
        }
        return sha

    def ancestor(self, base, head):
        return base == head or any(
            self.ancestor(base, p) for p in self.commits[head]["parents"]
        )

    def pr_value(self):
        if not self.pr:
            return None
        return {
            **self.pr,
            "head": {
                "repo": {"id": c.REPO_ID},
                "ref": self.pr["branch"],
                "sha": self.branches[self.pr["branch"]],
            },
            "base": {"repo": {"id": c.REPO_ID}, "ref": "main"},
            "node_id": "PR_local",
        }

    def request(self, method, url, **kw):
        assert url.startswith("https://api.github.com/")
        assert kw["allow_redirects"] is False and kw["stream"] is True
        assert kw["headers"]["Authorization"] == "Bearer local-github"
        path = unquote(url.removeprefix("https://api.github.com"))
        path = path.removeprefix("/repos/" + c.REPO)
        params, body = kw.get("params") or {}, kw.get("json") or {}
        if method == "GET":
            self.reads.append((path, params))
            return self.get(path, params)
        self.writes.append((method, path, body))
        if self.request_hook:
            self.request_hook(method, path, body)
        if self.deny:
            return Response({}, 403)
        if self.timeout == path and not self.after:
            raise TimeoutError("provider secret must not be logged")
        result = self.mutate(method, path, body)
        if self.timeout == path and self.after:
            raise TimeoutError("lost response")
        return result

    def get(self, path, params):
        if path == "":
            return Response(
                {
                    "id": c.REPO_ID,
                    "full_name": c.REPO,
                    "default_branch": "main",
                    "permissions": {"push": True},
                    "allow_merge_commit": True,
                }
            )
        if path.startswith("/git/ref/heads/"):
            branch = path.removeprefix("/git/ref/heads/")
            return (
                Response(
                    {
                        "ref": "refs/heads/" + branch,
                        "object": {"type": "commit", "sha": self.branches[branch]},
                    }
                )
                if branch in self.branches
                else Response({}, 404)
            )
        if path.startswith("/contents/"):
            file = path.removeprefix("/contents/")
            ref = params["ref"]
            head = self.branches.get(ref, ref)
            value = self.commits[head]["files"].get(file)
            return (
                Response(
                    {
                        "type": "file",
                        "sha": c.blob(value),
                        "encoding": "base64",
                        "content": base64.b64encode(value.encode()).decode(),
                    }
                )
                if value is not None
                else Response({}, 404)
            )
        if path.startswith("/git/commits/"):
            head = path.split("/")[-1]
            return Response(
                {
                    "sha": head,
                    "parents": [{"sha": p} for p in self.commits[head]["parents"]],
                }
            )
        if path.startswith("/compare/"):
            base, head = path.removeprefix("/compare/").split("...")
            files = [
                {
                    "filename": k,
                    "sha": c.blob(v),
                    "status": "added"
                    if k not in self.commits[base]["files"]
                    else "modified",
                }
                for k, v in self.commits[head]["files"].items()
                if self.commits[base]["files"].get(k) != v
            ]
            if self.extra:
                files.append(
                    {
                        "filename": ".github/workflows/ci.yml",
                        "sha": "f" * 40,
                        "status": "modified",
                    }
                )
            return Response(
                {
                    "merge_base_commit": {
                        "sha": base if self.ancestor(base, head) else "0" * 40
                    },
                    "files": files,
                }
            )
        if path == "/pulls":
            return Response([self.pr_value()] if self.pr else [])
        if path == "/pulls/9":
            return Response(self.pr_value())
        if path == "/branches/main/protection":
            return (
                Response(self.protection)
                if self.protection is not None
                else Response({}, 404)
            )
        if path == "/rules/branches/main":
            return Response(self.rules)
        head = params.get("head_sha") or self.branches[self.pr["branch"]]
        if path.startswith("/actions/workflows/"):
            return Response(
                {
                    "total_count": 1,
                    "workflow_runs": [
                        {
                            "id": self.run_id,
                            "head_sha": head,
                            "event": "pull_request",
                            "workflow_id": 286291890,
                            "path": ".github/workflows/ci.yml",
                            "pull_requests": [{"number": 9}],
                            "status": "in_progress" if self.pending else "completed",
                            "conclusion": "failure" if self.failure else "success",
                        }
                    ],
                }
            )
        if path.startswith("/actions/runs/"):
            return Response(
                {
                    "total_count": 1,
                    "jobs": [
                        {
                            "name": "checks",
                            "head_sha": head,
                            "conclusion": "success",
                            "steps": [
                                {
                                    "name": n,
                                    "conclusion": "skipped"
                                    if n == self.omit_step
                                    else "success",
                                }
                                for n in (
                                    "Compile Python",
                                    "Ruff",
                                    "Tests",
                                    "Dependency audit",
                                    "Bandit medium/high",
                                )
                            ],
                        }
                    ],
                }
            )
        if path.endswith("/check-runs"):
            return Response(
                {
                    "total_count": 1,
                    "check_runs": [
                        {
                            "head_sha": head,
                            "name": "checks",
                            "status": "completed",
                            "conclusion": "success",
                        }
                    ],
                }
            )
        if path.endswith("/status"):
            return Response({"total_count": 0, "statuses": []})
        raise AssertionError((path, params))

    def mutate(self, method, path, body):
        if path == "/git/refs":
            self.branches[body["ref"].removeprefix("refs/heads/")] = body["sha"]
            return Response({"object": {"sha": body["sha"]}}, 201)
        if path.startswith("/contents/"):
            assert method == "PUT" and "sha" not in body
            file = path.removeprefix("/contents/")
            branch = body["branch"]
            parent = self.branches[branch]
            assert file not in self.commits[parent]["files"]
            files = {
                **self.commits[parent]["files"],
                file: base64.b64decode(body["content"]).decode(),
            }
            self.branches[branch] = head = self.commit(parent, files)
            return Response({"commit": {"sha": head}}, 201)
        if path == "/pulls":
            assert body["draft"] is True and body["base"] == "main"
            self.pr = {
                **body,
                "number": 9,
                "branch": body["head"],
                "state": "open",
                "merged": False,
            }
            return Response(self.pr_value(), 201)
        if path == "/graphql":
            self.pr["draft"] = False
            return Response(
                {
                    "data": {
                        "markPullRequestReadyForReview": {
                            "pullRequest": {"isDraft": False}
                        }
                    }
                }
            )
        if path == "/pulls/9/merge":
            assert body["sha"] == self.branches[self.pr["branch"]]
            head = body["sha"]
            base = self.branches["main"]
            merged = self.commit(base, self.commits[head]["files"], head)
            self.branches["main"] = merged
            self.pr.update(merged=True, state="closed", merge_commit_sha=merged)
            return Response({"merged": True, "sha": merged})
        if path == "/merges":
            assert body["base"] != "main"
            branch = body["base"]
            old = self.branches[branch]
            self.branches[branch] = head = self.commit(
                old, self.commits[old]["files"], body["head"]
            )
            return Response({"sha": head}, 201)
        raise AssertionError((method, path))


@pytest.fixture()
def blog(writing, monkeypatch):
    x = writing
    draft = authorize(x)
    w.run(draft)
    with x["db"].session() as db:
        row = db.get(OwnSeoDraft, draft)
        fingerprint = row.content_hash
    w.review(x["owner"].id, draft, fingerprint, True)
    remote = Remote()
    monkeypatch.setattr(
        b,
        "connection",
        lambda owner, **kw: ("local-github", b.s.digest(["own-blog", "local-github"])),
    )
    monkeypatch.setattr(b.requests, "request", remote.request)
    return {**x, "draft": draft, "remote": remote}


def proposal(x):
    ident = b.prepare(x["owner"].id, x["draft"], None, uuid.uuid4().hex, True)
    b.refresh(x["owner"].id, ident)
    assert row(x, ident).state == "proposal"
    return ident


def row(x, ident):
    with x["db"].session() as db:
        return db.get(b.Publication, ident)


def decide(x, ident, kind, approve=True):
    with x["db"].session() as db:
        b.s.locked(db)
        doc = adapters.inspect(db, x["owner"].id, kind, ident, "fr")
        result = adapters.apply(
            db, x["owner"].id, kind, ident, approve, uuid.uuid4().hex, doc["version"]
        )
        db.commit()
        return result


def reviewed_pr(x):
    ident = proposal(x)
    decide(x, ident, "blog_pr")
    b.run(ident)
    b.run(ident)
    assert row(x, ident).state == "review_ready"
    return ident


def test_proposal_and_get_are_read_only_and_duplicate_submission_is_one_row(blog):
    x = blog
    ident = proposal(x)
    assert b.prepare(x["owner"].id, x["draft"], None, uuid.uuid4().hex, True) == ident
    calls = len(x["remote"].reads)
    response = client(x["owner"]).get(b.PATH)
    assert response.status_code == 200 and "deux accords" in response.text
    assert len(x["remote"].reads) == calls and not x["remote"].writes
    assert len(x["calls"]) == 1  # Only the fixture's already-authorized draft request.


def test_two_distinct_decisions_merge_exact_files_and_observe_public_release(
    blog, monkeypatch, tmp_path
):
    x = blog
    ident = proposal(x)
    with pytest.raises(ValueError):
        decide(x, ident, "blog_publish")
    decide(x, ident, "blog_pr")
    b.run(ident)
    assert row(x, ident).state == "reviewing" and len(x["remote"].writes) == 3
    b.run(ident)
    assert row(x, ident).state == "review_ready"
    for _ in range(2):
        b.run(ident)
    assert len(x["remote"].writes) == 3
    decide(x, ident, "blog_publish")
    b.run(ident)
    result = row(x, ident)
    assert (
        result.state == "observing"
        and result.receipt["merge"] == x["remote"].branches["main"]
    )
    assert len(x["remote"].writes) == 5
    file = result.proposal["files"][0]
    public = served_article(file, result.receipt["merge"], monkeypatch, tmp_path)
    monkeypatch.setattr(
        b.requests,
        "get",
        lambda *a, **kw: Response(headers=dict(public.headers), raw=public.content),
    )
    b.run(ident)
    assert row(x, ident).state == "published"
    b.run(ident)
    assert len(x["remote"].writes) == 5


def served_article(file, release, monkeypatch, tmp_path):
    """Exercise the real public route and template, not a mirrored HTML fixture."""
    path = tmp_path / "article.md"
    path.write_text(file["text"], encoding="utf-8")
    article = m.content_library._load_page(path, collection="blog", lang=file["lang"])
    original = m.content_library.get_article
    monkeypatch.setattr(
        m.content_library,
        "get_article",
        lambda slug, lang="fr": (
            article
            if slug == article["slug"] and lang == file["lang"]
            else original(slug, lang)
        ),
    )
    monkeypatch.setenv("RENDER_GIT_COMMIT", release)
    monkeypatch.setenv("PUBLIC_BASE_URL", "https://noyaru.com")
    return client().get("/ressources-seo/" + article["slug"] + "?lang=" + file["lang"])


@pytest.mark.parametrize("stage", ["blog_pr", "blog_publish"])
def test_refusal_never_performs_the_corresponding_write(blog, stage):
    x = blog
    ident = proposal(x) if stage == "blog_pr" else reviewed_pr(x)
    before = len(x["remote"].writes)
    decide(x, ident, stage, False)
    b.run(ident)
    assert row(x, ident).state == "cancelled" and len(x["remote"].writes) == before


@pytest.mark.parametrize(
    "change", ["paused", "draft", "source", "connection", "expiry", "path"]
)
def test_changes_after_approval_stop_before_any_github_write(blog, monkeypatch, change):
    x = blog
    ident = proposal(x)
    decide(x, ident, "blog_pr")
    with x["db"].session() as db:
        item = db.get(b.Publication, ident)
        if change == "paused":
            db.get(OwnSeoWorkspace, b.s.KEY).paused = True
        if change == "draft":
            db.get(OwnSeoDraft, x["draft"]).content_hash = "f" * 64
        if change == "expiry":
            item.expires_at = NOW - timedelta(seconds=1)
        if change == "path":
            value = copy.deepcopy(item.proposal)
            value["files"][0]["path"] = ".github/workflows/ci.yml"
            item.proposal = value
        db.commit()
    if change == "connection":
        monkeypatch.setattr(
            b,
            "connection",
            lambda owner, **kw: ("changed", b.s.digest(["own-blog", "changed"])),
        )
    if change == "source":
        from backend import seo_watch

        with x["db"].session() as db:
            seo_watch.withdraw(db, "google_essentials", actor=x["owner"])
            db.commit()
    b.run(ident)
    assert not x["remote"].writes


@pytest.mark.parametrize(
    "pending,failure,omitted",
    [(True, False, None), (False, True, None), (False, False, "Tests")],
)
def test_incomplete_failed_or_skipped_ci_never_offers_publication(
    blog, pending, failure, omitted
):
    x = blog
    ident = proposal(x)
    decide(x, ident, "blog_pr")
    b.run(ident)
    x["remote"].pending = pending
    x["remote"].failure = failure
    x["remote"].omit_step = omitted
    b.run(ident)
    assert row(x, ident).state == "reviewing"
    with pytest.raises(ValueError):
        decide(x, ident, "blog_publish")
    assert len(x["remote"].writes) == 3


def test_unreviewed_change_in_branch_blocks_merge(blog):
    x = blog
    ident = reviewed_pr(x)
    decide(x, ident, "blog_publish")
    x["remote"].extra = True
    b.run(ident)
    assert row(x, ident).state == "needs_review" and len(x["remote"].writes) == 3


def test_main_advance_needs_fresh_prepare_approval_syncs_only_owned_branch(blog):
    x = blog
    ident = reviewed_pr(x)
    x["remote"].branches["main"] = x["remote"].commit(x["remote"].base, {})
    decide(x, ident, "blog_publish")
    b.run(ident)
    assert row(x, ident).error_code == "base_changed" and len(x["remote"].writes) == 3
    b.refresh(x["owner"].id, ident)
    assert row(x, ident).state == "proposal"
    decide(x, ident, "blog_pr")
    b.run(ident)
    b.run(ident)
    assert row(x, ident).state == "review_ready"
    assert x["remote"].writes[-1][1] == "/merges" and x["remote"].writes[-1][2][
        "base"
    ].startswith("noyaru-blog/")
    assert row(x, ident).receipt["number"] == 9


@pytest.mark.parametrize("where", ["branch", "file", "pr", "ready", "merge"])
def test_lost_write_receipt_is_reconciled_without_reissuing_mutation(blog, where):
    x = blog
    paths = {
        "branch": "/git/refs",
        "file": "/contents/" + c.ROOT + output()["slug"] + ".md",
        "pr": "/pulls",
        "ready": "/graphql",
        "merge": "/pulls/9/merge",
    }
    ident = reviewed_pr(x) if where in {"ready", "merge"} else proposal(x)
    decide(x, ident, "blog_publish" if where in {"ready", "merge"} else "blog_pr")
    x["remote"].timeout = paths[where]
    x["remote"].after = True
    b.run(ident)
    assert row(x, ident).state == "uncertain"
    x["remote"].timeout = None
    b.run(ident)
    assert len([v for v in x["remote"].writes if v[1] == paths[where]]) == 1
    assert row(x, ident).state in {"reviewing", "observing"}


def test_lost_request_without_remote_effect_is_never_retried(blog):
    x = blog
    ident = proposal(x)
    decide(x, ident, "blog_pr")
    x["remote"].timeout = "/git/refs"
    b.run(ident)
    x["remote"].timeout = None
    b.run(ident)
    b.run(ident)
    assert row(x, ident).state == "uncertain" and len(x["remote"].writes) == 1


def test_active_lease_prevents_second_worker(blog):
    x = blog
    ident = proposal(x)
    decide(x, ident, "blog_pr")
    worker = b.claim(x["owner"].id, ident)
    b.run(ident)
    assert not x["remote"].writes
    b.release(worker)


def test_content_is_valid_catalog_markdown_with_literal_prose_and_fixed_sources(
    blog, tmp_path
):
    x = blog
    ident = proposal(x)
    file = row(x, ident).proposal["files"][0]
    path = tmp_path / "article.md"
    path.write_text(file["text"], encoding="utf-8")
    page = m.content_library._load_page(path, collection="blog")
    assert page["slug"] == output()["slug"] and page["source_sha256"] == file["sha256"]
    assert (
        "<script" not in page["body_html"]
        and "https://noyaru.com/docs/" in page["body_html"]
    )
    assert c.prose("Noyaru *texte* <x>") == "{{app_name}} \\*texte\\* &lt;x&gt;"


def test_owner_only_page_and_csrf(blog):
    x = blog
    assert client(user(x["db"])).get(b.PATH).status_code == 403
    assert client(user(x["db"])).post(b.PATH + "/prepare", data={}).status_code in {403}
    response = client(x["owner"]).get(b.PATH)
    soup = BeautifulSoup(response.text, "html.parser")
    assert soup.select_one('label[for="blog-fr"]') and soup.select_one(
        'input[name="_csrf"]'
    )
    assert client(x["owner"]).post(b.PATH + "/prepare", data={}).status_code == 403


def test_english_interface_and_public_article_headers(blog, monkeypatch):
    x = blog
    client_en = client(x["owner"])
    client_en.cookies.set("lang", "en")
    response = client_en.get(b.PATH + "?lang=en")
    assert response.status_code == 200 and "Noyaru blog publication" in response.text
    monkeypatch.setenv("RENDER_GIT_COMMIT", "f" * 40)
    article = m.content_library.blog_pages()[0]
    public = client_en.get("/ressources-seo/" + article["slug"] + "?lang=fr")
    assert public.headers["X-Noyaru-Article-SHA256"] == article["source_sha256"]
    assert public.headers["X-Noyaru-Release"] == "f" * 40


def test_fr_en_pair_uses_same_canonical_slug_and_only_two_added_files(blog):
    x = blog
    with x["db"].session() as db:
        state = b.s.view(db, x["owner"].id, lang="en")
    item = next(c for c in state["candidates"] if c["kind"] == "research")
    brief = b.s.prepare(x["owner"].id, x["token"], item["key"], "en")
    b.s.review(x["owner"].id, brief, x["token"], confirm=True, status="reviewed")
    with x["db"].session() as db:
        _, fingerprint = w.bundle(db, x["owner"].id, brief)
    en = w.authorize(x["owner"].id, brief, fingerprint, uuid.uuid4().hex, True)
    w.run(en)
    with x["db"].session() as db:
        draft = db.get(OwnSeoDraft, en)
        content = copy.deepcopy(draft.content)
        content["slug"] = "an-english-draft-slug"
        draft.content = content
        draft.content_hash = b.s.digest(content)
        fingerprint = draft.content_hash
        db.commit()
    w.review(x["owner"].id, en, fingerprint, True)
    ident = b.prepare(x["owner"].id, x["draft"], en, uuid.uuid4().hex, True)
    b.refresh(x["owner"].id, ident)
    files = row(x, ident).proposal["files"]
    assert (
        len(files) == 2
        and files[1]["path"] == c.ROOT + "en/" + row(x, ident).slug + ".md"
    )
    assert files[1]["url"].startswith("https://noyaru.com/en/ressources-seo/")
    assert (
        "/en/docs/" in files[1]["text"]
        and "an-english-draft-slug" not in files[1]["text"]
    )
    decide(x, ident, "blog_pr")
    b.run(ident)
    assert row(x, ident).state == "reviewing" and len(x["remote"].writes) == 4


@pytest.mark.parametrize(
    "constraint", ["bypass", "reviews", "missing_check", "merge_queue"]
)
def test_branch_constraints_are_never_bypassed_by_an_admin_connection(blog, constraint):
    x = blog
    ident = proposal(x)
    decide(x, ident, "blog_pr")
    b.run(ident)
    protection = {
        "enforce_admins": {"enabled": True},
        "required_status_checks": {"strict": True, "contexts": ["checks"]},
    }
    if constraint == "bypass":
        protection["enforce_admins"]["enabled"] = False
    if constraint == "reviews":
        protection["required_pull_request_reviews"] = {
            "required_approving_review_count": 1
        }
    if constraint == "missing_check":
        protection["required_status_checks"]["contexts"].append("Missing")
    if constraint == "merge_queue":
        x["remote"].rules = [{"type": "merge_queue"}]
    x["remote"].protection = protection
    b.run(ident)
    assert row(x, ident).state != "review_ready" and len(x["remote"].writes) == 3


def test_expired_request_is_reprepared_with_new_version_without_extending_old_approval(
    blog,
):
    x = blog
    ident = proposal(x)
    old = b.version(row(x, ident), "blog_pr")
    with x["db"].session() as db:
        db.get(b.Publication, ident).expires_at = NOW - timedelta(seconds=1)
        db.commit()
    with pytest.raises(ValueError):
        decide(x, ident, "blog_pr")
    b.refresh(x["owner"].id, ident)
    assert b.version(row(x, ident), "blog_pr") != old and not x["remote"].writes


def test_actual_telegram_callback_queues_blog_not_writing_and_double_click_is_idempotent(
    blog, bot, monkeypatch
):
    from types import SimpleNamespace
    from backend import telegram_approvals as a
    from test_telegram_approvals import activate, rows, decision

    x = blog
    with x["db"].session() as db:
        draft = db.get(OwnSeoDraft, x["draft"])
        content = copy.deepcopy(draft.content)
        original = content["sections"]
        content["sections"] = [
            {
                "heading": original[i % 2]["heading"] + " " + str(i + 1),
                "paragraphs": original[i % 2]["paragraphs"] * 3,
            }
            for i in range(5)
        ]
        draft.content = content
        draft.content_hash = b.s.digest(content)
        db.commit()
    activate(bot)
    ident = proposal(x)
    monkeypatch.setattr(
        a.threading,
        "Thread",
        lambda **kw: SimpleNamespace(start=lambda: kw["target"](*kw.get("args", ()))),
    )
    a.sync()
    a.deliver_one()
    request = next(r for r in rows(bot) if r.kind == "blog_pr" and r.target_id == ident)
    assert len(request.pages) > 1 and b.w.utc(request.expires_at) <= b.w.utc(
        row(x, ident).expires_at
    )
    decision(bot, request.id)
    assert row(x, ident).state == "reviewing" and len(x["remote"].writes) == 3
    before = len(x["calls"])
    a.run_result("blog_pr", ident)
    assert row(x, ident).state == "review_ready" and len(x["calls"]) == before
    a.sync()
    publish = next(
        r for r in rows(bot) if r.kind == "blog_publish" and r.target_id == ident
    )
    for _ in range(5):
        if any(r.id == publish.id and r.delivery == "sent" for r in rows(bot)):
            break
        a.deliver_one()
    decision(bot, publish.id, False)
    assert row(x, ident).state == "cancelled" and len(x["remote"].writes) == 3


def test_public_200_without_exact_hash_is_not_publication_proof(blog, monkeypatch):
    x = blog
    ident = reviewed_pr(x)
    decide(x, ident, "blog_publish")
    b.run(ident)
    monkeypatch.setattr(
        b.requests,
        "get",
        lambda *a, **kw: Response(
            headers={"X-Noyaru-Release": x["remote"].branches["main"]}
        ),
    )
    b.run(ident)
    assert (
        row(x, ident).state == "observing"
        and row(x, ident).error_code == "deploy_pending"
    )


def test_lost_sync_response_recovers_same_pr_without_repeating_sync(blog):
    x = blog
    ident = reviewed_pr(x)
    x["remote"].branches["main"] = x["remote"].commit(x["remote"].base, {})
    b.refresh(x["owner"].id, ident)
    decide(x, ident, "blog_pr")
    x["remote"].timeout = "/merges"
    x["remote"].after = True
    b.run(ident)
    assert row(x, ident).state == "uncertain"
    x["remote"].timeout = None
    b.run(ident)
    assert row(x, ident).state == "reviewing"
    assert len([v for v in x["remote"].writes if v[1] == "/merges"]) == 1


def test_refused_proposal_stays_refused_until_explicit_fresh_preparation(blog):
    x = blog
    ident = proposal(x)
    decide(x, ident, "blog_pr", False)
    b.tick()
    assert row(x, ident).state == "cancelled" and not x["remote"].writes
    fresh = b.prepare(x["owner"].id, x["draft"], None, uuid.uuid4().hex, True)
    assert fresh != ident and row(x, fresh).state == "checking"


@pytest.mark.parametrize("encrypted", [False, True])
def test_connection_read_under_decision_lock_never_rotates_or_commits(
    blog, monkeypatch, encrypted
):
    from backend.models import UserConnection

    x = blog
    monkeypatch.setattr(b, "connection", REAL_CONNECTION)
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEY", "blog-test-encryption-" + "e" * 50)
    monkeypatch.setenv("SEO_AGENT_ENCRYPTION_KEYS", "")
    stored = m._encrypt_secret("local-github") if encrypted else "local-github"
    with x["db"].session() as db:
        db.add(
            UserConnection(
                user_id=x["owner"].id, key="GITHUB_TOKEN", secret_value=stored, meta={}
            )
        )
        db.commit()
    monkeypatch.setattr(
        m,
        "_effective_user_connection_value",
        lambda **kw: pytest.fail("No helper that can commit"),
    )
    with x["db"].session() as db:
        b.s.locked(db)
        monkeypatch.setattr(
            db, "commit", lambda: pytest.fail("No implicit commit of the decision")
        )
        assert REAL_CONNECTION(x["owner"].id, db=db)[0] == "local-github"
        db.rollback()
    with x["db"].session() as db:
        assert db.query(UserConnection).one().secret_value == stored


@pytest.mark.parametrize(
    "change", ["language", "canonical", "title", "description", "body"]
)
def test_correct_release_headers_cannot_hide_wrong_rendered_article(
    blog, monkeypatch, tmp_path, change
):
    x = blog
    ident = reviewed_pr(x)
    decide(x, ident, "blog_publish")
    b.run(ident)
    result = row(x, ident)
    file = result.proposal["files"][0]
    public = served_article(file, result.receipt["merge"], monkeypatch, tmp_path)
    parsed = BeautifulSoup(public.content, "html.parser")
    if change == "language":
        parsed.html["lang"] = "en"
    if change == "canonical":
        parsed.select_one("link[rel=canonical]")["href"] = "https://noyaru.com/other"
    if change == "title":
        parsed.select_one("article h1").string = "Autre article"
    if change == "description":
        parsed.select_one("meta[name=description]")["content"] = "Autre description"
    if change == "body":
        parsed.select_one("article .content-body").clear()
    monkeypatch.setattr(
        b.requests,
        "get",
        lambda *a, **kw: Response(
            headers=dict(public.headers), raw=str(parsed).encode()
        ),
    )
    b.run(ident)
    assert (
        row(x, ident).state == "observing"
        and row(x, ident).error_code == "deploy_pending"
    )
