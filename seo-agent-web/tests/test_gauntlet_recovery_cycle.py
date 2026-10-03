from __future__ import annotations

import base64
import json
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from backend import app as m
from backend.db import Database
from ops.gauntlet import recovery_cycle as bench

BASE = "a" * 40
HEAD = "b" * 40
ORIGINAL = "<!doctype html><html lang='fr'><head></head><body><h1>Fixture</h1></body></html>"


def response(status, data):
    value = requests.Response()
    value.status_code = status
    value._content = json.dumps(data).encode()
    return value


class FakeGitHub:
    def __init__(self):
        self.branches = {"main": BASE}
        self.contents = {BASE: ORIGINAL}
        self.prs = []
        self.calls = []

    def __call__(self, method, url, **kwargs):
        assert url.startswith("https://api.github.com" + bench.PREFIX)
        assert kwargs["allow_redirects"] is False
        parsed = urlsplit(url)
        path = parsed.path
        params = {key: values[0] for key, values in parse_qs(parsed.query).items()}
        params.update(kwargs.get("params") or {})
        body = json.loads(kwargs["data"]) if kwargs.get("data") else kwargs.get("json", {})
        self.calls.append((method, path, body))
        if method == "GET":
            if path == bench.PREFIX:
                return response(200, {"full_name": bench.OWNER + "/" + bench.REPO})
            if "/git/ref/heads/" in path:
                branch = path.split("/git/ref/heads/", 1)[1]
                if branch not in self.branches:
                    return response(404, {})
                return response(200, {"ref": "refs/heads/" + branch,
                                      "object": {"sha": self.branches[branch], "type": "commit"}})
            if path.endswith("/git/commits/" + BASE):
                return response(200, {"sha": BASE})
            if path == bench.FILE_API:
                ref = params["ref"]
                sha = self.branches.get(ref, ref)
                return response(200, {"path": bench.FILE, "sha": "blob-original",
                                      "content": base64.b64encode(self.contents[sha].encode()).decode()})
            if path == bench.PREFIX + "/pulls":
                head = params.get("head", "").partition(":")[2]
                return response(200, [row for row in self.prs if row["head"]["ref"] == head])
            if path == bench.PREFIX + "/pulls/7":
                return response(200, self.prs[0])
        if method == "POST" and path.endswith("/git/refs"):
            self.branches[body["ref"].removeprefix("refs/heads/")] = body["sha"]
            return response(201, {})
        if method == "PUT" and path == bench.FILE_API:
            self.branches[body["branch"]] = HEAD
            self.contents[HEAD] = base64.b64decode(body["content"]).decode()
            return response(200, {"commit": {"sha": HEAD, "html_url": "https://github.com/fixture/commit/" + HEAD}})
        if method == "POST" and path.endswith("/pulls"):
            repo = {"full_name": bench.OWNER + "/" + bench.REPO}
            self.prs.append({"number": 7, "html_url": f"https://github.com/{bench.OWNER}/{bench.REPO}/pull/7",
                             "state": "open", "draft": True, "merged": False, "merged_at": None,
                             "node_id": "PR_fixture", "head": {"ref": body["head"], "sha": HEAD, "repo": repo},
                             "base": {"ref": "main", "sha": BASE, "repo": repo}})
            return response(201, self.prs[0])
        if method == "PATCH" and path.endswith("/pulls/7"):
            assert body == {"state": "closed"}
            self.prs[0]["state"] = "closed"
            return response(200, self.prs[0])
        raise AssertionError((method, path))


def relay():
    value = bench.FaultRelay("fixture-token")
    value.main_sha = BASE
    value.file_sha = "blob-original"
    value.patched = "patched fixture"
    return value


@pytest.mark.parametrize("method,path,body", [
    ("POST", "/repos/client/real/pulls", {"head": "main"}),
    ("POST", bench.PREFIX + "/git/refs", {"ref": "refs/heads/main", "sha": BASE}),
    ("POST", bench.PREFIX + "/git/refs", {"ref": "refs/heads/seo-fix/missing_title-fixture", "sha": HEAD}),
    ("PUT", bench.FILE_API, {"branch": "main", "sha": "blob-original", "content": ""}),
    ("PUT", bench.PREFIX + "/contents/another.html", {}),
    ("PUT", bench.PREFIX + "/pulls/7/merge", {}),
    ("DELETE", bench.PREFIX + "/git/refs/heads/main", {}),
    ("GET", "https://another.example/" + bench.PREFIX, {}),
])
def test_the_relay_cannot_mutate_customer_repos_main_or_other_files(method, path, body):
    with pytest.raises(ValueError):
        relay().validate(method, path, body)


def test_the_relay_enforces_the_two_branch_one_commit_one_draft_pr_budget():
    value = relay()
    for index in range(2):
        value.validate("POST", bench.PREFIX + "/git/refs", {
            "ref": f"refs/heads/seo-fix/missing_title-{index}", "sha": BASE})
    with pytest.raises(ValueError):
        value.validate("POST", bench.PREFIX + "/git/refs", {
            "ref": "refs/heads/seo-fix/missing_title-third", "sha": BASE})
    put = {"branch": "seo-fix/missing_title-1", "sha": "blob-original",
           "content": base64.b64encode(value.patched.encode()).decode()}
    assert value.validate("PUT", bench.FILE_API, put) == "file"
    with pytest.raises(ValueError):
        value.validate("PUT", bench.FILE_API, put)
    pr = {"draft": True, "base": "main", "head": "seo-fix/missing_title-1"}
    assert value.validate("POST", bench.PREFIX + "/pulls", pr) == "pr"
    with pytest.raises(ValueError):
        value.validate("POST", bench.PREFIX + "/pulls", pr)


@pytest.mark.parametrize("patch", [{"draft": False}, {"base": "release"}, {"head": "main"}])
def test_pr_publication_requires_a_draft_on_a_bench_created_branch(patch):
    value = relay()
    value.branches.append("refs/heads/seo-fix/missing_title-1")
    with pytest.raises(ValueError):
        value.validate("POST", bench.PREFIX + "/pulls", {
            "draft": True, "base": "main", "head": "seo-fix/missing_title-1", **patch})


def test_an_unknown_accepted_pr_is_found_and_closed_without_merging():
    github = FakeGitHub()
    value = relay()
    value.forward = github
    value.pr_attempts = 1
    value.pr_head = "seo-fix/missing_title-fixture"
    github("POST", "https://api.github.com" + bench.PREFIX + "/pulls", allow_redirects=False,
           json={"head": value.pr_head})
    assert value.close_prs() == [{"number": 7, "state": "closed", "merged": False}]


def test_unconfirmable_pr_cleanup_is_reported_as_unknown_not_success():
    value = relay()
    value.pr_attempts = 1
    value.pr_head = "seo-fix/missing_title-fixture"
    value.forward = lambda *a, **kw: response(403, {})
    result = value.close_prs()
    assert result[0]["outcome"] == "unknown_pr_acceptance"


def test_an_empty_lookup_cannot_prove_cleanup_after_unknown_pr_acceptance():
    value = relay()
    value.pr_attempts = 1
    value.pr_head = "seo-fix/missing_title-fixture"
    value.forward = lambda *a, **kw: response(200, [])
    assert value.close_prs()[0]["outcome"] == "unknown_pr_acceptance"


def test_cleanup_rechecks_pr_identity_before_closing_anything():
    github = FakeGitHub()
    value = relay()
    value.forward = github
    head = "seo-fix/missing_title-fixture"
    pr = github("POST", "https://api.github.com" + bench.PREFIX + "/pulls", allow_redirects=False,
                json={"head": head}).json()
    value.prs = [{"number": 7, "url": pr["html_url"], "branch": head}]
    github.prs[0]["head"]["ref"] = "another-branch"
    assert "cleanup_error_type" in value.close_prs()[0]
    assert not any(method == "PATCH" for method, _, _ in github.calls)


def test_the_relay_forwards_json_only_to_its_fixed_github_host():
    value = relay()
    def forward(method, url, **kwargs):
        assert method == "GET" and url == "https://api.github.com" + bench.PREFIX
        assert kwargs["headers"]["Content-Type"] == "application/json"
        return response(200, {})
    value.forward = forward
    assert value.get(bench.PREFIX) == {}


def test_bench_checks_are_runtime_checks_not_optional_python_assertions():
    with pytest.raises(bench.BenchCheckFailed, match="expected_failure"):
        bench.require(False, "expected_failure")


def test_the_cycle_rejects_a_database_outside_its_workdir(tmp_path):
    with pytest.raises(ValueError, match="isolated bench database"):
        bench.cycle(m, "fixture-token", tmp_path)


def test_socket_drop_and_real_worker_restarts_end_to_end_without_live_github(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "bench.db").as_posix())
    monkeypatch.setenv("FIXTURE_TOKEN", "fixture-token")
    monkeypatch.setenv("SEO_AGENT_SECRET_KEY", "fixture-only-session-secret")
    db = Database(data_dir=tmp_path / "data")
    monkeypatch.setattr(m, "DB", db)
    github = FakeGitHub()
    try:
        result = bench.cycle(m, "fixture-token", tmp_path, forward=github)
        assert result["status"] == "passed", result
        assert len(result["scenarios"]) == 3 and len(result["workers"]) == 7
        assert result["main_unchanged"] and result["pull_requests_closed_without_merge"]
        assert result["used_credits"] == 1 and result["simulated_model_calls"] == 1 and result["claude_calls"] == 0
        assert len(result["branches_retained"]) == 2
        assert github.branches["main"] == BASE and github.prs[0]["state"] == "closed"
        assert [event["fault"] for event in result["github_events"] if "fault" in event] == ["hold_branch", "drop_pr"]
        assert "fixture-token" not in (tmp_path / "recovery.json").read_text(encoding="utf-8")
        for row in result["workers"]:
            assert "fixture-token" not in (tmp_path / row["log"]).read_text(encoding="utf-8")
    finally:
        db.engine.dispose()
