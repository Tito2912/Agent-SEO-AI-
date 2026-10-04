from __future__ import annotations

import base64
import json
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
import requests

from backend import app as m
from backend.db import Database
from ops.gauntlet import bulk_recovery_cycle as bench

BASE = "a" * 40
DESCRIPTION = "Une description complete pour le banc de reprise des corrections, avec suffisamment de texte pour rester conforme aux seuils."


def response(status, data):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(data).encode()
    return result


class FakeGitHub:
    def __init__(self):
        title = "Un titre commun pour les deux pages du banc de validation"
        self.contents = {BASE: {p: '<!doctype html><html lang="fr"><head>'
            + ("<title>" + title + "</title>" if p != bench.FILES[2] else "")
            + '<meta name="description" content="' + DESCRIPTION + '" />'
            + '</head><body><h1>Controle de reprise</h1></body></html>' for p in bench.FILES}}
        self.branches = {"main": BASE}
        self.prs, self.calls = [], []
        self.commits = 0

    def __call__(self, method, url, **kw):
        assert url.startswith("https://api.github.com" + bench.PREFIX)
        assert kw["allow_redirects"] is False
        parsed = urlsplit(url)
        path = unquote(parsed.path)
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        params.update(kw.get("params") or {})
        body = json.loads(kw["data"]) if kw.get("data") else kw.get("json", {})
        self.calls.append((method, path, body))
        if method == "GET":
            if path == bench.PREFIX:
                return response(200, {"full_name": bench.OWNER + "/" + bench.REPO})
            if "/git/ref/heads/" in path:
                branch = path.partition("/git/ref/heads/")[2]
                if branch not in self.branches:
                    return response(404, {})
                return response(200, {"ref": "refs/heads/" + branch,
                                      "object": {"sha": self.branches[branch], "type": "commit"}})
            if path.endswith("/git/commits/" + BASE):
                return response(200, {"sha": BASE})
            if "/git/trees/" in path:
                return response(200, {"tree": [{"type": "blob", "path": p} for p in bench.FILES]})
            if "/contents/" in path:
                file = path.partition("/contents/")[2]
                ref = params["ref"]
                sha = self.branches.get(ref, ref)
                return response(200, {"path": file, "sha": "blob-" + file,
                                      "content": base64.b64encode(self.contents[sha][file].encode()).decode()})
            if path == bench.PREFIX + "/pulls":
                head = params.get("head", "").partition(":")[2]
                return response(200, [r for r in self.prs if not head or r["head"]["ref"] == head])
            if path.endswith("/pulls/8"):
                return response(200, self.prs[0])
        if method == "POST" and path.endswith("/git/refs"):
            self.branches[body["ref"].removeprefix("refs/heads/")] = body["sha"]
            return response(201, {})
        if method == "PUT" and "/contents/" in path:
            self.commits += 1
            head = f"{self.commits:040x}"
            file = path.partition("/contents/")[2]
            content = dict(self.contents[self.branches[body["branch"]]])
            content[file] = base64.b64decode(body["content"]).decode()
            self.contents[head] = content
            self.branches[body["branch"]] = head
            return response(200, {"content": {"sha": "patched-" + file}, "commit": {"sha": head}})
        if method == "POST" and path.endswith("/pulls"):
            repo = {"full_name": bench.OWNER + "/" + bench.REPO}
            self.prs.append({"number": 8, "html_url": f"https://github.com/{bench.OWNER}/{bench.REPO}/pull/8",
                             "state": "open", "draft": True, "merged": False, "merged_at": None,
                             "head": {"ref": body["head"], "sha": self.branches[body["head"]], "repo": repo},
                             "base": {"ref": "main", "sha": BASE, "repo": repo}})
            return response(201, self.prs[0])
        if method == "PATCH" and path.endswith("/pulls/8"):
            assert body == {"state": "closed"}
            self.prs[0]["state"] = "closed"
            return response(200, self.prs[0])
        raise AssertionError((method, path))


@pytest.mark.parametrize("url", [
    "sqlite:///client.db",
    "postgresql+psycopg://fixture@production.example/seo_corrector_test_bulk_1",
    "postgresql+psycopg://fixture@127.0.0.1:55476/client_db",
    "postgresql+psycopg://admin@127.0.0.1:55476/seo_corrector_test_bulk_1",
    "postgresql+psycopg://fixture:secret@127.0.0.1:55476/seo_corrector_test_bulk_1",
    "postgresql+psycopg://fixture@127.0.0.1:55476/seo_corrector_test_bulk_1?host=production.example",
])
def test_only_a_passwordless_dedicated_loopback_postgres_or_the_bench_sqlite_is_allowed(tmp_path, url):
    with pytest.raises(ValueError):
        bench.database_url(tmp_path, url)


@pytest.mark.parametrize("path", [
    "/repos/client/site/pulls", "https://example.com" + bench.PREFIX,
    bench.PREFIX + "/../../client/site", bench.PREFIX + "/%2e%2e/other",
    bench.PREFIX + "/%5c..%5cclient", bench.PREFIX + "/pulls#fragment",
])
def test_the_fixture_relay_cannot_escape_its_fixed_repository(path):
    with pytest.raises(ValueError):
        bench.scoped_path(path)


def relay():
    value = bench.BulkRelay("fixture-token")
    value.main_sha = BASE
    value.sources = {p: {"sha": "blob-" + p, "patched": "changed " + p} for p in bench.FILES}
    return value


def branch(value, n):
    name = "seo-fix/bulk-fixture-" + str(n)
    value.validate("POST", bench.PREFIX + "/git/refs", {"ref": "refs/heads/" + name, "sha": BASE})
    return name


def put(value, head, path):
    source = value.sources[path]
    return value.validate("PUT", bench.PREFIX + "/contents/" + path,
        {"branch": head, "sha": source["sha"], "content": base64.b64encode(source["patched"].encode()).decode()})


def test_two_branches_four_exact_commits_one_completed_batch_draft_pr_are_hard_limits():
    value = relay()
    first, second = branch(value, 1), branch(value, 2)
    with pytest.raises(ValueError):
        branch(value, 3)
    assert put(value, first, bench.FILES[0]) == "file"
    with pytest.raises(ValueError):
        put(value, first, bench.FILES[0])
    for file in bench.FILES:
        assert put(value, second, file) == "file"
    with pytest.raises(ValueError):
        put(value, first, bench.FILES[1])
    pr = {"draft": True, "head": second, "base": "main"}
    assert value.validate("POST", bench.PREFIX + "/pulls", pr) == "pr"
    with pytest.raises(ValueError):
        value.validate("POST", bench.PREFIX + "/pulls", pr)


@pytest.mark.parametrize("method,path,body", [
    ("POST", bench.PREFIX + "/git/refs", {"ref": "refs/heads/main", "sha": BASE}),
    ("PUT", bench.PREFIX + "/contents/client.html", {}),
    ("PUT", bench.PREFIX + "/contents/" + bench.FILES[0], {"branch": "main", "sha": "wrong", "content": ""}),
    ("POST", bench.PREFIX + "/pulls", {"head": "main", "draft": False, "base": "main"}),
    ("PUT", bench.PREFIX + "/pulls/8/merge", {}),
    ("DELETE", bench.PREFIX + "/git/refs/heads/main", {}),
])
def test_other_contents_main_merge_and_delete_are_never_forwarded(method, path, body):
    with pytest.raises(ValueError):
        relay().validate(method, path, body)


def test_existing_database_is_refused_before_remote_reads(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "bench.db").as_posix())
    db = Database(data_dir=tmp_path / "data")
    db.create_tables()
    monkeypatch.setattr(m, "DB", db)
    try:
        with pytest.raises(ValueError, match="fresh isolated fixture"):
            bench.cycle(m, "fixture-token", tmp_path, forward=lambda *a, **kw: pytest.fail("Existing data must not be touched"))
    finally:
        db.engine.dispose()


def test_partial_file_kill_and_atomic_bulk_charge_recovery_with_real_api_processes(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "bench.db").as_posix())
    monkeypatch.setenv("FIXTURE_TOKEN", "fixture-token")
    db = Database(data_dir=tmp_path / "data")
    monkeypatch.setattr(m, "DB", db)
    github = FakeGitHub()
    try:
        result = bench.cycle(m, "fixture-token", tmp_path, forward=github)
        assert result["status"] == "passed", result
        assert result["used_credits"] == 3 and result["usage_events"] == 1 and result["tasks_restored"] == 2
        assert result["main_unchanged"] and result["pull_requests_closed_without_merge"] and result["claude_calls"] == 0
        assert len(result["workers"]) == 6 and len(result["branches_retained"]) == 2
        assert github.commits == 4 and len(github.prs) == 1 and github.prs[0]["state"] == "closed"
        assert [r["fault"] for r in result["github_events"] if "fault" in r] == ["hold_file", "drop_pr"]
        assert all(r["result"]["ai_budget"]["attempted"] == 0 for r in result["workers"] if r["result"])
        assert "fixture-token" not in (tmp_path / "bulk-recovery.json").read_text(encoding="utf-8")
    finally:
        db.engine.dispose()
