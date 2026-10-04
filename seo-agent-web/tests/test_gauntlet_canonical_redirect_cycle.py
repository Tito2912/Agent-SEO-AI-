"""Positive controls and bounded owned-repository writes for the canonical bench."""

import base64
import re
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

from backend import app as m
from ops.gauntlet import canonical_redirect_cycle as bench
from ops.gauntlet.ai_budget import ClaudeBudget


def test_customer_page_names_are_rejected():
    with pytest.raises(ValueError):
        bench.page("customer")


def test_nonzero_provider_budget_is_refused_before_io(tmp_path):
    with pytest.raises(ValueError):
        bench.cycle(None, "unused", tmp_path, ClaudeBudget(1))
    assert not list(tmp_path.iterdir())


def fake_cycle(monkeypatch, tmp_path, *, unsafe="", cleanup_ok=True, positive=True):
    sitemap = m._sitemap_xml([bench.SITE])
    original_rules = "/old / 301\n"
    files = {"main": {"index.html": "<html><body><h1>Control</h1></body></html>\n",
                      "sitemap.xml": sitemap, "_redirects": original_rules}}
    refs, prs, writes = {"main": bench.MAIN}, [], []

    def get(path, **kw):
        if "/git/ref/" in path:
            return {"object": {"sha": refs[path.split("/heads/", 1)[1]]}}
        if "/trees/" in path:
            return {"tree": [{"path": name, "type": "blob"} for name in files["main"]]}
        name = unquote(path.split("/contents/", 1)[1])
        return {"sha": "blob", "content": base64.b64encode(files[kw["params"]["ref"]][name].encode()).decode()}

    def post(path, **kw):
        name = kw["json_body"]["ref"].removeprefix("refs/heads/")
        parent = next(branch for branch, sha in refs.items() if sha == kw["json_body"]["sha"])
        files[name] = dict(files[parent])
        refs[name] = name

    def put(path, **kw):
        body = kw["json_body"]
        name = unquote(path.split("/contents/", 1)[1])
        writes.append((body["branch"], name))
        files[body["branch"]][name] = base64.b64decode(body["content"]).decode()
        refs[body["branch"]] += "-write"
        return {"content": {"sha": "new"}}

    def pr(**kw):
        assert kw["draft"] and kw["base"] == "main"
        prs.append(kw)
        return {"number": len(prs), "html_url": "https://fixture.test/" + str(len(prs))}

    def current(preview):
        number = int(preview.split("deploy-preview-", 1)[1].split("--", 1)[0])
        return files[prs[number-1]["head"]]

    def observations(preview, work):
        content = current(preview)
        return {"dead": {"status": 404 if positive else 503},
                bench.NAMES[2]: {"status": 301 if bench.RULE in content["_redirects"] else 200, "x_robots_tag": "noindex"}}

    def report(raw, preview, site, sitemap, output):
        content = current(preview)
        loop = bench.RULE in content["_redirects"]
        rows = [{"url": bench.SITE, "status_code": 200, "content_type": "text/html"},
                {"url": bench.DEAD, "status_code": 404}]
        for name, path in bench.FILES.items():
            if loop and name == bench.NAMES[2]:
                continue
            canonical = re.search(r'rel="canonical" href="([^"]+)"', content[path])[1]
            rows.append({"url": bench.SITE.rstrip("/") + bench.ROUTES[name], "status_code": 200,
                         "content_type": "text/html", "canonical": canonical, "title": name})
        broken = [p["url"] + " -> " + bench.DEAD for p in rows if p.get("canonical") == bench.DEAD]
        return {"pages": rows, "issues": {
            "canonical_points_to_4xx": {"count": len(broken), "examples": broken},
            "redirect_3xx": {"count": int(loop), "examples": [bench.SITE.rstrip("/") + bench.LOOP] if loop else [],
                "evidence": {"kind": "page_values", "items": [{"page": bench.SITE.rstrip("/") + bench.LOOP,
                    "field": m._SELF_LOOP_FIELD, "value": ""}] if loop else []}}}}

    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_post", post)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_ouvrir_pull_request", pr)
    monkeypatch.setattr(m, "_github_tarball_grep", lambda *a, **kw: list(bench.FILES.values()))
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: pytest.fail("No model permitted"))
    monkeypatch.setattr(bench.live, "wait_build", lambda backend, repo, token, number, *, expected_sha: {
        "head_sha": expected_sha, "deploy_preview_status": "success"})
    monkeypatch.setattr(bench.live, "close_prs", lambda repo, token, rows: [
        {"number": row["number"], "state": "closed" if cleanup_ok else "open", "merged": False} for row in rows])
    monkeypatch.setattr(bench.live, "crawl", lambda *a, **kw: None)
    monkeypatch.setattr(bench.titles, "preview_sitemap", lambda url: current(url)["sitemap.xml"].encode())
    monkeypatch.setattr(bench, "observed_response", observations)
    monkeypatch.setattr(bench.projection, "rescore", report)
    if unsafe:
        def bad_apply(**kw):
            target = "netlify.toml" if unsafe == "path" else "_redirects"
            return m._github_api_put(m._github_content_api_path(bench.live.OWNER, bench.REPO, target), token="unused",
                json_body={"branch": "main" if unsafe == "main" else kw["fix_branch"], "content": ""})
        monkeypatch.setattr(m, "_apply_prepared_issue_fix", bad_apply)
    result = bench.cycle(m, "unused", tmp_path, ClaudeBudget(0))
    assert m._github_api_put is put
    assert files["main"]["_redirects"] == original_rules and refs["main"] == bench.MAIN
    assert all(branch != "main" for branch, _ in writes)
    return result, writes


def test_both_real_helpers_are_exercised_and_both_prs_closed(tmp_path, monkeypatch):
    result, writes = fake_cycle(monkeypatch, tmp_path)
    assert result["status"] == "measured_selected_canonical_and_self_loop", result
    assert result["prs_closed_unmerged"] and result["main_unchanged"] and len(writes) == 9
    assert result["checks"]["selected_dead_canonical_occurrences"] == [2, 0]
    assert result["ai_budget"]["attempted"] == 0
    assert len(result["pull_requests"]) == 2


@pytest.mark.parametrize("unsafe", ["main", "path"])
def test_out_of_scope_writes_are_stopped_before_remote_effect(tmp_path, monkeypatch, unsafe):
    result, writes = fake_cycle(monkeypatch, tmp_path, unsafe=unsafe)
    assert result["status"] == "failed" and result["failure_code"] == "outside_owned_qa_write_scope"
    assert len(writes) == 6 and result["prs_closed_unmerged"]


def test_nondefinitive_positive_control_cannot_pass(tmp_path, monkeypatch):
    result, writes = fake_cycle(monkeypatch, tmp_path, positive=False)
    assert result["status"] == "failed" and result["failure_code"] == "missing_http_positive_controls"
    assert len(writes) == 6


def test_failed_cleanup_cannot_be_certified(tmp_path, monkeypatch):
    result, _ = fake_cycle(monkeypatch, tmp_path, cleanup_ok=False)
    assert result["status"] == "unverified"


def test_http_observation_does_not_follow_the_loop(monkeypatch, tmp_path):
    seen = []

    def get(url, **kw):
        assert kw["allow_redirects"] is False
        seen.append(url)
        return SimpleNamespace(status_code=301, content=b"loop", headers={"location": url, "x-robots-tag": "noindex"})

    monkeypatch.setattr(bench.live.requests, "get", get)
    rows = bench.observed_response("https://preview.test/", tmp_path)
    assert len(seen) == 4 and rows[bench.NAMES[2]]["status"] == 301
