"""Validate fixture-only scope, positive controls and the real recrawl prerequisite."""

import base64
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

from backend import app as m
from ops.gauntlet import sitemap_lifecycle_cycle as bench
from ops.gauntlet.ai_budget import ClaudeBudget


@pytest.mark.parametrize("body,expected", [(b"<broken", None), (m._sitemap_xml([bench.SITE]).encode(), [bench.SITE])])
def test_xml_observation_requires_actual_parseable_urlset(body, expected):
    assert bench.locs(body) == expected


def test_an_index_cannot_silently_be_used_as_an_empty_sitemap():
    with pytest.raises(ValueError):
        bench.locs(b"<sitemapindex />")


def test_entities_are_not_evaluated():
    assert bench.locs(b'<!DOCTYPE x [<!ENTITY y "secret">]><urlset>&y;</urlset>') is None


def test_robots_declarations_are_case_insensitive():
    assert bench.declarations("User-agent: *\n# Sitemap: ignored\n SITEMAP: https://site.test/sitemap.xml\n") == ["https://site.test/sitemap.xml"]


@pytest.mark.parametrize("change", [{"blocked_by_host": True}, {"content_type": "application/json"},
    {"status_code": "200"}, {"meta_robots": "noindex"}, {"x_robots_tag": "noindex"},
    {"canonical": bench.SITE + "OTHER/"}])
def test_independent_xml_membership_check_excludes_ineligible_pages(change):
    page = {"url": bench.SITE, "status_code": 200, "content_type": "text/html", "canonical": bench.SITE}
    assert not bench.eligible_urls({"pages": [{**page, **change}]})


@pytest.mark.parametrize("case,limit", [("customer", 0), ("missing", 1), ("invalid", 1)])
def test_nonfixture_or_nonzero_provider_budget_is_refused_before_remote_io(tmp_path, case, limit):
    with pytest.raises(ValueError):
        bench.cycle(None, "unused", case, tmp_path, ClaudeBudget(limit))
    assert not list(tmp_path.iterdir())


def run_fake(tmp_path, monkeypatch, case, *, unsafe="", cleanup_ok=True, missing_positive=False):
    refs = {"main": bench.MAIN_SHA}
    robots = "User-agent: *\nAllow: /\n\nUser-agent: PrivateBot\nDisallow: /secret\n"
    files = {"main": {"sitemap.xml": m._sitemap_xml([bench.SITE]), "robots.txt": robots, "index.html": "HTML"}}
    prs, remote, crawls = [], [], []

    def branch_for_ref(ref):
        return ref if ref in refs else next(name for name, sha in refs.items() if sha == ref)

    def get(path, **kw):
        if "/git/ref/" in path:
            return {"object": {"sha": refs[path.split("/heads/", 1)[1]]}}
        if "/trees/" in path:
            return {"tree": [{"type": "blob", "path": name} for name in files["main"]]}
        name = unquote(path.split("/contents/", 1)[1])
        branch = branch_for_ref(kw["params"]["ref"])
        source = files[branch][name]
        return {"sha": "blob", "encoding": "base64", "content": base64.b64encode(source.encode()).decode()}

    def post(path, **kw):
        body = kw["json_body"]
        name = body["ref"].removeprefix("refs/heads/")
        parent = branch_for_ref(body["sha"])
        files[name] = dict(files[parent])
        refs[name] = name

    def put(path, **kw):
        body = kw["json_body"]
        name = unquote(path.split("/contents/", 1)[1])
        remote.append(("put", body["branch"], name))
        files[body["branch"]][name] = base64.b64decode(body["content"]).decode()
        refs[body["branch"]] += "-write"
        return {"content": {"sha": "new"}}

    def delete(url, **kw):
        body = kw["json"]
        assert body["branch"].startswith("gauntlet-static-html-sitemap-")
        assert url.endswith("/contents/sitemap.xml")
        del files[body["branch"]]["sitemap.xml"]
        refs[body["branch"]] += "-delete"
        remote.append(("delete", body["branch"], "sitemap.xml"))
        return SimpleNamespace(raise_for_status=lambda: None)

    def pr(**kw):
        assert kw["draft"] is True and kw["base"] == "main"
        prs.append(kw)
        return {"number": len(prs), "html_url": "https://fixture.test/pr/" + str(len(prs))}

    def http(url, **kw):
        number = int(url.split("deploy-preview-", 1)[1].split("--", 1)[0])
        branch = prs[number-1]["head"]
        name = url.rsplit("/", 1)[1]
        if not name:
            return SimpleNamespace(status_code=200, headers={"content-type": "text/html"})
        source = files[branch].get(name)
        return SimpleNamespace(status_code=200 if source is not None else 404,
                               content=(source or "404").encode(), headers={"content-type": "application/xml" if name.endswith("xml") else "text/plain"})

    def controlled(raw, preview, work):
        crawls.append(work.name)
        observed = bench.live.load(work / "files.json")
        body = (work / "sitemap.xml").read_bytes()
        invalid = observed["sitemap.xml"]["status_code"] == 200 and bench.locs(body) is None
        missing = observed["sitemap.xml"]["status_code"] == 404
        return {"issues": {"sitemap_xml_not_found": {"count": int(missing)},
            "sitemap_invalid_format": {"count": int(invalid)},
            "sitemap_not_in_robots": {"count": 0 if missing_positive else int(not bench.declarations((work / "robots.txt").read_text()))}},
            "pages": [{"url": bench.SITE, "final_url": bench.SITE, "canonical": bench.SITE,
                       "status_code": 200, "content_type": "text/html"}]}

    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_post", post)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_ouvrir_pull_request", pr)
    monkeypatch.setattr(bench.live.requests, "delete", delete)
    monkeypatch.setattr(bench.live.requests, "get", http)
    monkeypatch.setattr(bench.live, "crawl", lambda *a, **kw: None)
    monkeypatch.setattr(bench, "controlled", controlled)
    def wait_build(backend, repo, token, number, *, expected_sha):
        assert expected_sha == refs[prs[number-1]["head"]]
        return {"head_sha": expected_sha, "deploy_preview_status": "success"}
    monkeypatch.setattr(bench.live, "wait_build", wait_build)
    monkeypatch.setattr(bench.live, "close_prs", lambda repo, token, rows: [
        {"number": row["number"], "state": "closed" if cleanup_ok else "open", "merged": False} for row in rows])
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: pytest.fail("No model should be invoked"))
    if unsafe:
        def bad_apply(**kw):
            target = ("package.json" if unsafe == "path" else "sitemap.xml")
            repo = "customer" if unsafe == "repo" else bench.REPO
            return m._github_api_put(m._github_content_api_path(bench.live.OWNER, repo, target), token="unused",
                json_body={"branch": "main" if unsafe == "main" else kw["fix_branch"], "content": ""})
        monkeypatch.setattr(m, "_apply_prepared_issue_fix", bad_apply)
    result = bench.cycle(m, "unused", case, tmp_path, ClaudeBudget(0))
    assert m._github_api_put is put
    assert files["main"]["robots.txt"] == robots and refs["main"] == bench.MAIN_SHA
    assert all(branch != "main" for _, branch, _ in remote)
    return result, remote, crawls


@pytest.mark.parametrize("case", bench.KEYS)
def test_sitemap_is_revisited_before_robots_is_fixed_and_prs_are_closed(tmp_path, monkeypatch, case):
    result, remote, crawls = run_fake(tmp_path, monkeypatch, case)
    assert result["status"] == "measured_observed_sitemap_lifecycle"
    assert result["main_unchanged"] and result["prs_closed_unmerged"]
    assert crawls == ["baseline", "sitemap_repaired", "final"]
    assert len(result["pull_requests"]) == 2
    assert [len(p["builds"]) for p in result["pull_requests"]] == [1, 2]
    assert len(remote) == 3 and result["ai_budget"]["attempted"] == 0
    assert result["target_counts"][bench.KEYS[case]] == [1, 0]
    if case == "invalid":
        assert result["stale_repair_refused_before_put"] is True


@pytest.mark.parametrize("unsafe", ["path", "main", "repo"])
def test_unrelated_writes_are_blocked_before_the_remote_effect(tmp_path, monkeypatch, unsafe):
    result, remote, _ = run_fake(tmp_path, monkeypatch, "invalid", unsafe=unsafe)
    assert result["status"] == "failed" and result["error_type"] == "ValueError"
    assert len(remote) == 1
    assert result["prs_closed_unmerged"]


def test_cleanup_failure_cannot_be_reported_as_measured(tmp_path, monkeypatch):
    result, _, _ = run_fake(tmp_path, monkeypatch, "invalid", cleanup_ok=False)
    assert result["status"] == "unverified" and not result["prs_closed_unmerged"]


def test_absent_positive_control_is_not_success(tmp_path, monkeypatch):
    result, remote, _ = run_fake(tmp_path, monkeypatch, "invalid", missing_positive=True)
    assert result["status"] == "failed" and len(remote) == 1
    assert result["prs_closed_unmerged"]
