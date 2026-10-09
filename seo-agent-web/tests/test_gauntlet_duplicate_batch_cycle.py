from __future__ import annotations

import base64
import copy
from types import SimpleNamespace

import pytest

from backend import app as m, repo_index
from ops.gauntlet import duplicate_batch_cycle as bench
from ops.gauntlet.ai_budget import ClaudeBudget

SITE = "https://fixture.test/"
DESC = "Description commune au groupe du parcours de validation, pour observer les doublons puis verifier leur correction apres un build."
TITLE = "Titre commun au groupe du parcours de validation"


def reports():
    rows = [{"url": SITE.rstrip("/") + route, "final_url": SITE.rstrip("/") + route,
             "status_code": 200, "content_type": "text/html", "canonical": SITE.rstrip("/") + route,
             "title": TITLE, "meta_description": DESC, "title_tag_count": 1, "meta_description_tag_count": 1}
            for route in sorted(bench.ROUTES | {"/unselected-a/", "/unselected-b/"})]
    before = {"pages": rows, "issues": {key: {"count": len(rows), "examples": [p["url"] for p in rows]}
                                        for key in bench.KEYS}}
    after = copy.deepcopy(before)
    for p in after["pages"]:
        if bench.live._route(p["url"]) in bench.ROUTES:
            name = bench.live._route(p["url"]).strip("/").removeprefix("gauntlet/")
            p["title"] = "Titre distinct de la page de controle " + name
            p["meta_description"] = "Description distincte pour la page " + name + ". " + "Controle des lots, de leurs limites et du rendu apres publication sur la fixture."
    return before, after


def scopes(before):
    return {key: {"impacted_urls": bench.select_urls(before, key, SITE)[0]} for key in bench.KEYS}


@pytest.mark.parametrize("key", bench.KEYS)
def test_all_eight_group_members_and_unselected_occurrences_are_explicit(key):
    before, _ = reports()
    selected, unselected = bench.select_urls(before, key, SITE)
    assert len(selected) == 8 and {bench.live._route(u) for u in selected} == bench.ROUTES
    assert unselected == [SITE + "unselected-a/", SITE + "unselected-b/"]


@pytest.mark.parametrize("key", bench.KEYS)
@pytest.mark.parametrize("change", [
    {"status_code": 202}, {"content_type": "application/json"}, {"blocked_by_host": True}, {"error": "missing"},
    {"canonical": SITE + "elsewhere/"}, {"meta_robots": "noindex"}, {"x_robots_tag": "noindex"},
    {"x_robots_tag": "googlebot: noindex, follow"},
    {"title_tag_count": True, "meta_description_tag_count": True},
])
def test_ineligible_or_unmeasured_pages_are_refused_before_correction(key, change):
    before, _ = reports()
    before["pages"][0].update(change)
    with pytest.raises(ValueError):
        bench.select_urls(before, key, SITE)


@pytest.mark.parametrize("defect", ["missing", "different_group", "foreign_host", "counter_zero"])
def test_incomplete_or_foreign_positive_controls_are_refused(defect):
    before, _ = reports()
    if defect == "missing":
        before["issues"]["duplicate_titles"]["examples"].pop(0)
    elif defect == "different_group":
        before["pages"][0]["title"] = "Une autre valeur de titre"
    elif defect == "foreign_host":
        before["issues"]["duplicate_titles"]["examples"][0] = "https://customer.test" + sorted(bench.ROUTES)[0]
    else:
        before["issues"]["duplicate_titles"]["count"] = 0
    with pytest.raises(ValueError):
        bench.select_urls(before, "duplicate_titles", SITE)


def test_eight_page_html_scope_is_not_silently_interpreted_as_the_old_pair():
    before, after = reports()
    selected = scopes(before)
    assert bench.titles.html_checks(before, after, selected)[0]["verdict"] == "unverified"
    checks = bench.titles.html_checks(before, after, selected, duplicate_routes=bench.ROUTES)
    assert [r["verdict"] for r in checks] == ["resolved_on_selected_observed_html"] * 2 + ["unchanged_or_coherent_social_updates"]
    assert all(len(r["observations"]) == 8 for r in checks[:2])


@pytest.mark.parametrize("defect", ["unchanged", "previous_batch", "unselected_page", "too_short", "too_long", "unknown_tag_count", "missing_route"])
def test_a_zero_counter_cannot_hide_an_unresolved_selected_html_value(defect):
    before, after = reports()
    after["issues"] = {key: {"count": 0} for key in bench.KEYS}
    p = after["pages"][0]
    if defect == "unchanged":
        p["title"] = before["pages"][0]["title"]
    elif defect == "previous_batch":
        after["pages"][7]["title"] = p["title"]
    elif defect == "unselected_page":
        after["pages"][-1]["title"] = p["title"]
    elif defect == "too_short":
        p["title"] = "Test"
    elif defect == "too_long":
        p["title"] = "x" * 71
    elif defect == "unknown_tag_count":
        p["title_tag_count"] = None
    else:
        after["pages"].pop(0)
    check = bench.titles.html_checks(before, after, scopes(before), duplicate_routes=bench.ROUTES)[0]
    assert check["verdict"] == "unverified"


@pytest.mark.parametrize("field,value", [("internal_links", [SITE + "new/"]), ("images_missing_alt", 0),
                                        ("meta_viewport", "changed"), ("ld_json_blocks", ["{}"]),
                                        ("h1_tag_count", 1)])
def test_observed_body_changes_are_not_certified_as_title_only_repairs(field, value):
    before, after = reports()
    assert bench.body_checks(before, after)["verdict"] == "unchanged"
    after["pages"][0][field] = value
    row = bench.body_checks(before, after)
    assert row["verdict"] == "unverified" and row["changes"][0]["fields"] == [field]


def test_missing_body_route_is_not_an_unchanged_observation():
    before, after = reports()
    after["pages"].pop()
    assert bench.body_checks(before, after)["verdict"] == "unverified"


@pytest.mark.parametrize("key", bench.KEYS)
def test_source_checks_reserve_values_across_completed_batches(key):
    before, after = reports()
    state = {}
    paths = ["page-a.html", "page-b.html"]
    for path, page in zip(paths, after["pages"]):
        state[path] = {"sha": "blob", "content": f'<html><head><title>{page["title"]}</title><meta name="description" content="{page["meta_description"]}" /></head><body></body></html>'}
    rows = bench.source_checks(m, state, paths, key)
    assert rows["unique_across_completed_batches"] and all(len(p["source_sha256"]) == 64 for p in rows["sources"])
    state[paths[1]]["content"] = state[paths[0]]["content"]
    with pytest.raises(ValueError, match="preceding batch"):
        bench.source_checks(m, state, paths, key)


@pytest.mark.parametrize("bad", ["missing", "short", "invalid"])
def test_unreadable_out_of_bounds_or_invalid_committed_sources_are_refused(bad):
    raw = "<html><head><title>Test</title></head></html>"
    path = "page.html"
    if bad == "missing":
        raw = "<html><head></head></html>"
    if bad == "invalid":
        path, raw = "page.json", '{"title": "Une valeur correcte du parcours",}'
    with pytest.raises(ValueError):
        bench.source_checks(m, {path: {"content": raw, "sha": "old"}}, [path], "duplicate_titles")


def fake_cycle(tmp_path, monkeypatch, *, bad_write="", changed_seed=False, bad_partition=""):
    before, after = reports()
    stack, seed = "hugo", bench.SEEDS["hugo"]
    production = "https://noyaru-stack-hugo.netlify.app/"
    for report in (before, after):
        for p in report["pages"]:
            for key in ("url", "final_url", "canonical"):
                p[key] = p[key].replace(SITE, production)
        for block in report["issues"].values():
            block["examples"] = [u.replace(SITE, production) for u in block["examples"]]
    refs = {"main": "main-sha", seed["branch"]: "changed" if changed_seed else seed["sha"]}
    files = ["hugo.toml", "layouts/_default/baseof.html"] + [f"content{route.rstrip('/')}.md" for route in sorted(bench.ROUTES)]
    writes, opened = [], []

    def get(path, **kw):
        if "/compare/" in path:
            return {"merge_base_commit": {"sha": refs["main"]}}
        if "/trees/" in path:
            return {"tree": [{"type": "blob", "path": p} for p in files]}
        return {"object": {"sha": refs[path.split("/heads/", 1)[1]]}}

    def post(path, **kw):
        body = kw["json_body"]
        refs[body["ref"].removeprefix("refs/heads/")] = body["sha"]

    def put(path, **kw):
        writes.append(path)
        return {"content": {"sha": "written-blob"}}

    def pr(**kw):
        opened.append(kw)
        return {"number": len(opened), "html_url": SITE + "pr/" + str(len(opened))}

    module = SimpleNamespace(_github_api_get=get, _github_api_post=post, _github_api_put=put,
        _github_api_path=m._github_api_path, _github_ref_api_path=m._github_ref_api_path,
        _github_content_api_path=m._github_content_api_path, _github_branch_allowed=m._github_branch_allowed,
        _ouvrir_pull_request=pr, _prepare_issue_fix=lambda **kw: {"refusal": None},
        _openai_generate_file_patch=lambda **kw: {},
        _find_head_text_value=m._find_head_text_value, _refus_de_format=m._refus_de_format,
        _js_unescape=m._js_unescape, html=m.html)

    def apply(**kw):
        paths = [p for u in kw["impacted"] for p in repo_index.route_files(kw["index"], u)]
        current, deferred = paths[:6], paths[6:]
        if bad_write:
            file = "netlify.toml" if bad_write == "config" else current[0]
            repo = "customer" if bad_write == "repo" else "noyaru-stack-hugo"
            module._github_api_put(m._github_content_api_path(bench.live.OWNER, repo, file),
                                   json_body={"branch": "main" if bad_write == "main" else kw["fix_branch"]})
        kw["ecartes"].extend(deferred)
        for path in current:
            route = "/" + path.removeprefix("content").removesuffix(".md").strip("/") + "/"
            page = next(p for p in after["pages"] if bench.live._route(p["url"]) == route)
            raw = f'<html><head><title>{page["title"]}</title><meta name="description" content="{page["meta_description"]}" /></head><body></body></html>'
            module._github_api_put(m._github_content_api_path(bench.live.OWNER, "noyaru-stack-hugo", path),
                                   json_body={"branch": kw["fix_branch"], "content": base64.b64encode(raw.encode()).decode()})
            kw["file_state"][path] = {"content": raw, "sha": "written-blob"}
        result = {"patched": current, "targets": current, "ai_files": current, "skipped": [], "config_changes": []}
        if bad_partition == "lost_deferred":
            kw["ecartes"].clear()
        elif bad_partition == "overlap":
            kw["ecartes"].append(current[0])
        elif bad_partition == "duplicate_write":
            result["patched"] = current + current[:1]
        elif bad_partition == "wrong_target":
            result["targets"] = current + ["netlify.toml"]
        elif bad_partition == "missing_ai":
            result["ai_files"] = current[:-1]
        elif bad_partition == "skip":
            result["skipped"] = current[:1]
        elif bad_partition == "config":
            result["config_changes"] = ["netlify.toml"]
        elif bad_partition == "error":
            result["error"] = "fixture refused"
        return result

    module._apply_prepared_issue_fix = apply
    monkeypatch.setattr(bench.live, "wait_build", lambda *args: {"head_sha": seed["sha"], "deploy_preview_status": "success"})
    monkeypatch.setattr(bench.titles, "preview_sitemap", lambda url: b"<urlset />")
    monkeypatch.setattr(bench.live, "crawl", lambda stack, url, path, *a, **kw: path.mkdir(parents=True))
    monkeypatch.setattr(bench.titles.previous, "rescore", lambda path, *a: before if "baseline" in path.parts else after)
    monkeypatch.setattr(bench.live, "compare", lambda *a: {
        "comparable": True, "increases": [], "new_issue_routes": [], "missing_html_routes": [],
        "targeted_families": [{"family": k, "verdict": "partial"} for k in bench.KEYS]})
    monkeypatch.setattr(bench.live, "close_prs", lambda repo, token, prs: [{"number": p["number"], "state": "closed", "merged": False} for p in prs])
    original_put = module._github_api_put
    result = bench.cycle(module, "unused", stack, tmp_path, ClaudeBudget(0))
    assert module._github_api_put is original_put
    return result, writes, opened


def test_fake_cycle_finishes_six_two_batches_for_both_fields_without_claiming_full_group(tmp_path, monkeypatch):
    result, writes, prs = fake_cycle(tmp_path, monkeypatch)
    assert result["status"] == "measured_selected_occurrences"
    assert len(writes) == 16 and len(set(writes)) == 8 and len(prs) == 2
    assert all(p["draft"] and p["base"] == "main" for p in prs)
    assert result["main_unchanged"] and result["pull_requests_closed_without_merge"]
    assert result["all_group_occurrences_certified"] is False
    assert result["ai_budget"]["attempted"] == 0
    for scope in result["repair_scopes"].values():
        assert len(scope["unselected_impacted_urls"]) == 2
        assert [len(b["patched"]) for b in scope["batches"]] == [6, 2]
        assert [len(b["deferred_files"]) for b in scope["batches"]] == [2, 0]
        assert [len(b["source_checks"]["sources"]) for b in scope["batches"]] == [6, 8]


@pytest.mark.parametrize("write", ["config", "main", "repo"])
def test_unrelated_content_or_main_write_is_blocked_before_the_remote_effect(tmp_path, monkeypatch, write):
    result, writes, prs = fake_cycle(tmp_path, monkeypatch, bad_write=write)
    assert result["status"] == "failed" and result["error_type"] == "ValueError"
    assert writes == [] and len(prs) == 1 and result["pull_requests_closed_without_merge"]


@pytest.mark.parametrize("partition", ["lost_deferred", "overlap", "duplicate_write", "wrong_target", "missing_ai", "skip", "config", "error"])
def test_a_partial_or_inconsistent_partition_cannot_pass_the_bench(tmp_path, monkeypatch, partition):
    result, writes, prs = fake_cycle(tmp_path, monkeypatch, bad_partition=partition)
    assert result["status"] == "failed" and result["stage"] == "duplicate_titles"
    assert len(writes) == 6 and len(prs) == 1 and result["pull_requests_closed_without_merge"]


def test_changed_seed_has_no_branch_pr_or_content_effect(tmp_path, monkeypatch):
    result, writes, prs = fake_cycle(tmp_path, monkeypatch, changed_seed=True)
    assert result["status"] == "failed" and writes == prs == []
    assert not result["pull_requests_closed_without_merge"]


def test_customer_stack_and_excessive_budget_are_refused_before_access(tmp_path):
    with pytest.raises(ValueError, match="owned"):
        bench.cycle(None, "unused", "customer", tmp_path, ClaudeBudget(0))
    with pytest.raises(ValueError, match="24"):
        bench.cycle(None, "unused", "hugo", tmp_path, ClaudeBudget(25))


def test_cli_budget_cannot_be_raised_past_twenty_four(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.argv", ["bench", "--workdir", str(tmp_path / "new"), "--ai-max-calls", "25"])
    with pytest.raises(SystemExit) as exc:
        bench.main()
    assert exc.value.code == 2 and not (tmp_path / "new").exists()
