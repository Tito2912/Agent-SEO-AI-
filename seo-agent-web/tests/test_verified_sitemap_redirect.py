"""A sitemap redirect proposal needs observed hops and a still-valid literal source."""

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index
from tests.test_verified_sitemap_canonical import entry, sitemap

KEY, SITE = "sitemap_3xx_redirect", "https://site.test"
A, B, C = (SITE + "/" + name for name in ("old", "master", "hop"))
PAIR = {"page": A, "from": A, "to": B}


def row(url, **changes):
    return {"url": url, "final_url": url, "status_code": 200, "content_type": "text/html",
            "canonical": url, "redirect_chain": [], "redirect_statuses": [], **changes}


def redirected(**changes):
    return row(A, **{"final_url": B, "canonical": B, "redirect_chain": [A], "redirect_statuses": [301], **changes})


def prepare(rows, pairs=None, paths=None):
    pairs = [PAIR] if pairs is None else pairs
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: {"count": len(pairs), "examples": [p["from"] for p in pairs],
        "evidence": {"kind": "url_pairs", "items": pairs}}}, impacted=[p["from"] for p in pairs],
        all_paths=paths or ["sitemap.xml", "_redirects", "master.html"], site_name="site.test", owner="fixture",
        repo_name="fixture", branch="baseline", token="unused", pages=rows)


def test_verified_redirect_selects_only_literal_sitemap_without_a_model():
    prep = prepare([redirected(), row(B)])
    assert not prep["refusal"] and not prep["rewriter_ai_fallback"]
    assert prep.get("sitemap_redirect_pairs") == [PAIR] and prep["targets_override"] == ["sitemap.xml"]


@pytest.mark.parametrize("change", [
    {"status_code": 404}, {"status_code": 503}, {"status_code": True}, {"status_code": "200"},
    {"content_type": "application/json"}, {"error": "timeout"}, {"blocked_by_host": True},
    {"meta_robots": "noindex"}, {"x_robots_tag": "googlebot: NOINDEX"},
    {"canonical": C}, {"canonical": "not a URL"}, {"final_url": C},
    {"redirect_statuses": [301], "redirect_chain": [B]},
])
def test_unhealthy_or_non_direct_master_refuses(change):
    assert prepare([redirected(), row(B, **change)])["refusal"]


@pytest.mark.parametrize("change", [
    {"status_code": 404}, {"status_code": "200"}, {"content_type": "text/plain"}, {"error": "timeout"},
    {"blocked_by_host": True}, {"final_url": C}, {"canonical": C}, {"meta_robots": "noindex"},
    {"redirect_statuses": []}, {"redirect_statuses": [True]}, {"redirect_statuses": [304]},
    {"redirect_statuses": ["301"]}, {"redirect_statuses": [301, 302]}, {"redirect_chain": []},
    {"redirect_chain": [C]}, {"redirect_chain": [A, A], "redirect_statuses": [301, 301]},
    {"redirect_chain": [A, B], "redirect_statuses": [301, 301]},
])
def test_incomplete_or_contradictory_chain_refuses(change):
    assert prepare([redirected(**change), row(B)])["refusal"]


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_real_followable_statuses_are_supported(status):
    assert prepare([redirected(redirect_statuses=[status]), row(B)]).get("sitemap_redirect_pairs") == [PAIR]


def test_multihop_requires_consistent_observations_and_an_independent_direct_master():
    source = redirected(redirect_chain=[A, C], redirect_statuses=[302, 301])
    assert prepare([source])["refusal"]
    assert prepare([source, row(B)]).get("sitemap_redirect_pairs") == [PAIR]
    assert prepare([source, redirected(), row(B)])["refusal"]
    assert prepare([source, row(B)], [PAIR, {**PAIR, "to": C}])["refusal"]
    assert prepare([source, row(B)], [{**PAIR, "page": C}])["refusal"]


@pytest.mark.parametrize("hop", ["https://external.test/hop", "http://site.test/hop"])
def test_external_or_downgraded_intermediate_hop_refuses(hop):
    assert prepare([redirected(redirect_chain=[A, hop], redirect_statuses=[301, 301]), row(B)])["refusal"]


def test_partial_refusals_are_named_and_not_counted_as_repaired():
    dead = SITE + "/dead"
    prep = prepare([redirected(), row(B), row(dead, final_url=C, redirect_chain=[dead], redirect_statuses=[301])],
                   [PAIR, {"page": dead, "from": dead, "to": C}])
    assert not prep["refusal"] and prep.get("sitemap_redirect_pairs") == [PAIR]
    assert dead in prep["side_effects"]


@pytest.mark.parametrize("paths", [["app/sitemap.ts"], ["sitemap.xml", "backup-sitemap.xml"],
    ["sitemap.xml", "app/sitemap.ts"], ["dist/sitemap.xml", "_redirects"], ["sitemap.xml"]])
def test_generated_ambiguous_or_unverifiable_redirect_source_refuses(paths):
    assert prepare([redirected(), row(B)], paths=paths)["refusal"]


@pytest.mark.parametrize("case", ["verified", "multihop", "missing_master_entry", "stale_rule", "missing_rule", "wrong_code",
    "duplicate_rule", "wildcard", "conditional", "rewrite", "unreadable_config", "other_redirect_config",
    "missing_sha", "invalid_base64", "generated_package", "stale_sitemap", "noindex_master", "computed_master",
    "different_master_canonical", "unobserved_master", "insufficient_budget", "query_not_in_rule", "unforced_shadowed_source"])
def test_pipeline_rechecks_configuration_and_master_before_only_one_sitemap_put(monkeypatch, case):
    alias = entry(A, "<lastmod>2001-01-01</lastmod>")
    master = entry(B, "<lastmod>2026-01-01</lastmod><priority>0.8</priority>")
    source = sitemap(alias, master)
    html = '<html lang="fr"><head><link rel="canonical" href="' + B + '" /></head><body>Master</body></html>'
    sources = {"sitemap.xml": source, "_redirects": "/old /master 301!\n", "master.html": html}
    rows = [redirected(), row(B)]
    if case == "multihop":
        rows[0] = redirected(redirect_chain=[A, C], redirect_statuses=[302, 301])
        sources["_redirects"] = "/old /hop 302\n/hop /master 301\n"
    elif case == "missing_master_entry":
        sources["sitemap.xml"] = source = sitemap(alias)
    elif case in {"stale_rule", "missing_rule", "wrong_code", "duplicate_rule", "wildcard", "conditional", "rewrite"}:
        sources["_redirects"] = {"stale_rule": "/old /other 301\n", "missing_rule": "# disappeared\n",
            "wrong_code": "/old /master 302\n", "duplicate_rule": "/old /master 301\n/old /other 301\n",
            "wildcard": "/* /master 301\n", "conditional": "/old /master 301 Country=fr\n",
            "rewrite": "/old /master 200\n"}[case]
    elif case == "other_redirect_config":
        sources["netlify.toml"] = '[[redirects]]\nfrom="/old"\nto="/other"\nstatus=301\n'
    elif case == "generated_package":
        sources["package.json"] = '{"dependencies":{"next-sitemap":"4.0.0"}}'
    elif case == "stale_sitemap":
        sources["sitemap.xml"] = sitemap(master)
    elif case == "noindex_master":
        sources["master.html"] = html.replace("</head>", '<meta name="robots" content="noindex" /></head>')
    elif case == "computed_master":
        sources["master.html"] = html.replace("Master", "{{ content }}")
    elif case == "different_master_canonical":
        sources["master.html"] = html.replace(B, C)
    elif case == "unobserved_master":
        rows = rows[:1]
    elif case == "unforced_shadowed_source":
        sources["old.html"] = html.replace(B, A)
        sources["_redirects"] = "/old /master 301\n"
    elif case == "query_not_in_rule":
        query = A + "?variant=1"
        rows[0] = row(query, final_url=B, canonical=B, redirect_chain=[query], redirect_statuses=[301])
    writes = {}
    def get(path, **kwargs):
        name = unquote(path.split("/contents/", 1)[1])
        if case == "unreadable_config" and name == "_redirects":
            raise OSError("unreadable")
        return {"sha": "" if case == "missing_sha" else "original", "content": "!!!!" if case == "invalid_base64"
                else base64.b64encode(sources[name].encode()).decode()}
    def put(path, **kwargs):
        name = unquote(path.split("/contents/", 1)[1])
        assert kwargs["json_body"]["sha"] == "original" and kwargs["json_body"]["branch"] == "qa"
        writes[name] = base64.b64decode(kwargs["json_body"]["content"]).decode()
        return {"content": {"sha": "patched"}}
    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    def forbidden(*args, **kwargs):
        pytest.fail("No model or heuristic targeting for a verified sitemap redirect")
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, forbidden)
    pairs = [PAIR] if case != "query_not_in_rule" else [{"page": rows[0]["url"], "from": rows[0]["url"], "to": B}]
    prep = prepare(rows, pairs=pairs, paths=list(sources))
    applied = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="baseline", token="unused",
        fix_branch="qa", all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=[A], site_name="site.test",
        file_state={}, max_files=0 if case == "insufficient_budget" else 1, prep=prep, pages=rows,
        index=repo_index.build_repo_index(list(sources)))
    assert not applied["ai_files"]
    if case in {"verified", "multihop", "missing_master_entry"}:
        expected = source.replace(alias, "") if case != "missing_master_entry" else source.replace(alias, entry(B))
        assert writes == {"sitemap.xml": expected}
    else:
        assert not writes
