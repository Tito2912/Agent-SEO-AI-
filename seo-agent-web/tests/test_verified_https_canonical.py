"""A protocol-only canonical upgrade still needs measured, page-local evidence."""

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index

KEY = "canonical_from_https_to_http"
SOURCE = "https://site.test/a?edition=FR"
OLD = "http://site.test/a?edition=FR"


def row(url, canonical, **extra):
    return {"url": url, "final_url": url, "status_code": 200,
            "content_type": "text/html", "canonical": canonical, **extra}


def inputs(self_target=True):
    old = OLD if self_target else "http://master.test/b?edition=FR"
    dest = "https:" + old[5:]
    pairs = [{"page": SOURCE, "from": old, "to": dest}]
    rows = [row(SOURCE, old)]
    if not self_target:
        rows.append(row(dest, dest))
    block = {"count": 1, "examples": [SOURCE], "evidence": {"kind": "url_pairs", "items": pairs}}
    return pairs, rows, block


def prepare(rows, block):
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: block}, impacted=[SOURCE],
        all_paths=["a.html"], site_name="site.test", owner="fixture", repo_name="fixture",
        branch="qa-baseline", token="unused", pages=rows)


@pytest.mark.parametrize("self_target", [True, False])
def test_exact_observed_upgrade_is_page_bound_and_model_free(self_target):
    pairs, rows, block = inputs(self_target)
    prep = prepare(rows, block)
    assert not prep["refusal"] and prep.get("canonical_pairs") == pairs
    assert not prep["rewriter_ai_fallback"]


@pytest.mark.parametrize("case", ["missing_pairs", "missing_pages", "unobserved_destination", "stale_source"])
def test_missing_or_stale_evidence_refuses(case):
    _, rows, block = inputs(False)
    if case == "missing_pairs":
        block.pop("evidence")
    elif case == "missing_pages":
        rows = []
    elif case == "unobserved_destination":
        rows = rows[:1]
    else:
        rows[0]["canonical"] = "http://site.test/stale"
    prep = prepare(rows, block)
    assert prep["refusal"] and prep["link_rewriter"] is None


@pytest.mark.parametrize("self_target", [True, False])
@pytest.mark.parametrize("change", [
    {"status_code": 404}, {"status_code": 503}, {"status_code": "200"}, {"status_code": True},
    {"error": "timeout"}, {"blocked_by_host": True}, {"content_type": "application/json"},
    {"meta_robots": "index,follow;NOINDEX"}, {"x_robots_tag": "googlebot: noindex"},
    {"canonical": "https://site.test/another-master"}, {"final_url": "https://site.test/redirected"},
])
def test_unhealthy_noncanonical_or_redirected_destination_refuses(self_target, change):
    _, rows, block = inputs(self_target)
    rows[-1].update(change)
    prep = prepare(rows, block)
    assert prep["refusal"] and prep["link_rewriter"] is None


@pytest.mark.parametrize("destination", [
    "https://other.test/a?edition=FR", "https://site.test/A?edition=FR", "https://site.test/a/?edition=FR",
    "https://site.test/a?edition=fr", "https://site.test:443/a?edition=FR", "http://site.test/a?edition=FR",
])
def test_forged_upgrade_cannot_change_authority_path_query_or_port(destination):
    _, rows, block = inputs()
    block["evidence"]["items"][0]["to"] = destination
    rows.append(row(destination, destination))
    prep = prepare(rows, block)
    assert prep["refusal"] and prep["link_rewriter"] is None


def test_contradictory_observation_refuses_and_duplicates_do_not_duplicate_pairs():
    pairs, rows, block = inputs()
    block["evidence"]["items"] = list(pairs) + pairs
    assert prepare(rows, block).get("canonical_pairs") == pairs
    rows.append(row(SOURCE, "https://site.test/other"))
    assert prepare(rows, block)["refusal"]


@pytest.mark.parametrize("self_target", [True, False])
def test_http_redirect_to_the_exact_https_destination_is_compatible(self_target):
    pairs, rows, block = inputs(self_target)
    pair = pairs[0]
    rows.append(row(pair["from"], pair["from"] if self_target else pair["to"],
                    final_url=pair["to"], redirect_statuses=[301]))
    assert prepare(rows, block).get("canonical_pairs") == pairs


@pytest.mark.parametrize("change", [{"final_url": "https://site.test/different", "redirect_statuses": [301]},
                                    {"canonical": "https://site.test/different"}])
def test_observed_http_master_cannot_be_overridden_by_a_protocol_guess(change):
    _, rows, block = inputs(False)
    pair = block["evidence"]["items"][0]
    previous = row(pair["from"], pair["from"])
    previous.update(change)
    rows.append(previous)
    prep = prepare(rows, block)
    assert prep["refusal"] and prep["link_rewriter"] is None


@pytest.mark.parametrize("reject_first", [False, True])
def test_real_pipeline_only_changes_each_sources_literal_canonical(monkeypatch, reject_first):
    urls = ["https://site.test/a", "https://site.test/b"]
    olds = [u.replace("https:", "http:") for u in urls]
    pairs = [{"page": u, "from": old, "to": u} for u, old in zip(urls, olds)]
    rows = [row(u, old) for u, old in zip(urls, olds)]
    if reject_first:
        rows[0]["meta_robots"] = "noindex"
    sources = {name: '<html><head><link rel="canonical" href="' + old + '" />'
        + '<link rel="alternate" hreflang="fr" href="' + old + '" />'
        + '<meta property="og:url" content="' + old + '" />'
        + '</head><body><a href="' + old + '">HTTP control</a></body></html>'
        for name, old in zip(("a.html", "b.html"), olds)}
    sources["control.html"] = sources["a.html"]
    written = {}
    monkeypatch.setattr(m, "_github_tarball_grep", lambda *a, **k: list(sources))
    monkeypatch.setattr(m, "_github_api_get", lambda path, **k: {"sha": "old-blob", "content":
        base64.b64encode(sources[unquote(path.split("/contents/", 1)[1])].encode()).decode()})

    def put(path, **kwargs):
        written[unquote(path.split("/contents/", 1)[1])] = base64.b64decode(kwargs["json_body"]["content"]).decode()
        return {"content": {"sha": "new-blob"}}

    monkeypatch.setattr(m, "_github_api_put", put)
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files"):
        monkeypatch.setattr(m, name, lambda *a, **k: pytest.fail("No model or AI targeting permitted"))
    prep = m._prepare_issue_fix(issue_key=KEY, issues={KEY: {"count": 2, "examples": urls,
        "evidence": {"kind": "url_pairs", "items": pairs}}}, impacted=urls, all_paths=list(sources),
        site_name="site.test", owner="fixture", repo_name="fixture", branch="baseline", token="unused", pages=rows)
    assert prep.get("canonical_pairs") == (pairs[1:] if reject_first else pairs)
    applied = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="baseline", token="unused",
        fix_branch="qa-correction", all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=urls,
        site_name="site.test", file_state={}, max_files=4, prep=prep, pages=rows,
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=True)
    assert set(written) == ({"b.html"} if reject_first else {"a.html", "b.html"}) and not applied["ai_files"]
    for name, pair in zip(("a.html", "b.html"), pairs):
        if name in written:
            assert written[name] == sources[name].replace('rel="canonical" href="' + pair["from"],
                                                          'rel="canonical" href="' + pair["to"])


@pytest.mark.parametrize("path,source", [
    ("component.jsx", '<link rel="canonical" href="' + OLD + '" />'),
    ("a.html", '<link rel="canonical" href={getCanonical()} />'),
])
def test_unknown_route_or_generated_canonical_does_not_fall_back_to_model(monkeypatch, path, source):
    pairs, rows, block = inputs()
    monkeypatch.setattr(m, "_github_tarball_grep", lambda *a, **k: [path])
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **k: {"sha": "old", "content":
        base64.b64encode(source.encode()).decode()})
    for name in ("_github_api_put", "_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files"):
        monkeypatch.setattr(m, name, lambda *a, **k: pytest.fail("No writes or models without a bound literal"))
    prep = prepare(rows, block)
    assert prep["canonical_pairs"] == pairs
    applied = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="baseline", token="unused",
        fix_branch="qa-correction", all_paths=[path], issue_key=KEY, issue_label=KEY, impacted=[SOURCE],
        site_name="site.test", file_state={}, max_files=1, prep=prep, pages=rows,
        index=repo_index.build_repo_index([path]), allow_ai_targeting=True)
    assert not applied["patched"] and not applied["ai_files"]
