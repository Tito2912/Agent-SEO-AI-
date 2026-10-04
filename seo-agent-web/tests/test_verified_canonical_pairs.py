"""Canonical destinations are measured, page-local and never guessed by a model."""

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index

SITE = "https://site.test"
KEYS = ("canonical_points_to_redirect", "non_canonical_page_specified_as_canonical_one")
A, B = SITE + "/a", SITE + "/b"
OLD = (SITE + "/old-a", SITE + "/old-b")
DEST = (SITE + "/master-a?edition=FR", SITE + "/master-b?edition=EN")


def row(url, canonical=None, **extra):
    return {"url": url, "final_url": url, "status_code": 200, "content_type": "text/html",
            "canonical": url if canonical is None else canonical, **extra}


def inputs(key):
    pairs = [{"page": page, "from": old, "to": dest} for page, old, dest in zip((A, B), OLD, DEST)]
    rows = [row(page, old) for page, old in zip((A, B), OLD)]
    rows += [row(dest) for dest in DEST]
    rows += [row(old, dest, **({"final_url": dest, "redirect_statuses": [301]} if key == KEYS[0] else {}))
             for old, dest in zip(OLD, DEST)]
    block = {"count": 2, "examples": [A, B], "evidence": {"kind": "url_pairs", "items": pairs}}
    return pairs, rows, block


def prepare(key, rows, block):
    return m._prepare_issue_fix(issue_key=key, issues={key: block}, impacted=[A, B],
        all_paths=["a.html", "b.html", "control.html"], site_name="site.test",
        owner="fixture", repo_name="fixture", branch="qa-baseline", token="unused", pages=rows)


@pytest.mark.parametrize("key", KEYS)
def test_verified_destinations_keep_the_source_page_contract(key):
    pairs, rows, block = inputs(key)
    prep = prepare(key, rows, block)
    assert not prep["refusal"] and prep.get("canonical_pairs") == pairs
    assert not prep["rewriter_ai_fallback"]


@pytest.mark.parametrize("key", KEYS)
def test_missing_evidence_refuses_instead_of_enabling_prompt_only_repair(key):
    _, rows, _ = inputs(key)
    prep = prepare(key, rows, {"count": 2, "examples": [A, B]})
    assert prep["refusal"] and prep["link_rewriter"] is None


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("change", [
    {"status_code": 404}, {"status_code": 503}, {"status_code": "200"},
    {"status_code": True}, {"error": "timeout"}, {"blocked_by_host": True},
    {"content_type": "application/json"}, {"meta_robots": "index,follow;NOINDEX"},
    {"x_robots_tag": "googlebot: noindex"}, {"canonical": SITE + "/other-master"},
])
def test_unhealthy_unknown_or_noncanonical_destination_is_refused(key, change):
    _, rows, block = inputs(key)
    for page in rows:
        if page["url"] in DEST:
            page.update(change)
    prep = prepare(key, rows, block)
    assert prep["refusal"] and prep["link_rewriter"] is None


@pytest.mark.parametrize("key", KEYS)
def test_an_unrelated_good_page_does_not_validate_the_proposed_destination(key):
    _, rows, block = inputs(key)
    for pair in block["evidence"]["items"]:
        pair["to"] = SITE + "/unrelated"
    rows.append(row(SITE + "/unrelated"))
    prep = prepare(key, rows, block)
    assert prep["refusal"] and prep["link_rewriter"] is None


@pytest.mark.parametrize("key", KEYS)
def test_stale_source_observation_cannot_authorize_a_rewrite(key):
    _, rows, block = inputs(key)
    for page in rows:
        if page["url"] in (A, B):
            page["canonical"] = SITE + "/different"
    prep = prepare(key, rows, block)
    assert prep["refusal"] and prep["link_rewriter"] is None


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("reject_first", [False, True])
def test_real_pipeline_does_not_apply_another_pages_pair(monkeypatch, key, reject_first):
    pairs, rows, block = inputs(key)
    if reject_first:
        rows[2]["meta_robots"] = "noindex"
    sources = {name: '<html><head><title>Test</title><link rel="canonical" href="' + old + '" />'
        + '<link rel="alternate" hreflang="fr" href="' + old + '" />'
        + '<script>const example = {canonical:"' + neighbor + '"};</script></head><body><h1>Test</h1></body></html>'
        for name, old, neighbor in zip(("a.html", "b.html"), OLD, reversed(OLD))}
    sources["control.html"] = sources["a.html"]
    written = {}
    monkeypatch.setattr(m, "_github_tarball_grep", lambda *args, **kwargs: list(sources))
    monkeypatch.setattr(m, "_github_api_get", lambda path, **kw: {"sha": "baseline-blob",
        "content": base64.b64encode(sources[unquote(path.split("/contents/", 1)[1])].encode()).decode()})

    def put(path, **kwargs):
        assert kwargs["json_body"]["branch"] == "qa-correction"
        written[unquote(path.split("/contents/", 1)[1])] = base64.b64decode(kwargs["json_body"]["content"]).decode()
        return {"content": {"sha": "patched"}}

    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kwargs: pytest.fail("No model permitted"))
    prep = prepare(key, rows, block)
    assert not prep["refusal"]
    assert prep["canonical_pairs"] == (pairs[1:] if reject_first else pairs)
    assert bool(prep["canonical_refusals"]) == reject_first
    applied = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="qa-baseline", token="unused",
        fix_branch="qa-correction", all_paths=list(sources), issue_key=key, issue_label=key,
        impacted=[A, B], site_name="site.test", file_state={}, max_files=4, prep=prep, pages=rows,
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=False)
    assert set(written) == ({"b.html"} if reject_first else {"a.html", "b.html"}) and not applied["ai_files"]
    if reject_first:
        assert applied["skipped"] and not applied["error"]
    for name, pair in zip(("a.html", "b.html"), pairs):
        if name not in written:
            continue
        assert written[name] == sources[name].replace('rel="canonical" href="' + pair["from"],
                                                     'rel="canonical" href="' + pair["to"])


@pytest.mark.parametrize(("old", "new", "value", "expected"), [
    (SITE + "/old", "https://other.test/new", "/old", "https://other.test/new"),
    ("https://other.test/old", SITE + "/new", "/old", "/old"),
    (SITE.replace("https:", "http:") + "/old", SITE + "/new", "/old", "/old"),
    (SITE + "/old?x=One", SITE + "/new?x=Two", "/old?x=One", "/new?x=Two"),
])
def test_relative_canonical_variants_respect_the_source_origin(old, new, value, expected):
    source = '<link rel="canonical" href="' + value + '" />'
    output, count = m._rewrite_head_url_values(source,
        [{"page": A, "from": old, "to": new}], canonical_only=True)
    assert output == source.replace(value, expected) and count == int(value != expected)


def test_unresolved_canonical_source_does_not_spend_on_ai_targeting(monkeypatch):
    monkeypatch.setattr(m, "_github_tarball_grep", lambda *args, **kwargs: ["component.jsx"])
    monkeypatch.setattr(m, "_github_api_get", lambda *args, **kwargs: {"sha": "old", "content": base64.b64encode(
        ('<link rel="canonical" href="' + OLD[0] + '" />').encode()).decode()})
    for name in ("_ai_map_urls_to_files", "_ai_pick_repo_files", "_openai_generate_file_patch", "_github_api_put"):
        monkeypatch.setattr(m, name, lambda *args, **kwargs: pytest.fail("No AI or PUT permitted"))
    patched, _, _, ai_files = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="baseline",
        token="unused", fix_branch="correction", all_paths=["component.jsx"], issue_key=KEYS[0], issue_label=KEYS[0],
        impacted_urls=[A], site_name="site.test", file_state={}, evidence=[OLD[0]], allow_ai_targeting=True,
        canonical_pairs=[{"page": A, "from": OLD[0], "to": DEST[0]}],
        index=repo_index.build_repo_index(["component.jsx"]))
    assert not patched and not ai_files
