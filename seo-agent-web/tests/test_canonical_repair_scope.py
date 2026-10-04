"""A shared dead target must never merge unrelated page canonicals."""

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index

SITE = "https://site.test"
A, B, DEAD = SITE + "/a", SITE + "/b", SITE + "/Gone/?q=One"
KEY = "canonical_points_to_4xx"


def block(urls=(A, B)):
    return {"count": len(urls), "examples": [u + " -> " + DEAD for u in urls]}


def pages(target=DEAD, code=404):
    return [{"url": A, "status_code": 200}, {"url": B, "status_code": 200},
            {"url": target, "status_code": code}]


def test_two_sources_of_one_dead_target_keep_two_distinct_destinations():
    pairs, _ = m._canonical_self_pairs(block(), pages())
    assert pairs == [{"page": u, "from": DEAD, "to": u} for u in (A, B)]


@pytest.mark.parametrize("target", [DEAD.replace("https:", "http:"), DEAD.replace("Gone", "gone"),
    DEAD.replace("/?", "?"), DEAD.replace("One", "one"), DEAD.replace("q=", "Q=")])
def test_a_different_measured_url_is_not_evidence_of_absence(target):
    pairs, notes = m._canonical_self_pairs(block((A,)), pages(target))
    assert not pairs and notes


@pytest.mark.parametrize("codes", [(404, 200), (200, 404), (410, 503), (404, "200")])
def test_conflicting_or_untyped_observations_are_not_definitive_absence(codes):
    rows = pages()[:2] + [{"url": DEAD, "status_code": code} for code in codes]
    pairs, notes = m._canonical_self_pairs(block((A,)), rows)
    assert not pairs and notes


def test_a_redirect_response_is_not_the_status_of_its_original_url():
    rows = pages()[:2] + [{"url": DEAD, "final_url": SITE + "/actual-missing", "status_code": 404}]
    assert not m._canonical_self_pairs(block((A,)), rows)[0]


@pytest.mark.parametrize("code", [401, 403, 429, 500, 503, None, True, "404"])
def test_temporary_protected_or_unknown_targets_remain_refused(code):
    assert not m._canonical_self_pairs(block((A,)), pages(code=code))[0]


def test_host_case_and_fragments_do_not_invalidate_the_same_observation():
    pairs, _ = m._canonical_self_pairs(block((A,)), pages(DEAD.replace("site.test", "SITE.TEST") + "#section"))
    assert len(pairs) == 1


def apply(monkeypatch, *, aliases=False, stale=False):
    urls = (A, A + "?different=1") if aliases else (A, B)
    sources = {name + ".html": '<!doctype html><html><head><title>A valid test page</title>'
        + '<link rel="canonical" href="' + (u if stale else DEAD) + '" />'
        + '<link rel="alternate" hreflang="fr" href="' + DEAD + '" />'
        + '</head><body><h1>Test</h1><a href="' + DEAD + '">Keep</a></body></html>'
        for name, u in zip(("a", "b"), (A, B))}
    written = {}
    monkeypatch.setattr(m, "_github_tarball_grep", lambda *a, **kw: list(sources))
    monkeypatch.setattr(m, "_github_api_get", lambda path, **kw: {"sha": "old", "content": base64.b64encode(
        sources[unquote(path.split("/contents/", 1)[1])].encode()).decode()})

    def put(path, **kw):
        assert kw["json_body"]["branch"] == "qa-correction"
        written[unquote(path.split("/contents/", 1)[1])] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "new"}}

    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: pytest.fail("No AI permitted"))
    prep = m._prepare_issue_fix(issue_key=KEY, issues={KEY: block(urls)}, impacted=list(urls),
        all_paths=list(sources), site_name="site.test", owner="fixture", repo_name="fixture", branch="qa-baseline",
        token="unused", pages=pages())
    assert not prep["refusal"]
    result = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="qa-baseline", token="unused",
        fix_branch="qa-correction", all_paths=list(sources), issue_key=KEY, issue_label=KEY, impacted=list(urls),
        site_name="site.test", file_state={}, max_files=4, prep=prep, pages=pages(),
        index=repo_index.build_repo_index(list(sources)), allow_ai_targeting=False)
    return result, written, sources


def test_real_pipeline_binds_each_dead_canonical_to_its_own_page(monkeypatch):
    result, written, sources = apply(monkeypatch)
    assert set(result["patched"]) == set(sources) and not result["ai_files"]
    for name, own in (("a.html", A), ("b.html", B)):
        assert written[name] == sources[name].replace('rel="canonical" href="' + DEAD,
                                                     'rel="canonical" href="' + own)


def test_two_different_page_urls_resolving_to_one_file_are_not_guessed(monkeypatch):
    result, written, _ = apply(monkeypatch, aliases=True)
    assert not written and not result["patched"] and result["skipped"]


def test_stale_repaired_canonical_does_not_rewrite_the_neighboring_alternate(monkeypatch):
    result, written, _ = apply(monkeypatch, stale=True)
    assert not written and not result["patched"]


@pytest.mark.parametrize("source", [
    '<link rel="canonical" href="' + DEAD + '" /><link rel="alternate" hreflang="fr" href="' + DEAD + '" />',
    "useHead({link:[{rel:'canonical',href:'" + DEAD + "'},{rel:'alternate',hreflang:'fr',href:'" + DEAD + "'}]});",
    "const canonical = '" + DEAD + "'; const languages = {fr: '" + DEAD + "'};",
    "metadata = {alternates:{canonical:'" + DEAD + "',languages:{fr:'" + DEAD + "'}}};",
])
def test_canonical_only_mode_keeps_alternate_declarations(source):
    output, count = m._rewrite_head_url_values(source, [{"from": DEAD, "to": A}], canonical_only=True)
    assert count == 1 and output == source.replace(DEAD, A, 1)


@pytest.mark.parametrize(("value", "expected"), [
    ("/Gone/?q=One", "/a?language=FR#part"),
    ("/Gone/", "/Gone/"),
    ("/Gone/?q=Two", "/Gone/?q=Two"),
])
def test_relative_canonical_keeps_query_identity_and_destination_components(value, expected):
    source = '<link rel="canonical" href="' + value + '" />'
    output, count = m._rewrite_head_url_values(source,
        [{"from": DEAD, "to": A + "?language=FR#part"}], canonical_only=True)
    assert output == source.replace(value, expected) and count == int(value != expected)
