from __future__ import annotations

import copy

import pytest

from backend import app as m, repo_index

SITE = "https://fixture.test"
EN, FR, DE = (SITE + "/" + code + "/" for code in ("en", "fr", "de"))


def pages():
    output = []
    for url, code in ((EN, "en"), (FR, "fr"), (DE, "de")):
        output.append({"url": url, "final_url": url, "status_code": 200, "content_type": "text/html",
                       "canonical": url, "lang": code, "hreflang": {"en": EN, "fr": FR, "de": DE}})
    for row in output[1:]:
        row["hreflang"]["x-default"] = FR
    return output


def prepare(rows, monkeypatch=None):
    if monkeypatch:
        monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: pytest.fail("GitHub read"))
    return m._prepare_issue_fix(issue_key="x_default_hreflang_missing", issues={}, impacted=[EN],
                                all_paths=["en/index.html"], site_name="fixture.test", owner="fixture",
                                repo_name="fixture", branch="main", token="unused", pages=rows)


def test_existing_group_default_is_reused_not_the_english_self_url():
    rows = pages()
    original = copy.deepcopy(rows)
    assert m._x_default_group_targets([EN], rows) == ({EN: FR}, [])
    assert rows == original
    prep = prepare(rows)
    assert prep["x_default_targets"] == {EN: FR}
    assert f"{EN} : x-default = {FR}" in prep["extra_hint"]


def test_different_groups_keep_their_own_default_without_sitewide_majority_voting():
    other = copy.deepcopy(pages())
    for row in other:
        for key in ("url", "final_url", "canonical"):
            row[key] = row[key].replace("fixture.test", "second.test")
        row["hreflang"] = {c: h.replace("fixture.test", "second.test") for c, h in row["hreflang"].items()}
        if "x-default" in row["hreflang"]:
            row["hreflang"]["x-default"] = "https://second.test/de/"
    expected = {EN: FR, "https://second.test/en/": "https://second.test/de/"}
    assert m._x_default_group_targets(list(expected), pages() + other) == (expected, [])


def test_conflicting_peer_defaults_refuse_the_batch_before_any_file_read(monkeypatch):
    rows = pages()
    rows[2]["hreflang"]["x-default"] = DE
    assert m._x_default_group_targets([EN], rows) == ({}, [EN])
    prep = prepare(rows, monkeypatch)
    assert prep["refusal"] and "contradictoires" in prep["refusal"]


@pytest.mark.parametrize("change", [{"status_code": 404}, {"meta_robots": "noindex, follow"},
                                    {"x_robots_tag": "noindex"}, {"canonical": DE},
                                    {"x_robots_tag": "googlebot: noindex"},
                                    {"x_robots_tag": "noindex\n"}, {"meta_robots": "noindex; follow"},
                                    {"error": "not audited"}, {"blocked_by_host": True}])
def test_a_known_unusable_default_is_not_propagated(change):
    rows = pages()
    rows[1].update(change)
    assert m._x_default_group_targets([EN], rows) == ({}, [EN])


def test_missing_report_or_unobserved_policy_does_not_invent_a_default():
    assert m._x_default_group_targets([EN], None) == ({}, [])
    rows = pages()
    for row in rows:
        row["hreflang"].pop("x-default", None)
    assert m._x_default_group_targets([EN], rows) == ({}, [])


def test_unilateral_or_unrelated_annotations_cannot_set_the_group_policy():
    rows = pages()
    for row in rows[1:]:
        row["hreflang"].pop("en")
    assert m._x_default_group_targets([EN], rows) == ({}, [])


def test_path_case_is_not_collapsed_while_resolving_group_defaults():
    rows = pages()
    rows[2]["hreflang"]["x-default"] = SITE + "/FR/"
    assert m._x_default_group_targets([EN], rows) == ({}, [EN])


def test_malformed_source_urls_do_not_become_group_policy():
    rows = pages()
    bad = "javascript:alert(1)"
    rows[0].update(url=bad, final_url=bad, canonical=bad)
    for row in rows:
        row["hreflang"]["en"] = bad
    assert m._x_default_group_targets([bad], rows) == ({}, [])


def test_unsupported_js_unicode_escapes_cannot_fool_the_destination_guard():
    source = """useHead({
  link: [
    { rel: 'alternate', hreflang: 'x-default', href: 'https://fixture.test/\\u0041' }
  ]
});
"""
    assert not m._x_default_matches_target(source, SITE + "/u0041")


@pytest.mark.parametrize("stack", ["hugo", "nuxt"])
def test_literal_defaults_are_verified_in_both_exercised_source_shapes(stack):
    from ops.gauntlet.hreflang_cycle import seed_pages

    source = next(v for k, v in seed_pages(stack).items() if "-en." in k)
    expected = f"https://noyaru-stack-{stack}.netlify.app/gauntlet/qa-reciprocal-fr/"
    assert m._x_default_matches_target(source, expected)
    assert not m._x_default_matches_target(source, expected.replace("-fr/", "-en/"))


@pytest.mark.parametrize("content", [
    f'<link href="{EN}" hreflang="x-default" rel="alternate" />',
    f'<link href="{FR}" href="{EN}" hreflang="x-default" rel="alternate" />',
    f'<link href="{FR}" hreflang="x-default" rel="alternate" />\n' * 2,
    f'<link href="{FR}" hreflang="x-default" rel="stylesheet" />',
    "useHead({ link: [{ rel: 'alternate', hreflang: 'x-default', href: computed }] });",
])
def test_wrong_duplicate_or_unverifiable_defaults_are_rejected(content):
    assert not m._x_default_matches_target(content, FR)


@pytest.mark.parametrize("destination,committed", [(EN, False), (FR, True)])
def test_ai_output_is_checked_before_any_github_write(monkeypatch, destination, committed):
    path = "en/index.html"
    source = f'<link rel="canonical" href="{EN}" />\n<link rel="alternate" hreflang="en" href="{EN}" />\n'
    proposed = source + f'<link rel="alternate" hreflang="x-default" href="{destination}" />\n'
    writes = []
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {"patched_content": proposed})
    monkeypatch.setattr(m, "_resolve_issue_targets", lambda **kw: [path])
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **kw: writes.append(kw) or {"content": {"sha": "new"}})
    result = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="main", token="unused",
                                      fix_branch="fixture-only", all_paths=["index.html", path],
                                      issue_key="x_default_hreflang_missing", issue_label="x-default", impacted_urls=[EN],
                                      site_name="fixture.test", file_state={path: {"content": source, "sha": "old"}},
                                      max_files=1, index=repo_index.build_repo_index(["index.html", path]),
                                      x_default_targets={EN: FR})
    assert bool(writes) is committed
    assert result[0] == ([path] if committed else [])
    assert result[1] == ([] if committed else [path])


def test_the_group_policy_reaches_the_shared_apply_path(monkeypatch):
    captured = {}
    monkeypatch.setattr(m, "_deep_patch_issue_files", lambda **kw: captured.update(kw) or ([], [], [], []))
    m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="main", token="unused",
                                fix_branch="fixture-only", all_paths=["en/index.html"],
                                issue_key="x_default_hreflang_missing", issue_label="x-default", impacted=[EN],
                                site_name="fixture.test", file_state={}, max_files=1, prep=prepare(pages()),
                                pages=pages(), index=None)
    assert captured["x_default_targets"] == {EN: FR}
