"""A matched edit can still leave an invalid head literal: retry once, never bypass it."""

import base64

import pytest

from backend import app as m


PATH = "pages/gauntlet/link-http.vue"
OLD = "<script setup>\nuseHead({\n  title: 'Original fixture title',\n  htmlAttrs: { lang: 'fr' },\n});\n</script>\n<template><main>Original body</main></template>\n"
GOOD = OLD.replace("Original fixture title", "Unique fixture title")
BAD = GOOD.replace("title: 'Unique fixture title',", "title: 'Unique fixture title','")
KW = dict(file_path=PATH, file_content=OLD, issue_key="duplicate_titles", issue_label="Duplicate titles",
          url="https://fixture.test/", site_name="fixture.test", occurrences_hint="Keep other fields.",
          model_override="test-model")


def fake_paths(monkeypatch, targeted, full):
    calls = []
    monkeypatch.setattr(m, "_patch_via_edits", lambda **kw: calls.append(("edits", kw)) or targeted)
    monkeypatch.setattr(m, "_patch_via_full_file", lambda **kw: calls.append(("full", kw)) or full)
    return calls


@pytest.mark.parametrize("bad", [BAD, BAD.replace("'", '"'),
                               GOOD.replace("lang: 'fr'", "lang: true false")])
def test_invalid_matched_edit_gets_one_full_file_fallback(monkeypatch, bad):
    calls = fake_paths(monkeypatch, {"patched_content": bad}, {"patched_content": GOOD})
    result = m._openai_generate_file_patch(**KW)
    assert result["patched_content"] == GOOD
    assert [kind for kind, _ in calls] == ["edits", "full"]
    assert calls[1][1]["file_content"] == OLD
    assert calls[1][1]["model_override"] == KW["model_override"]
    assert calls[1][1]["occurrences_hint"].startswith(KW["occurrences_hint"])
    assert calls[1][1]["occurrences_hint"] != KW["occurrences_hint"]


@pytest.mark.parametrize("candidate", [GOOD, GOOD.replace("Unique fixture title", "Le parcours d'obstacles"),
                                       GOOD.replace("title: 'Unique fixture title',", "title: 'Unique fixture title'"),
                                       GOOD.replace("title: 'Unique fixture title',", "title: makeTitle(),")])
def test_valid_or_mechanically_repairable_edit_does_not_spend_a_fallback(monkeypatch, candidate):
    targeted = {"patched_content": candidate}
    calls = fake_paths(monkeypatch, targeted, {"patched_content": GOOD})
    assert m._openai_generate_file_patch(**KW) is targeted
    assert [kind for kind, _ in calls] == ["edits"]


@pytest.mark.parametrize("full", [{}, {"no_change": True}, {"patched_content": BAD}])
def test_bad_full_file_is_not_retried_indefinitely(monkeypatch, full):
    calls = fake_paths(monkeypatch, {"patched_content": BAD}, full)
    m._openai_generate_file_patch(**KW)
    assert [kind for kind, _ in calls] == ["edits", "full"]


def test_an_unmatched_edit_still_uses_the_existing_fallback(monkeypatch):
    calls = fake_paths(monkeypatch, {}, {"patched_content": GOOD})
    assert m._openai_generate_file_patch(**KW)["patched_content"] == GOOD
    assert calls[1][1] == KW


def test_no_change_does_not_trigger_generation(monkeypatch):
    targeted = {"no_change": True}
    calls = fake_paths(monkeypatch, targeted, {"patched_content": GOOD})
    assert m._openai_generate_file_patch(**KW) is targeted
    assert len(calls) == 1


@pytest.mark.parametrize("full,expected", [(GOOD, True), (BAD, False)])
def test_final_format_guard_still_decides_before_github_put(monkeypatch, full, expected):
    calls = fake_paths(monkeypatch, {"patched_content": BAD}, {"patched_content": full})
    puts = []
    monkeypatch.setattr(m, "_github_api_put", lambda path, **kw: puts.append(kw["json_body"]) or
                        {"content": {"sha": "next"}})
    patched, skipped, _, _ = m._deep_patch_issue_files(
        owner="fixture", repo_name="fixture", branch="main", token="test", fix_branch="qa",
        all_paths=[PATH], issue_key="duplicate_titles", issue_label="Duplicate titles", impacted_urls=[],
        site_name="fixture.test", file_state={PATH: {"sha": "old", "content": OLD}}, max_files=1,
        targets_override=[PATH], allow_ai_targeting=False)
    assert bool(patched) is expected
    assert bool(puts) is expected
    assert skipped == ([] if expected else [PATH])
    assert [kind for kind, _ in calls] == ["edits", "full"]
    if puts:
        assert base64.b64decode(puts[0]["content"]).decode() == GOOD
        assert m._refus_de_format(PATH, GOOD) is None


def test_non_js_content_and_json_validation_keep_their_existing_contract(monkeypatch):
    calls = fake_paths(monkeypatch, {"patched_content": '{"invalid": unquoted}'}, {})
    result = m._openai_generate_file_patch(**{**KW, "file_path": "settings.json", "file_content": '{"valid": true}'})
    assert result["error"] == "invalid_syntax"
    assert len(calls) == 1
