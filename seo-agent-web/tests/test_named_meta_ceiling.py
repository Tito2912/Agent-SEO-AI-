from __future__ import annotations

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index

OLD = "Description commune aux pages du parcours de validation, afin de verifier les corrections des doublons sans introduire un autre defaut."
LONG = "Page du parcours d'obstacles Noyaru dediee aux liens internes en HTTP : un seul defaut cible pour declencher uniquement la famille https_page_has_internal_links_to_http."


def source(value, *, quote="'", before=False, escaped=True, multiline=False, social=None):
    value = m._js_escape(value, quote) if escaped else value
    props = [f"name: {quote}description{quote}", f"content: {quote}{value}{quote}"]
    if before:
        props.reverse()
    sep = ",\n    " if multiline else ", "
    social = value if social is None else m._js_escape(social, quote)
    return ("<script setup>\nuseHead({\n  title: 'Titre de validation du parcours',\n  meta: [\n    { " + sep.join(props) + " },\n"
            + f"    {{ property: 'og:description', content: {quote}{social}{quote} }},\n"
            + f"    {{ name: 'twitter:description', content: {quote}{social}{quote} }},\n"
            + "    { name: 'viewport', content: 'width=device-width' },\n  ],\n});\n</script>\n<template><main><h1>Controle</h1></main></template>\n")


def description(raw):
    return m._js_unescape(m._find_head_text_value(raw, "description")[1])


@pytest.mark.parametrize("quote", ["'", '"'])
@pytest.mark.parametrize("before", [False, True])
@pytest.mark.parametrize("multiline", [False, True])
def test_written_named_description_is_trimmed_without_damaging_the_object(quote, before, multiline):
    old, new = source(OLD, quote=quote, before=before, multiline=multiline), source(LONG, quote=quote, before=before, multiline=multiline)
    out, notes = m._enforce_length_ceilings(new, old)
    fixed = m._trim_to_ceiling(LONG, 160)
    assert out == source(fixed, quote=quote, before=before, multiline=multiline)
    assert 100 <= len(description(out)) <= 160 and notes
    assert m._refus_de_format("page.vue", out) is None


def test_unchanged_overlong_named_description_belongs_to_another_family():
    old = source(LONG)
    new = old.replace("<h1>Controle</h1>", "<h1>Autre controle</h1>")
    out, notes = m._enforce_length_ceilings(new, old)
    assert out == new and notes == []


def test_a_different_editorial_social_text_is_not_trimmed_or_synchronized():
    other = "Texte social volontairement different et tres long. " * 4
    out, _ = m._enforce_length_ceilings(source(LONG, social=other), source(OLD, social=other))
    assert out == source(m._trim_to_ceiling(LONG, 160), social=other)


def test_a_named_description_expression_is_not_replaced_by_its_literal_prefix():
    old = source(OLD)
    new = source(LONG).replace("content: '" + m._js_escape(LONG, "'") + "'", "content: '" + m._js_escape(LONG, "'") + "' + suffix", 1)
    out, notes = m._enforce_length_ceilings(new, old)
    assert out == new and notes == []


@pytest.mark.parametrize("escaped", [False, True])
def test_actual_duplicate_pipeline_trims_named_values_after_quote_repair(monkeypatch, escaped):
    paths = ["pages/a.vue", "pages/b.vue"]
    old = source(OLD)
    calls, writes = [], []

    def model(**kw):
        path = kw["file_path"]
        calls.append(path)
        # Distinct prefixes survive the ceiling; generated apostrophes may be unescaped.
        return {"patched_content": source("Edition " + path + ". " + LONG, escaped=escaped)}

    monkeypatch.setattr(m, "_openai_generate_file_patch", model)
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: {"sha": "old", "content": base64.b64encode(old.encode()).decode()})
    monkeypatch.setattr(m, "_github_api_put", lambda path, **kw: writes.append((unquote(path.partition("/contents/")[2]), base64.b64decode(kw["json_body"]["content"]).decode())) or {"content": {"sha": "new"}})
    result = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="main", token="unused", fix_branch="fixture-only",
        all_paths=["nuxt.config.ts"] + paths, issue_key="duplicate_meta_descriptions", issue_label="Doublons",
        impacted_urls=["https://fixture.test/a", "https://fixture.test/b"], site_name="fixture.test", file_state={},
        max_files=2, index=repo_index.build_repo_index(["nuxt.config.ts"] + paths), targets_override=paths)
    assert result[0] == paths and result[1] == [] and len(calls) == 2
    assert all(100 <= len(description(raw)) <= 160 and m._refus_de_format(path, raw) is None for path, raw in writes)
    assert len({description(raw) for _, raw in writes}) == 2


def test_named_descriptions_that_collide_after_trimming_are_not_both_written(monkeypatch):
    paths = ["pages/a.vue", "pages/b.vue"]
    old, writes = source(OLD), []
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {"patched_content": source(LONG + " " + kw["file_path"])})
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: {"sha": "old", "content": base64.b64encode(old.encode()).decode()})
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **kw: writes.append(base64.b64decode(kw["json_body"]["content"]).decode()) or {"content": {"sha": "new"}})
    result = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="main", token="unused", fix_branch="fixture-only",
        all_paths=["nuxt.config.ts"] + paths, issue_key="duplicate_meta_descriptions", issue_label="Doublons",
        impacted_urls=["https://fixture.test/a", "https://fixture.test/b"], site_name="fixture.test", file_state={},
        max_files=2, index=repo_index.build_repo_index(["nuxt.config.ts"] + paths), targets_override=paths)
    assert len(writes) == 1 and result[0] == paths[:1] and result[1] == paths[1:]
    assert 100 <= len(description(writes[0])) <= 160


@pytest.mark.parametrize("kind", ["title", "description"])
def test_last_source_guard_cannot_reintroduce_an_overlong_target(monkeypatch, kind):
    old = "<html><head><title>Titre commun au groupe du parcours</title>" + f'<meta name="description" content="{OLD}" /></head></html>'
    value = "Titre distinct apres correction" if kind == "title" else "Description distincte apres correction, pour verifier la derniere barriere de longueur avant une publication du correcteur."
    literal, previous = m._find_head_text_value(old, kind)
    new = old.replace(literal, literal.replace(previous, value))
    writes = []
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {"patched_content": new})
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: {"sha": "old", "content": base64.b64encode(old.encode()).decode()})
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **kw: writes.append(kw) or {"content": {"sha": "new"}})
    monkeypatch.setattr(m, "_requote_toml_apostrophes", lambda raw, path: (raw.replace(value, value + " controles supplementaires" * 8), []))
    result = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="main", token="unused", fix_branch="fixture-only",
        all_paths=["page.html"], issue_key="duplicate_titles" if kind == "title" else "duplicate_meta_descriptions", issue_label="Doublons",
        impacted_urls=["https://fixture.test/page"], site_name="fixture.test", file_state={}, max_files=1, targets_override=["page.html"])
    assert writes == [] and result[0] == [] and result[1] == ["page.html"]
