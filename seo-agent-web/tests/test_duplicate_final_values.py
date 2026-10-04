from __future__ import annotations

import base64
import html

import pytest

from backend import app as m, repo_index


def run(monkeypatch, values, *, kind="title", transform=None, pages=None, index=None, impacted=None, state=None):
    source = '<html><head><title>Ancien titre commun aux pages</title>'
    source += '<meta name="description" content="' + "Ancienne description commune. " * 4 + '" /></head></html>'
    calls, writes = [], []
    candidates = iter(values)

    def generate(**kw):
        calls.append(kw)
        value = next(candidates, values[-1])
        literal, _ = m._find_head_text_value(source, kind)
        replacement = f'<title>{value}</title>' if kind == "title" else f'<meta name="description" content="{value}" />'
        return {"patched_content": source.replace(literal, replacement)}

    monkeypatch.setattr(m, "_openai_generate_file_patch", generate)
    monkeypatch.setattr(m, "_resolve_issue_targets", lambda **kw: ["a.html", "b.html"])
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: {
        "content": base64.b64encode(source.encode()).decode(), "sha": "old"})
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **kw: writes.append(
        base64.b64decode(kw["json_body"]["content"]).decode()) or {"content": {"sha": "new"}})
    if transform:
        monkeypatch.setattr(m, "_requote_toml_apostrophes", lambda content, path: (transform(content), []))
    result = m._deep_patch_issue_files(
        owner="fixture", repo_name="fixture", branch="main", token="unused", fix_branch="fixture-only",
        all_paths=["a.html", "b.html"], issue_key="duplicate_titles" if kind == "title" else "duplicate_meta_descriptions",
        issue_label="Duplicate", impacted_urls=impacted or ["https://fixture.test/a", "https://fixture.test/b"],
        site_name="fixture.test", file_state=state or {}, max_files=2, pages=pages, index=index)
    texts = [html.unescape(m._find_head_text_value(w, kind)[1]).strip() for w in writes]
    return calls, texts, result


@pytest.mark.parametrize("kind,prefix", [
    ("title", "Guide de validation des titres et descriptions du parcours Noyaru " * 2),
    ("description", "Cette edition presente les titres et descriptions du parcours Noyaru et les controles avant publication. " * 3),
])
def test_values_that_collide_after_the_ceiling_are_never_both_written(monkeypatch, kind, prefix):
    _, texts, result = run(monkeypatch, [prefix + "edition A", prefix + "edition B"], kind=kind)
    assert len(texts) == len(set(texts)) == 1
    assert result[0] == ["a.html"] and result[1] == ["b.html"]


def test_a_shorter_but_valid_unique_retry_beats_a_longer_duplicate(monkeypatch):
    first = "Titre choisi pour la premiere page de validation"
    valid = "Titre distinct pour la deuxieme page"
    calls, texts, result = run(monkeypatch, [first, first, valid])
    assert texts == [first, valid]
    assert len(calls) == 3 and result[1] == []


def test_entity_encoded_and_literal_titles_are_compared_as_rendered(monkeypatch):
    first = "Titres & descriptions : validation du parcours"
    _, texts, _ = run(monkeypatch, [first, first.replace("&", "&amp;")])
    assert len(texts) == len(set(texts)) == 1


def test_uniqueness_is_checked_again_after_the_last_source_guard(monkeypatch):
    first = "Titre valide de la premiere page"
    second = "Titre distinct de la seconde page"
    _, texts, result = run(monkeypatch, [first, second], transform=lambda c: c.replace(second, first))
    assert texts == [first]
    assert result[1] == ["b.html"]


def test_a_shorter_retry_still_below_the_floor_is_not_written(monkeypatch):
    first = "Titre valide de la premiere page"
    _, texts, _ = run(monkeypatch, [first, first, "Court", "Court"])
    assert texts == [first]


@pytest.mark.parametrize("kind,value", [("title", "Titre reserve par une autre page du site"),
                                       ("description", "Une description reservee par une autre page du parcours. " * 2)])
def test_existing_values_outside_the_target_files_cannot_be_reused(monkeypatch, kind, value):
    rows = [{"url": "https://fixture.test/existing", "status_code": 200, "content_type": "text/html",
             "title" if kind == "title" else "meta_description": value}]
    _, texts, result = run(monkeypatch, [value], kind=kind, pages=rows)
    assert texts == [] and result[1] == ["a.html", "b.html"]


def test_an_impacted_but_capped_page_still_reserves_its_current_value(monkeypatch):
    value = "Titre reserve par une page exclue du plafond"
    rows = [{"url": "https://fixture.test/existing", "status_code": 200, "content_type": "text/html", "title": value}]
    index = repo_index.build_repo_index(["a.html", "b.html", "existing.html"])
    _, texts, result = run(monkeypatch, [value], pages=rows, index=index,
                           impacted=["https://fixture.test/a", "https://fixture.test/b", "https://fixture.test/existing"])
    assert texts == [] and result[1] == ["a.html", "b.html"]


def test_values_written_by_an_earlier_family_are_reserved_across_the_batch(monkeypatch):
    value = "Titre deja ecrit par la famille precedente"
    state = {"existing.html": {"content": f'<html><head><title>{value}</title></head></html>', "sha": "new"}}
    _, texts, result = run(monkeypatch, [value], state=state)
    assert texts == [] and result[1] == ["a.html", "b.html"]
