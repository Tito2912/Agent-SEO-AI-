from __future__ import annotations

from pathlib import Path

import pytest

from backend import app as m


def source(head="", body="<title>Ce titre dans le corps ne doit pas etre lu</title>"):
    return "+++\ntitle = 'Parcours'\nraw_head = '''\n" + head + "\n'''\nraw_body = '''\n" + body + "\n'''\n+++\n"


def test_a_missing_hugo_raw_head_title_is_not_the_short_front_matter_placeholder():
    old = source('<meta name="viewport" content="width=device-width" />')
    assert m._find_head_text_value(old, "title") is None
    title = "Un titre complet pour la page de validation Hugo"
    new = old.replace("raw_head = '''\n", "raw_head = '''\n<title>" + title + "</title>\n", 1)
    assert m._find_head_text_value(new, "title") == ("<title>" + title + "</title>", title)


def test_the_existing_raw_head_title_wins_and_its_replacement_is_bounded():
    value = "Le titre effectivement servi par cette page Hugo"
    text = source(f"<title>{value}</title>")
    literal, actual = m._find_head_text_value(text, "title")
    assert actual == value and literal in text
    patched = text.replace(literal, "<title>Un autre titre valide pour cette page</title>", 1)
    assert "title = 'Parcours'" in patched
    assert "Ce titre dans le corps" in patched


def test_description_uses_raw_head_instead_of_an_unserved_front_matter_value():
    text = source('<meta name="description" content="La vraie description dans le head" />')
    text = text.replace("title = 'Parcours'", "title = 'Parcours'\ndescription = 'Champ qui ne produit pas la description'")
    assert m._find_head_text_value(text, "description")[1] == "La vraie description dans le head"


@pytest.mark.parametrize("raw", ["", "raw_head = ''\n"])
def test_ordinary_front_matter_stays_readable_without_a_nonempty_raw_head(raw):
    text = "+++\ntitle = 'Le titre lu par le gabarit ordinaire'\n" + raw + "+++\n"
    assert m._find_head_text_value(text, "title")[1] == "Le titre lu par le gabarit ordinaire"


def test_invalid_toml_or_decoded_literal_absent_from_source_abstains():
    assert m._find_head_text_value(source("<title>Un titre reel</title>").replace("'Parcours'", "'Une apostrophe d'ici'"), "title") is None
    text = '+++\ntitle = "Un autre titre"\nraw_head = "<title>Un titre \\u00e9chappe</title>"\n+++\n'
    assert m._find_head_text_value(text, "title") is None


def test_the_live_missing_title_fixture_accepts_a_valid_added_html_title(monkeypatch):
    old = (Path(__file__).parent / "fixtures/hugo/content/gauntlet/missing-title.md").read_text(encoding="utf-8")
    title = "Validation du titre manquant sur la page de controle Hugo"
    patched = old.replace("raw_head = '''\n", f"raw_head = '''\n<title>{title}</title>\n", 1)
    writes = []
    path = "content/gauntlet/missing-title.md"
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {"patched_content": patched})
    monkeypatch.setattr(m, "_github_api_put", lambda *a, **kw: writes.append(kw) or {"content": {"sha": "new"}})
    result = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="main", token="unused",
        fix_branch="fixture-only", all_paths=[path], issue_key="missing_title", issue_label="Missing title",
        impacted_urls=["https://fixture.test/gauntlet/missing-title/"], site_name="fixture.test",
        targets_override=[path], file_state={path: {"sha": "old", "content": old}}, max_files=1)
    assert len(writes) == 1 and result[0] == [path] and not result[1]


@pytest.mark.parametrize("value", ["Test", "Un titre volontairement trop long pour tenir dans la balise, avec encore davantage de texte inutile."])
def test_length_repair_changes_the_raw_head_not_the_first_front_matter_occurrence(monkeypatch, value):
    old = source(f"<title>{value}</title>").replace("'Parcours'", repr(value))
    replacement = "Un titre valide dans la balise servie par la page Hugo"
    monkeypatch.setattr(m, "_length_value_for_page", lambda **kw: replacement)
    changed, n = m._rewrite_length_values(old, {"https://fixture.test/": {"rendered": value, "len": len(value)}}, "title")
    assert n == 1 and f"<title>{replacement}</title>" in changed
    assert "title = " + repr(value) in changed
    assert m._find_head_text_value(changed, "title")[1] == replacement


def test_an_identical_tag_in_the_body_makes_bounded_source_placement_ambiguous(monkeypatch):
    text = source("<title>Test</title>", body="<title>Test</title>")
    assert m._find_head_text_value(text, "title") is None
    monkeypatch.setattr(m, "_length_value_for_page", lambda **kw: pytest.fail("Ambiguous placement must not call Claude"))
    assert m._rewrite_length_values(text, {"https://fixture.test/": {"rendered": "Test", "len": 4}}, "title") == (text, 0)


def test_a_crawl_value_present_only_in_body_text_is_not_a_head_value(monkeypatch):
    text = '<html><head><title>Un titre deja valide pour cette page</title></head><body><p>Test</p></body></html>'
    monkeypatch.setattr(m, "_length_value_for_page", lambda **kw: pytest.fail("No matching head value"))
    assert m._rewrite_length_values(text, {"https://fixture.test/": {"rendered": "Test", "len": 4}}, "title") == (text, 0)


@pytest.mark.parametrize("text,kind,rendered", [
    ('<html><head><title>title</title></head></html>', "title", "title"),
    ("export const head = { title: 'title' };", "title", "title"),
    ('<html><head><meta name="description" content="description"></head></html>', "description", "description"),
    ('<html><head><meta name="description" content="Un texte deja suffisamment explicite"></head></html>', "description", "content"),
])
def test_a_crawl_value_matching_field_syntax_abstains_before_ai(monkeypatch, text, kind, rendered):
    monkeypatch.setattr(m, "_length_value_for_page", lambda **kw: pytest.fail("Only the unambiguous field value may be rewritten"))
    samples = {"https://fixture.test/": {"rendered": rendered, "len": len(rendered)}}
    assert m._rewrite_length_values(text, samples, kind) == (text, 0)
