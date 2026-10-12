# -*- coding: utf-8 -*-
"""Un champ de front matter que les gabarits n'impriment pas ne corrige rien.

Banc complet du 12/10/2026, jekyll, `missing_title` : le modele a ajoute `title:` au front matter
de la page — l'idiome Jekyll — et la page servie n'avait toujours aucun titre, parce que son
gabarit n'imprime que `page.raw_head`. Une pull request sans effet, et rien pour le dire.

Le correcteur lit desormais la chaine des gabarits (layout, layouts parents, `include`,
`{% seo %}`) : il dit au modele quels champs comptent, et refuse un fichier qui en ajoute un
autre. Quand la chaine sort du depot — un theme en gem — il ne sait pas, et ne refuse rien.
"""
from __future__ import annotations

import base64
from pathlib import Path
from urllib.parse import unquote

import pytest

from backend import app as m
from backend import jekyll_champs as jc
from backend import repo_index

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "jekyll"


def lire_fixture(path):
    f = FIXTURE / path
    return f.read_text(encoding="utf-8") if f.is_file() else None


FIXTURE_PATHS = ["_config.yml", "_layouts/default.html", "_layouts/gauntlet.html",
                 "gauntlet/missing-title.html", "a-propos.md"]


def test_the_bench_layout_prints_only_what_the_page_hands_it() -> None:
    page = lire_fixture("gauntlet/missing-title.html")
    assert jc.champs_rendus(page, FIXTURE_PATHS, lire_fixture) == {"html_attrs", "raw_head", "raw_body"}


def test_the_site_layout_prints_title_and_description() -> None:
    page = "---\nlayout: default\ntitle: A propos\n---\nTexte\n"
    rendus = jc.champs_rendus(page, FIXTURE_PATHS, lire_fixture)
    assert {"title", "description", "canonical"} <= rendus


@pytest.mark.parametrize("sources, attendu", [
    # Un include et un layout parent sont suivis.
    ({"_layouts/page.html": "---\nlayout: base\n---\n{% include head.html %}{{ content }}",
      "_layouts/base.html": "<html><body>{{ page.lang }}{{ content }}</body></html>",
      "_includes/head.html": "<title>{{ page.title }}</title>"}, {"title", "lang"}),
    # jekyll-seo-tag rend le titre et la description.
    ({"_layouts/page.html": "<head>{% seo %}</head>"}, {"title", "description"}),
    # Un theme en gem : le layout n'est pas dans le depot. On ne sait pas.
    ({}, None),
    # Un include calcule ou absent : on ne sait pas non plus.
    ({"_layouts/page.html": "{% include {{ page.partial }} %}"}, None),
    ({"_layouts/page.html": "{% include absent.html %}"}, None),
])
def test_the_chain_is_followed_and_unknown_means_none(sources, attendu) -> None:
    page = "---\nlayout: page\n---\nTexte\n"
    assert jc.champs_rendus(page, list(sources) + ["p.md"], sources.get) == attendu


def test_only_keys_the_patch_adds_and_no_template_prints_are_reported() -> None:
    avant = "---\nlayout: gauntlet\nraw_head: |\n  <meta />\n---\n"
    apres = avant.replace("layout: gauntlet\n", "layout: gauntlet\ntitle: Titre\n")
    assert jc.champs_sans_effet(apres, avant, {"raw_head"}) == ["title"]
    assert jc.champs_sans_effet(apres, avant, {"raw_head", "title"}) == []
    assert jc.champs_sans_effet(apres, avant, None) == []
    assert "page.raw_head" in jc.consigne({"raw_head"}) and "SANS EFFET" in jc.consigne({"raw_head"})


# --- de bout en bout ---------------------------------------------------------------------------

URL = "https://site.test/gauntlet/missing-title"


def run(monkeypatch, ecrite):
    sources = {p: lire_fixture(p) or "" for p in FIXTURE_PATHS}
    written, consignes = {}, []

    def get(api, **kw):
        path = unquote(api.split("/contents/", 1)[1])
        return {"sha": "s", "encoding": "base64", "content": base64.b64encode(sources[path].encode()).decode()}

    def put(api, **kw):
        written[unquote(api.split("/contents/", 1)[1])] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "n"}}

    def modele(**kw):
        consignes.append(kw.get("occurrences_hint") or "")
        return {"patched_content": ecrite(kw["file_content"])}

    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_openai_generate_file_patch", modele)
    for name in ("_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, lambda *a, **kw: [])
    m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="qa", all_paths=list(FIXTURE_PATHS),
        issue_key="missing_title", issue_label="Titre manquant", impacted_urls=[URL], site_name="site.test",
        file_state={}, max_files=2, index=repo_index.build_repo_index(FIXTURE_PATHS),
        targets_override=["gauntlet/missing-title.html"])
    return written, consignes


def test_a_title_in_a_field_the_layout_ignores_is_refused(monkeypatch) -> None:
    written, consignes = run(monkeypatch, lambda raw: raw.replace(
        "layout: gauntlet\n", "layout: gauntlet\ntitle: \"Parcours d'obstacles\"\n", 1))
    assert written == {}
    assert "page.raw_head" in consignes[0] and "SANS EFFET" in consignes[0]


def test_a_title_written_where_the_page_writes_its_head_goes_through(monkeypatch) -> None:
    ecrite = lambda raw: raw.replace("raw_head: |\n", "raw_head: |\n  <title>Parcours d'obstacles</title>\n", 1)  # noqa: E731
    written, _ = run(monkeypatch, ecrite)
    assert list(written) == ["gauntlet/missing-title.html"]
    assert "<title>Parcours d'obstacles</title>" in written["gauntlet/missing-title.html"]
