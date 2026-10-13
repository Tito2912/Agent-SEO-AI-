# -*- coding: utf-8 -*-
"""Les jumelles d'un framework recoivent leur canonical : le code tranche, le modele ecrit.

Banc complet du 12/10/2026 : `duplicate_pages_without_canonical` n'etait corrigee que sur le HTML
pur — le plan protege ecrit dans du HTML litteral, et refusait sur les frameworks et les gabarits
Jinja. Le maitre de chaque groupe est deja tranche par le code (`_duplicate_canonical_masters`) :
sur un framework, le modele n'a qu'a ECRIRE ce canonical dans l'idiome du fichier, et toute autre
URL fait refuser le fichier. Comme pour le plan protege : le groupe entier ou rien.
"""
from __future__ import annotations

import base64
from urllib.parse import unquote

import pytest

from backend import app as m
from backend import repo_index

S = "https://site.test"
A, B = S + "/gauntlet/no-canonical-a", S + "/gauntlet/no-canonical-b"
ASTRO = ["astro.config.mjs", "package.json", "src/pages/gauntlet/no-canonical-a.astro",
         "src/pages/gauntlet/no-canonical-b.astro", "src/layouts/Base.astro"]
KEY = "duplicate_pages_without_canonical"


def jumelle(url):
    return {"url": url, "final_url": url, "status_code": 200, "content_type": "text/html", "canonical": None,
            "title": "Deux jumelles", "meta_description": "", "lang": "fr", "h1": ["Page"], "text_word_count": 80,
            "content_sketch": list(range(30)), "image_urls": [], "internal_links": [], "external_links": [],
            "ld_json_blocks": 0}


def prepare(paths, rows=None):
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: {"count": 2, "examples": [A, B]}}, impacted=[A, B],
                                all_paths=paths, site_name="site.test", owner="o", repo_name="r", branch="main",
                                token="t", pages=rows or [jumelle(A), jumelle(B)])


def test_a_framework_group_gets_the_master_imposed_on_every_twin() -> None:
    prep = prepare(ASTRO)
    assert not prep["refusal"] and prep["canonical_masters"] is None
    assert prep["valeurs_imposees"] == {"src/pages/gauntlet/no-canonical-a.astro": {"canonical": A},
                                        "src/pages/gauntlet/no-canonical-b.astro": {"canonical": A}}
    assert 'canonical = "' + A + '"' in prep["extra_hint"]


def test_a_pure_html_group_keeps_the_protected_plan() -> None:
    prep = prepare(["index.html", "gauntlet/no-canonical-a.html", "gauntlet/no-canonical-b.html"])
    assert prep["canonical_masters"] == {A: A, B: A} and "valeurs_imposees" not in prep


def test_an_incomplete_group_writes_nothing() -> None:
    prep = prepare([p for p in ASTRO if not p.endswith("no-canonical-b.astro")])
    assert prep["refusal"] and "valeurs_imposees" not in prep


PAGE = "---\n---\n<html lang=\"fr\">\n  <head>\n    <title>Deux jumelles</title>\n  </head>\n</html>\n"


def run(monkeypatch, ecrire):
    written = {}
    monkeypatch.setattr(m, "_github_api_get", lambda api, **kw: {
        "sha": "s", "encoding": "base64", "content": base64.b64encode(PAGE.encode()).decode()})

    def put(api, **kw):
        written[unquote(api.split("/contents/", 1)[1])] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "n"}}
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {"patched_content": ecrire(kw["file_path"])})
    for name in ("_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail("les cibles sont connues"))
    rows = [jumelle(A), jumelle(B)]
    applied = m._apply_prepared_issue_fix(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="qa", all_paths=list(ASTRO), issue_key=KEY,
        issue_label=KEY, impacted=[A, B], site_name="site.test", file_state={}, max_files=4, prep=prepare(ASTRO, rows),
        pages=rows, index=repo_index.build_repo_index(ASTRO))
    return applied, written


def avec(url):
    return PAGE.replace("  </head>", '    <link rel="canonical" href="' + url + '" />\n  </head>')


def test_both_twins_point_to_the_master(monkeypatch) -> None:
    _, written = run(monkeypatch, lambda path: avec(A))
    assert written == {"src/pages/gauntlet/no-canonical-a.astro": avec(A), "src/pages/gauntlet/no-canonical-b.astro": avec(A)}


def test_a_twin_pointing_at_the_other_is_refused_no_loop(monkeypatch) -> None:
    # La boucle du 16/09/2026 : chacune designe l'autre. Le modele ne choisit pas le maitre.
    _, written = run(monkeypatch, lambda path: avec(B if path.endswith("-a.astro") else A))
    assert "src/pages/gauntlet/no-canonical-a.astro" not in written
