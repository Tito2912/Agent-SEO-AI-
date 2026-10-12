# -*- coding: utf-8 -*-
"""Une page sans extrait publié reçoit une description RÉDIGÉE, et seulement elle.

Décision du propriétaire, 12/10/2026. Depuis le 08/10, une description courte ou absente n'était
corrigée qu'en reprenant 100 à 160 caractères de phrases déjà publiées ; une page trop pauvre
(contact, catégorie, accueil visuelle) restait sans description, et sur un framework (Next,
Astro, Nuxt, Hugo...) le chemin protégé refusait TOUJOURS, faute de HTML littéral. Mesuré sur
le banc des 9 sites : la page indexable sans description ressortait sans description partout.

Ce que ces tests tiennent : l'extrait reste prioritaire DANS LE CODE (une valeur rédigée est
refusée dès qu'un extrait existe), la valeur rédigée est bornée et typée, elle est signalée dans
le commit et dans la PR, et les frameworks reprennent le chemin des longueurs avec repli fichier.
"""
from __future__ import annotations

import base64
import html
import json

import pytest

from backend import app as m, repo_index
from backend import short_description as sd
from tests.test_verified_short_description import FACT, KEY, S, SHORT, page, prepare, source

WRITTEN = ("Contactez notre équipe pour toute question sur nos services : nous vous répondons "
           "rapidement et vous orientons vers la bonne personne.")
ASTRO = ["package.json", "astro.config.mjs", "src/pages/index.astro"]


def thin(absent=False):
    """La même page, mais son seul paragraphe est trop court pour fournir un extrait."""
    return source(absent=absent).replace(FACT, "Un texte court.")


@pytest.mark.parametrize("value, ok", [
    (WRITTEN, True), ("x" * 100, True), ("x" * 160, True), ("x" * 99, False), ("x" * 161, False),
    (WRITTEN.replace(" : ", "\n"), False), (WRITTEN.replace(" ", "  ", 1), False), (" " + WRITTEN[1:], False),
    ("<b>" + WRITTEN[3:], False), (WRITTEN[:-1] + "\x07", False), (None, False), (123, False)])
def test_a_written_value_is_one_plain_line_inside_the_band(value, ok) -> None:
    assert sd.written_ok(value) is ok


@pytest.mark.parametrize("absent", [False, True])
def test_a_page_without_excerpt_receives_the_written_value_only_when_asked(absent) -> None:
    raw, observed = thin(absent), page(absent=absent)
    assert sd.rewrite(raw, observed, WRITTEN, m._duplicate_html_document, m._verification_url) == (raw, 0)
    output, count = sd.rewrite(raw, observed, WRITTEN, m._duplicate_html_document, m._verification_url, written=True)
    assert count == 1
    tag = 'content="' + html.escape(WRITTEN, quote=True) + '"'
    assert tag in output
    if not absent:
        assert output == raw.replace('content="' + SHORT + '"', tag, 1)


def test_the_excerpt_keeps_priority_in_code_not_only_in_the_prompt() -> None:
    raw, observed = source(), page()
    # A page that HAS an excerpt refuses any written value, even when the caller asks for one…
    assert sd.rewrite(raw, observed, WRITTEN, m._duplicate_html_document, m._verification_url, written=True) == (raw, 0)
    # …and still takes its own excerpt.
    assert sd.rewrite(raw, observed, FACT, m._duplicate_html_document, m._verification_url, written=True)[1] == 1


def test_an_out_of_band_written_value_is_refused_even_without_excerpt() -> None:
    raw, observed = thin(), page()
    for bad in ("Trop court.", WRITTEN + " " + WRITTEN, "<script>" + WRITTEN + "</script>"):
        assert sd.rewrite(raw, observed, bad, m._duplicate_html_document, m._verification_url, written=True) == (raw, 0)


def test_the_model_writes_only_for_a_page_without_excerpt(monkeypatch) -> None:
    calls = []

    def ai(**kw):
        calls.append(json.loads(kw["user_msg"]))
        return {"description": WRITTEN}
    monkeypatch.setattr(m, "_correction_ai_json", ai)
    assert m._written_description_value(source(), page()) == ""
    assert calls == [], "a page with an excerpt must never reach the writer"
    assert m._written_description_value(thin(), page()) == WRITTEN
    assert len(calls) == 1
    assert calls[0]["lang"] == "fr" and calls[0]["title"] and calls[0]["h1"] == ["Accueil"]
    assert calls[0]["paragraphs"] == ["Un texte court."]


@pytest.mark.parametrize("answer", [
    {"description": "Trop court."}, {"description": WRITTEN, "extra": 1}, {"candidate": 0}, None, [],
    {"description": WRITTEN.replace(" : ", "\n")}, {"description": "<b>" + WRITTEN[3:]}, {"description": 42}])
def test_a_malformed_or_out_of_band_answer_writes_nothing(monkeypatch, answer) -> None:
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: answer)
    assert m._written_description_value(thin(), page()) == ""


def test_a_description_already_used_elsewhere_is_not_written_again(monkeypatch) -> None:
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"description": WRITTEN})
    assert m._written_description_value(thin(), page(), excluded_values={WRITTEN}) == ""


def test_the_whole_route_writes_flags_and_bills_the_written_description(monkeypatch) -> None:
    raw, writes, messages = thin(), {}, []
    monkeypatch.setattr(m, "_github_api_get", lambda api, **kw: {
        "sha": "current", "content": base64.b64encode(raw.encode()).decode()})

    def put(api, **kw):
        writes[api.split("/contents/", 1)[1]] = base64.b64decode(kw["json_body"]["content"]).decode()
        messages.append(kw["json_body"]["message"])
        return {"content": {"sha": "new"}}
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_short_description_page", lambda *a: True, raising=False)
    monkeypatch.setattr(m, "_correction_ai_json", lambda **kw: {"description": WRITTEN})
    for name in ("_openai_generate_file_patch", "_ai_map_urls_to_files", "_ai_pick_repo_files"):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail("no free-form model on a literal page"))
    rows = [page()]
    plan = prepare(rows)
    assert plan["short_description_urls"] == [S]
    assert "rédigée par l'IA" in plan["side_effects"]
    result = m._apply_prepared_issue_fix(
        owner="fixture", repo_name="fixture", branch="baseline", token="unused", fix_branch="qa",
        all_paths=["index.html"], issue_key=KEY, issue_label=KEY, impacted=[S], site_name="site.test",
        file_state={}, max_files=1, prep=plan, pages=rows, index=repo_index.build_repo_index(["index.html"]))
    assert result["patched"] == result["ai_files"] == ["index.html"]
    assert writes == {"index.html": raw.replace('content="' + SHORT + '"',
                                                'content="' + html.escape(WRITTEN, quote=True) + '"', 1)}
    assert messages and "AI-written description" in messages[0]


def test_a_framework_short_description_takes_the_length_route_with_the_file_fallback() -> None:
    plan = m._prepare_issue_fix(
        issue_key=KEY + "_indexable",
        issues={KEY + "_indexable": {"count": 1, "examples": [S], "length_samples": {S: {"rendered": "", "len": 0}}}},
        impacted=[S], all_paths=ASTRO, site_name="site.test", owner="fixture", repo_name="fixture",
        branch="baseline", token="unused", pages=[page(absent=True)])
    assert not plan.get("refusal")
    assert "short_description_urls" not in plan, "the literal plan can never read an .astro source"
    assert plan["link_rewriter"] is not None and plan["rewriter_ai_fallback"] and plan["rewriter_is_ai"]
    assert plan["targets_override"] == ["src/pages/index.astro"]
    assert "DESCRIPTION COURTE OU ABSENTE" in plan["extra_hint"]
    assert "rédigée par l'IA" in plan["side_effects"]


def test_the_framework_route_names_only_the_files_of_short_or_absent_pages() -> None:
    """Banc du 12/10 : sans liste de fichiers, le repli avait reecrit deux descriptions TROP
    LONGUES (next-app 192 -> 153, astro 268 -> 200) — hors de la decision du proprietaire."""
    long_url, paths = S + "a-propos", ASTRO + ["src/pages/a-propos.astro", "src/layouts/Base.astro"]
    samples = {S: {"rendered": "", "len": 0}, long_url: {"rendered": "x" * 268, "len": 268}}
    plan = m._prepare_issue_fix(
        issue_key=KEY,
        issues={KEY: {"count": 2, "examples": [S, long_url], "length_samples": samples}},
        impacted=[S, long_url], all_paths=paths, site_name="site.test", owner="fixture", repo_name="fixture",
        branch="baseline", token="unused", pages=[page(absent=True), page(long_url)])
    assert plan["targets_override"] == ["src/pages/index.astro"]


def test_a_short_page_without_its_own_source_file_is_refused_not_guessed() -> None:
    plan = m._prepare_issue_fix(
        issue_key=KEY,
        issues={KEY: {"count": 1, "examples": [S + "nulle-part"],
                      "length_samples": {S + "nulle-part": {"rendered": "", "len": 0}}}},
        impacted=[S + "nulle-part"], all_paths=ASTRO, site_name="site.test", owner="fixture", repo_name="fixture",
        branch="baseline", token="unused", pages=[page(S + "nulle-part", absent=True)])
    assert plan["refusal"] and not plan.get("targets_override")


def test_a_cut_written_description_ends_on_a_sentence_or_without_a_dangling_word() -> None:
    """Banc du 12/10, hugo : 167 caracteres rediges, coupes en « ...une anomalie et »."""
    hugo = ("Cette page du parcours traite le cas « missing meta description ». Elle appartient au parcours "
            "d'obstacles de la fixture et sert à provoquer une anomalie et une seule.")
    cut = m._trim_to_ceiling(hugo, 160)
    assert hugo.startswith(cut) and len(cut) <= 160
    assert cut.rsplit(" ", 1)[1] not in m._DANGLING_WORDS, cut
    # A sentence that ends late enough is kept whole.
    two = "Une premiere phrase assez longue pour depasser les trois quarts du plafond, avec du texte en plus ici. " + "Suite " * 20
    assert m._trim_to_ceiling(two, 120) == two[:two.index(". ") + 1]
    # A sentence ending too early is NOT taken: the cut would fall far below the floor.
    early = "Court. " + "mot " * 60
    assert len(m._trim_to_ceiling(early, 160)) > 120


@pytest.mark.parametrize("paths, opened", [(ASTRO, True), (["index.html"], False)])
def test_the_write_step_lets_only_the_framework_route_through(monkeypatch, paths, opened) -> None:
    """Banc du 12/10 : la route frameworks franchissait la preparation puis bloquait au second
    verrou, dans l'ecriture (« AUCUNE CIBLE » sur astro et next-app)."""
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: pytest.fail("no read needed"))
    _patched, notes, _targets, _ai = m._deep_patch_issue_files(
        owner="fixture", repo_name="fixture", branch="baseline", token="unused", fix_branch="qa",
        all_paths=paths, issue_key=KEY + "_indexable", issue_label=KEY, impacted_urls=[S],
        site_name="site.test", file_state={}, max_files=1, link_rewriter=lambda raw: (raw, 0),
        rewriter_ai_fallback=True, rewriter_is_ai=True, targets_override=[])
    assert any("Description refusee" in str(n) for n in notes) is not opened


def test_a_framework_without_short_sample_is_refused_before_any_model() -> None:
    plan = m._prepare_issue_fix(
        issue_key=KEY,
        issues={KEY: {"count": 1, "examples": [S], "length_samples": {S: {"rendered": "x" * 200, "len": 200}}}},
        impacted=[S], all_paths=ASTRO, site_name="site.test", owner="fixture", repo_name="fixture",
        branch="baseline", token="unused", pages=[page()])
    assert plan["refusal"] and not plan.get("rewriter_ai_fallback")


def test_long_descriptions_on_a_framework_keep_no_file_fallback() -> None:
    """The decision covers short/absent descriptions only: long ones stay value-first."""
    key = "meta_description_too_long"
    plan = m._prepare_issue_fix(
        issue_key=key,
        issues={key: {"count": 1, "examples": [S], "length_samples": {S: {"rendered": "x" * 200, "len": 200}}}},
        impacted=[S], all_paths=ASTRO, site_name="site.test", owner="fixture", repo_name="fixture",
        branch="baseline", token="unused", pages=[page()])
    assert not plan.get("rewriter_ai_fallback")
    assert "rédigée par l'IA" not in str(plan.get("side_effects") or "")
