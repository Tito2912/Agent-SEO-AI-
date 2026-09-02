"""Rewriting a page's snippet to answer a query it already ranks for.

This closes the loop the rest of the product exists for: Search Console says which query a page
is seen on and never clicked, the page's subject is already right — it ranks — and the fix is
the two lines a searcher actually reads.

Finding those two lines in the file is the novel part, because the seven supported stacks write
them four different ways: a component prop, TOML front matter, YAML front matter quoted OR BARE,
and a JS binding. The bare YAML form is the one every quoted-value pattern misses, and Jekyll
uses it.

The model is stubbed here: what these tests defend is the locating, the bounded swap and the
length guarantee, which must hold whatever it answers.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402

FIXTURES = WEB_ROOT / "tests" / "fixtures"


# ── locating the value, across every way the stacks write it ──────────────────────────────────

@pytest.mark.parametrize(
    "label, source, expected",
    [
        ("component prop", 'title="Vrai titre"', "Vrai titre"),
        ("TOML front matter", 'title = "Vrai titre"', "Vrai titre"),
        ("YAML front matter, quoted", 'title: "Vrai titre"', "Vrai titre"),
        ("YAML front matter, BARE — the one quoted patterns miss", "title: Vrai titre", "Vrai titre"),
        ("JS binding", "const title = 'Vrai titre';", "Vrai titre"),
    ],
)
def test_the_declaration_is_found_however_it_is_written(label, source, expected) -> None:
    found = app_module._find_head_text_value(source, "title")
    assert found is not None, label
    assert found[1] == expected


@pytest.mark.parametrize(
    "label, source",
    [
        ("og:title is a COPY of the value", '<meta property="og:title" content="Copie" />'),
        ("twitter:title likewise", '<meta name="twitter:title" content="Copie" />'),
        ("a data- attribute is not a declaration", '<div data-title="Copie">'),
        ("a value assembled from parts", "title={`${base} | Marque`}"),
    ],
)
def test_a_copy_or_an_assembled_value_is_not_mistaken_for_the_declaration(label, source) -> None:
    """Rewriting a copy leaves the original contradicting it; guessing at an assembled value
    rewrites something nobody wrote."""
    assert app_module._find_head_text_value(source, "title") is None, label


@pytest.mark.parametrize(
    "stack, relative",
    [
        ("astro", "astro/src/pages/a-propos.astro"),
        ("hugo", "hugo/content/a-propos.md"),
        ("jekyll", "jekyll/a-propos.html"),
        ("next-pages", "next-pages/pages/a-propos.js"),
        ("nuxt", "nuxt/pages/a-propos.vue"),
        ("sveltekit", "sveltekit/src/routes/a-propos/+page.svelte"),
        ("gatsby", "gatsby/src/pages/a-propos.js"),
        # The two written on 2026-08-30 to complete the nine. static-html is the one that was
        # BROKEN — its values are markup, which none of the framework patterns matched — and
        # next-app is the stack every customer PR so far has run on, which had no fixture of
        # its own because those were somebody's live repositories.
        ("static-html", "static-html/a-propos.html"),
        ("next-app", "next-app/app/a-propos/page.tsx"),
    ],
)
def test_both_values_are_found_in_every_real_fixture(stack: str, relative: str) -> None:
    """The nine repos the correction loop is claimed on, not hand-written snippets."""
    path = FIXTURES / relative
    if not path.exists():  # pragma: no cover - fixtures are committed alongside this test
        pytest.skip(f"{stack} fixture missing")
    source = path.read_text(encoding="utf-8")
    for field in ("title", "description"):
        found = app_module._find_head_text_value(source, field)
        assert found and found[1].strip(), f"{stack}: {field} not found"


# ── the rewrite itself ────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def model(monkeypatch):
    def _install(values: list[str]):
        queue = list(values)

        def _fake(*, system, user_msg, **kw):
            return {"value": queue.pop(0)} if queue else {}

        monkeypatch.setattr(app_module, "_correction_ai_json", _fake)

    return _install


SOURCE = 'title="Ancien titre"\ndescription="Ancienne description de la page."\n<p>corps</p>'


def test_both_lines_are_swapped_in_place(model) -> None:
    # Values inside the window, so this test stays about the SWAP: one outside it now costs a
    # second call, which `test_a_proposal_under_the_window_is_sent_back_once` covers.
    model(["Nouveau titre qui repond enfin a la requete posee par le visiteur", "Nouvelle description qui repond a la requete du visiteur, dit ce que la page apporte et donne envie de cliquer dessus."])
    new, count = app_module._rewrite_for_query(
        SOURCE, query="ma requête", url="https://site.fr/p", site_name="site.fr")
    assert count == 2
    assert 'title="Nouveau titre qui repond enfin a la requete posee par le visiteur"' in new
    assert "<p>corps</p>" in new, "the rewrite touched the body"


def test_an_over_long_answer_is_brought_under_the_ceiling(model) -> None:
    """Same guarantee as the length families: a model does not count characters, so the code
    does. Without this the loop would ship a snippet that trades one anomaly for another."""
    model(["T" * 200, "T" * 190, "T" * 180,   # title: first answer, then the retries
           "D" * 300, "D" * 290, "D" * 280])  # description: same
    new, count = app_module._rewrite_for_query(
        SOURCE, query="ma requête", url="https://site.fr/p")
    assert count == 2
    assert len(app_module._find_head_text_value(new, "title")[1]) <= app_module._LENGTH_CEILINGS["title"]
    assert len(app_module._find_head_text_value(new, "description")[1]) <= app_module._LENGTH_CEILINGS["description"]


def test_an_empty_query_does_nothing_and_costs_nothing(model) -> None:
    model(["ne devrait jamais servir"])
    new, count = app_module._rewrite_for_query(SOURCE, query="   ", url="https://site.fr/p")
    assert count == 0 and new == SOURCE


def test_an_answer_identical_to_the_current_value_is_not_a_change(model) -> None:
    model(["Ancien titre", "Ancienne description de la page."])
    new, count = app_module._rewrite_for_query(SOURCE, query="ma requête", url="https://site.fr/p")
    assert count == 0 and new == SOURCE


def test_a_value_carrying_the_wrong_quote_is_refused(model) -> None:
    """The replacement goes inside a quoted literal. A model is not asked to escape, so a value
    that would break the literal is refused rather than silently corrupting the file."""
    model(['Un titre avec des "guillemets" qui casseraient le literal ici',
           "Nouvelle description qui repond a la requete du visiteur, dit ce que la page apporte et donne envie de cliquer dessus."])
    new, count = app_module._rewrite_for_query(SOURCE, query="ma requête", url="https://site.fr/p")
    assert 'title="Ancien titre"' in new, "a broken literal was written into the file"
    assert count == 1, "the description should still have been rewritten"


def test_a_file_that_declares_nothing_is_left_alone(model) -> None:
    model(["ne devrait jamais servir"])
    source = "<p>une page sans titre ni description déclarés</p>"
    new, count = app_module._rewrite_for_query(source, query="ma requête", url="https://site.fr/p")
    assert count == 0 and new == source


# ── the language of the page ──────────────────────────────────────────────────────────────────
#
# Found on a real customer page, not in a fixture: `content/blog/pictory-ai-review-2026.mdx` on
# voiceoverstudioai.com is English and declares `lang: "en"`. Asked — in French — to rewrite its
# "meta description", Claude Opus answered in FRENCH four times out of four while keeping the
# title in English, which would have shipped a page whose two snippet lines are in different
# languages. "Garde la langue de la page" was already in the prompt.
#
# Naming the language explicitly fixed those runs (3/3 English afterwards). The check below is
# what makes it a guarantee rather than a hope, and it is the part these tests defend.

EN_SOURCE = ('title="Pictory AI review 2026: full test, use cases and limits"\n'
             'description="A complete 2026 Pictory AI review: script-to-video, captions, AI '
             'avatars, ElevenLabs voices, limits and production workflow."')

FRENCH_ANSWER = ("Pictory AI test 2026 : on a essayé script-to-video, sous-titres et voix. "
                 "Ce qui marche vraiment, les limites et les vrais prix avant de payer.")
ENGLISH_ANSWER = ("We tested Pictory AI in 2026: script-to-video, captions, AI voices and "
                  "avatars. See the real results, the limits and what it costs.")


@pytest.mark.parametrize(
    "expected, text",
    [
        ("en", "A complete 2026 Pictory AI review: script-to-video, captions, AI avatars, "
               "ElevenLabs voices, limits and production workflow."),
        ("fr", "Guide Kling AI : texte en vidéo, image en vidéo, contrôle caméra et workflows "
               "de production. Créez des clips cinématiques."),
        ("de", "Kling AI Preise 2026: Credits, Tarife und die echten Kosten für jeden Clip"),
        ("es", "Precios de Kling AI 2026: créditos, planes y el coste real por clip"),
        # Abstention is a deliberate answer: a short title carries no function words, and
        # refusing a correct rewrite on no evidence would be worse than the drift.
        ("", "Kling AI pricing 2026: credits, plans and real cost per clip"),
        ("", ""),
    ],
)
def test_the_language_detector_answers_or_abstains(expected: str, text: str) -> None:
    assert app_module._dominant_language(text) == expected


def test_a_translated_answer_is_refused_and_the_value_kept(model) -> None:
    """Translating half a page's snippet is not a rewrite, it is a defect. The old value costs a
    click; the wrong language costs the page."""
    # Description only: with both fields in play the title call would drain the stubbed queue and
    # this test would pass whether or not the guard exists — which it did, until measured.
    source = EN_SOURCE.splitlines()[1]
    model([FRENCH_ANSWER, FRENCH_ANSWER])  # the answer, then the same again after the retry
    new, count = app_module._rewrite_for_query(
        source, query="pictory test", url="https://site.fr/blog/pictory", site_name="site.fr")
    assert count == 0, "a French description was written into an English page"
    assert new == source


def test_the_model_gets_one_retry_naming_the_language(model, monkeypatch) -> None:
    """One retry, then it stops: the same measurement-carrying second chance the length family
    gets. Here the retry answers correctly and the rewrite goes through."""
    seen: list[str] = []

    def _fake(*, system, user_msg, **kw):
        seen.append(user_msg)
        return {"value": ENGLISH_ANSWER if len(seen) > 1 else FRENCH_ANSWER}

    monkeypatch.setattr(app_module, "_correction_ai_json", _fake)
    # Description only, so the two calls counted here are the refusal and its retry.
    source = EN_SOURCE.splitlines()[1]
    new, count = app_module._rewrite_for_query(
        source, query="pictory test", url="https://site.fr/blog/pictory", site_name="site.fr")
    assert count == 1, "the retry's correct answer was not used"
    assert ENGLISH_ANSWER in new
    assert "ta_proposition_refusee" in seen[1], "the retry did not say what was wrong"


def test_the_page_language_is_named_in_the_prompt(model, monkeypatch) -> None:
    """The cheap half. It is what turned 4 French answers out of 4 into 3 English ones out of 3
    on the real page — but it is a prompt, so the check above stays."""
    prompts: list[str] = []

    def _fake(*, system, user_msg, **kw):
        prompts.append(system)
        return {"value": ENGLISH_ANSWER}

    monkeypatch.setattr(app_module, "_correction_ai_json", _fake)
    app_module._rewrite_for_query(EN_SOURCE, query="pictory test",
                                 url="https://site.fr/blog/pictory", site_name="site.fr")
    assert any("anglais" in p for p in prompts), "the model was not told which language to keep"


def test_a_value_with_no_detectable_language_is_left_to_the_model(model) -> None:
    """The guard only fires when the page's own value says which language it is in. A brand-name
    title says nothing, and refusing there would block correct rewrites for no reading."""
    source = 'title="Kling AI Pricing 2026"\ndescription="Kling AI Pricing 2026"'
    model(["Kling AI : prix, credits et cout reel par clip en 2026, le guide",
           "Kling AI : prix, credits et cout reel par clip en 2026. Ce que chaque plan "
           "contient, et lequel choisir selon ton usage reel."])
    new, count = app_module._rewrite_for_query(source, query="kling ai prix",
                                              url="https://site.fr/p", site_name="site.fr")
    assert count == 2


def test_a_proposal_under_the_window_is_sent_back_once(model, monkeypatch) -> None:
    """A snippet the model wrote at 48 characters against a 60-68 window is legal — nothing
    flags a title until 15 — so no later crawl will ever ask those characters back. The keyword
    path used to route only OVER-long values through the window helper, which is how PR#3's title
    shipped at 56. The extra call happens only when the first answer misses."""
    seen: list[str] = []

    def _fake(*, system, user_msg, **kw):
        seen.append({"system": system, "user": user_msg})
        return {"value": "Titre court" if len(seen) == 1 else
                "Nouveau titre qui repond enfin a la requete posee par le visiteur"}

    monkeypatch.setattr(app_module, "_correction_ai_json", _fake)
    source = 'title="Ancien titre de la page qui ne repond pas du tout a la requete visee"'
    new, count = app_module._rewrite_for_query(
        source, query="ma requête", url="https://site.fr/p", site_name="site.fr")
    low, high = app_module._LENGTH_WINDOWS["title"]
    value = app_module._find_head_text_value(new, "title")[1]
    assert count == 1 and low <= len(value) <= high, f"{len(value)} caractères"
    assert len(seen) == 2, "the short proposal was accepted without a second look"
    # The second call is the window helper's first round: it states the problem in its SYSTEM
    # prompt, and carries the measured length of the value it was handed.
    assert "trop court" in seen[1]["system"]
    assert '"longueur_actuelle": 11' in seen[1]["user"]


# ── the way HTML writes it (2026-08-30) ───────────────────────────────────────────────────────
#
# The patterns above cover every way a FRAMEWORK declares the two values — a component prop,
# TOML, YAML quoted and bare, a JS binding. None of them covers the way plain HTML does it, and
# static-html is one of the two stacks validated in PRODUCTION (elevenlabs-avis.com): a real
# customer on a static site clicking "Réécrire (PR)" was told their values were "assemblées".

STATIC_PAGE = '''<!doctype html>
<html lang="fr"><head>
<title>Ancien titre de la page statique</title>
<meta name="description" content="Ancienne description de cette page statique." />
<meta property="og:title" content="Copie OG" />
<meta name="twitter:description" content="Copie twitter" />
</head><body><a href="/x" title="info-bulle">lien</a></body></html>'''


def test_a_static_html_page_declares_its_values_in_markup(model) -> None:
    assert app_module._find_head_text_value(STATIC_PAGE, "title")[1] == "Ancien titre de la page statique"
    assert app_module._find_head_text_value(STATIC_PAGE, "description")[1] == (
        "Ancienne description de cette page statique.")


def test_the_copies_and_the_tooltips_are_left_alone_on_a_static_page(model) -> None:
    """`og:`/`twitter:` are COPIES — rewriting one leaves the original contradicting it — and a
    link's `title=` attribute is a tooltip. The attribute patterns would have matched that
    tooltip, which is why a full document is read as markup only."""
    model(["Nouveau titre qui repond enfin a la requete posee par le visiteur",
           "Nouvelle description qui repond a la requete du visiteur, dit ce que la page "
           "apporte et donne envie de cliquer dessus."])
    new, count = app_module._rewrite_for_query(
        STATIC_PAGE, query="ma requête", url="https://site.fr/p", site_name="site.fr")
    assert count == 2
    assert "Copie OG" in new and "Copie twitter" in new
    assert 'title="info-bulle"' in new, "a link's tooltip was rewritten as if it were the title"
    assert "<title>Nouveau titre qui repond enfin a la requete posee par le visiteur</title>" in new


def test_a_next_pages_component_is_not_mistaken_for_an_html_document(model) -> None:
    """`<Head>` is Next.js's component, not a document. Matching it case-insensitively made the
    locator read `<title>{title}</title>` — an expression — and refuse the file outright, taking
    next-pages from 2/2 to 0/2. The page's values are the JS bindings above it."""
    source = (
        "import Head from 'next/head';\n\n"
        "const title = 'Ancien titre de cette page Next Pages';\n"
        "const description = \"Ancienne description de cette page Next Pages du site.\";\n\n"
        "export default function Page() {\n"
        "  return (<main><Head><title>{title}</title>"
        "<meta name=\"description\" content={description} /></Head></main>);\n}\n"
    )
    found = app_module._find_head_text_value(source, "title")
    assert found and found[1] == "Ancien titre de cette page Next Pages"
    assert app_module._find_head_text_value(source, "description")[1].startswith("Ancienne description")


def test_a_templated_title_element_is_still_refused(model) -> None:
    """`<title>{title}</title>` in a component is the page's LOGIC. Replacing it would hardcode
    one page's text into whatever the expression serves."""
    assert app_module._find_head_text_value("<title>{title}</title>", "title") is None
    assert app_module._find_head_text_value("<title></title>", "title") is None


def test_the_description_meta_is_matched_whatever_the_attribute_order(model) -> None:
    page = ('<!doctype html><html><head><meta content="La vraie description de la page ici." '
            'name="description"></head><body></body></html>')
    assert app_module._find_head_text_value(page, "description")[1] == (
        "La vraie description de la page ici.")


def test_a_french_apostrophe_inside_the_meta_survives(model) -> None:
    """`content="Page d'accueil…"` is ordinary French, and excluding both quote characters to
    stop the value overrunning would have broken every description on a French site."""
    page = ('<!doctype html><html><head>'
            '<meta name="description" content="Page d\'accueil du site, avec ce qu\'on y trouve.">'
            '</head><body></body></html>')
    assert app_module._find_head_text_value(page, "description")[1] == (
        "Page d'accueil du site, avec ce qu'on y trouve.")
