"""The length families stop trusting a model to count.

Measured against the model production uses, on a really built site: a 268-character description
with a 160-character threshold came back at 200, then 200 again under an explicit
"AU PLUS 160 — rester au-dessus ne corrige RIEN", then 191 when told exactly how many characters
to remove. Four runs, four values above the threshold. No wording fixed it, because the problem
is not the wording: a model does not count characters, it removes a clause and judges the job
done.

So the split changed. The model does what it is good at — choosing what to drop while keeping the
sense and the language — and the code does what it is good at: measuring. One retry carries the
real measurement back, and a deterministic trim guarantees the bound if that misses too. The
value is then placed by literal replacement, so the family stops being a full-file rewrite.

`_correction_ai_json` is stubbed here: these tests are about the arithmetic and the placement,
which must hold whatever the model answers.
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

CEILING = 160
TOO_LONG = "A" * 268


def _samples(rendered: str, declared: int | None = None) -> dict:
    return {"https://site.fr/a": {"rendered": rendered, "len": declared if declared is not None else len(rendered)}}


@pytest.fixture()
def answers(monkeypatch):
    """Queue the model's replies; returns the list of prompts it was actually given."""
    prompts: list[str] = []

    def _install(values: list[str]):
        queue = list(values)

        def _fake(*, system, user_msg, **kw):
            prompts.append(user_msg)
            return {"value": queue.pop(0)} if queue else {}

        monkeypatch.setattr(app_module, "_correction_ai_json", _fake)
        return prompts

    return _install


# ── the arithmetic ────────────────────────────────────────────────────────────────────────────

def test_the_family_name_normalises_to_the_table_key() -> None:
    """`_length_family_name` answers 'meta'; the window tables are keyed 'description'.

    The hint absorbed that with an inline ternary and the rewriter looked it up strictly, so it
    returned None and did nothing at all — silently, on a real fixture, until measured.
    """
    assert app_module._length_kind("meta") == "description"
    assert app_module._length_kind("title") == "title"
    assert app_module._length_kind("") == "description"


def test_the_trim_cuts_on_a_word_and_invents_nothing() -> None:
    value = "Un texte de démonstration " * 20
    trimmed = app_module._trim_to_ceiling(value, CEILING)
    assert len(trimmed) <= CEILING
    assert value.startswith(trimmed), "the trim rewrote instead of cutting"
    assert not trimmed.endswith(("...", "…")), (
        "an ellipsis tells the reader the sentence was cut, which is worse than an abrupt end"
    )
    assert trimmed == trimmed.rstrip(), "trailing whitespace left in a meta description"


def test_the_trim_leaves_a_short_value_alone() -> None:
    assert app_module._trim_to_ceiling("court", CEILING) == "court"


# ── the loop ──────────────────────────────────────────────────────────────────────────────────

def test_a_good_first_answer_is_used_as_is(answers) -> None:
    good = "B" * 150
    prompts = answers([good])
    new, count = app_module._rewrite_length_values(
        f'description="{TOO_LONG}"', _samples(TOO_LONG), "meta")
    assert count == 1 and f'description="{good}"' == new
    assert len(prompts) == 1, "a valid answer must not cost a second call"


def test_a_too_long_answer_triggers_one_retry_carrying_the_measurement(answers) -> None:
    prompts = answers(["C" * 200, "D" * 150])
    new, count = app_module._rewrite_length_values(
        f'description="{TOO_LONG}"', _samples(TOO_LONG), "meta")
    assert count == 1 and "D" * 150 in new
    assert len(prompts) == 2
    assert "longueur_de_ta_proposition" in prompts[1] and "200" in prompts[1], (
        "the retry must tell the model how long its own answer was — the one thing it cannot "
        "work out for itself"
    )


def test_a_model_that_never_gets_under_the_ceiling_is_trimmed(answers) -> None:
    """The guarantee. Two failures used to ship a PR that fixed nothing."""
    answers(["E" * 200, "F" * 190])
    new, count = app_module._rewrite_length_values(
        f'description="{TOO_LONG}"', _samples(TOO_LONG), "meta")
    assert count == 1
    import re

    value = re.search(r'description="([^"]*)"', new).group(1)
    assert len(value) <= CEILING, f"shipped {len(value)} characters against a {CEILING} ceiling"


def test_an_empty_answer_changes_nothing(answers) -> None:
    answers([])
    content = f'description="{TOO_LONG}"'
    new, count = app_module._rewrite_length_values(content, _samples(TOO_LONG), "meta")
    assert count == 0 and new == content


# ── the placement ─────────────────────────────────────────────────────────────────────────────

def test_a_truncated_sample_is_skipped(answers) -> None:
    """Old reports capped `rendered` at 200 while `len` said 268. Replacing a truncated string
    would cut the value in the file at exactly the wrong place."""
    prompts = answers(["G" * 150])
    content = f'description="{TOO_LONG}"'
    new, count = app_module._rewrite_length_values(
        content, _samples(TOO_LONG[:200], declared=268), "meta")
    assert count == 0 and new == content
    assert prompts == [], "the model was called for a value that could not be placed"


def test_a_value_that_is_not_in_the_file_verbatim_is_left_to_the_ai_fallback(answers) -> None:
    # A rendered value assembled from parts (`{title} | {brand}`) has no literal match. Doing
    # nothing here is correct: `rewriter_ai_fallback` is on, so the full-file patch still runs.
    prompts = answers(["H" * 150])
    content = 'description={`${base} | Marque`}'
    new, count = app_module._rewrite_length_values(content, _samples(TOO_LONG), "meta")
    assert count == 0 and new == content and prompts == []


def test_a_value_already_within_the_threshold_is_not_touched(answers) -> None:
    prompts = answers(["I" * 150])
    short = "J" * 120
    content = f'description="{short}"'
    new, count = app_module._rewrite_length_values(content, _samples(short), "meta")
    assert count == 0 and new == content and prompts == []


def test_only_the_flagged_value_moves(answers) -> None:
    """Placement is a literal replacement of one occurrence, so a second page's description in
    the same file — a layout holding a default — cannot be dragged along."""
    other = "K" * 120
    answers(["L" * 150])
    content = f'<meta name="description" content="{TOO_LONG}" />\n<meta name="og:description" content="{other}" />'
    new, count = app_module._rewrite_length_values(content, _samples(TOO_LONG), "meta")
    assert count == 1
    assert other in new, "an unflagged value was rewritten"


# ── the page's language, in the family that shortens German and Spanish titles ────────────────
#
# These instructions are written in French and the page often is not: on this customer's account
# the same shortening runs over DE, ES and FR pages. `_rewrite_for_query`, asked the same way,
# answered in French on an English page four runs out of four — so the risk here is measured,
# not hypothetical.

EN_TITLE = ("Kling AI pricing 2026: what the credits really cost for every plan and how to "
            "budget the whole thing properly")


def test_a_shortened_value_in_another_language_is_refused(monkeypatch) -> None:
    """The over-long value stays: it is a flagged anomaly the customer can see. A translated one
    is a page nobody flags and everybody reads."""
    monkeypatch.setattr(app_module, "_correction_ai_json", lambda **kw: {
        "value": "Kling AI prix 2026 : ce que coutent vraiment les credits et comment les gerer"})
    out = app_module._length_value_for_page(
        current=EN_TITLE, kind="title", url="https://site.fr/blog/kling-ai-pricing-2026",
        site_name="site.fr")
    assert out == ""


def test_the_page_language_is_named_in_the_shortening_prompt(monkeypatch) -> None:
    seen: list[str] = []

    def _fake(**kw):
        seen.append(kw.get("system", ""))
        return {"value": "Kling AI pricing 2026: what the credits cost and how to budget them"}

    monkeypatch.setattr(app_module, "_correction_ai_json", _fake)
    out = app_module._length_value_for_page(
        current=EN_TITLE, kind="title", url="https://site.fr/blog/kling-ai-pricing-2026",
        site_name="site.fr")
    assert out and any("anglais" in s for s in seen)


def test_a_value_whose_language_is_unreadable_is_left_to_the_model(monkeypatch) -> None:
    """Abstention: a title made of brand and keywords says nothing about its language, and
    refusing there would block correct shortenings for no reading at all."""
    monkeypatch.setattr(app_module, "_correction_ai_json", lambda **kw: {
        "value": "Kling AI Preise 2026: Credits und Tarife"})
    out = app_module._length_value_for_page(
        current="Kling AI Pricing 2026 Credits Plans Cost Guide Comparison Table Full Review",
        kind="title", url="https://site.fr/x", site_name="site.fr")
    assert out == "Kling AI Preise 2026: Credits und Tarife"
