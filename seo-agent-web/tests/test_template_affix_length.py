"""A `| Brand` suffix silently disabled the whole length machinery.

Measured on make-avis.com: every rendered title is `<file title> | Make Avis`. The bounded
rewriter only acted when the RENDERED value appeared verbatim in the file, so the match failed on
all 17 titles and the correction fell through to the free-form model patch — which has no window,
no retry and no refusal. 6 of 17 rendered titles landed outside 60-68, two of them above the 70
ceiling a deterministic trim is supposed to guarantee. The descriptions, whose rendered value IS
the file value, were 5 for 5 inside.

A brand suffix is not exotic; it is what most sites do.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402

# Long enough to exceed the 70-character ceiling once rendered — the only case this rewriter
# acts on, and the case make-avis.com actually presented.
TITLE = "Make vs Zapier (2026) : quel outil d automatisation choisir vraiment aujourd hui ?"
FILE = '---\ntitle: "' + TITLE + '"\n---\n'
RENDERED = TITLE + " | Make Avis"


def test_it_finds_the_part_the_file_actually_holds() -> None:
    base, affix = app_module._value_without_template_affix(RENDERED, FILE)
    assert base == 'Make vs Zapier (2026) : quel outil d automatisation choisir vraiment aujourd hui ?'
    assert affix == len(" | Make Avis")


def test_the_window_shrinks_by_what_the_template_adds() -> None:
    """The searcher sees the rendered length; the model is handed the file's share of it."""
    low, high = app_module._LENGTH_WINDOWS["title"]
    _base, affix = app_module._value_without_template_affix(RENDERED, FILE)
    assert affix > 0
    assert low - affix < low and high - affix < high


def test_no_separator_means_no_guessing() -> None:
    assert app_module._value_without_template_affix("Un titre simple", FILE) == ("", 0)


def test_a_two_word_head_is_not_a_split() -> None:
    """A stray separator inside a sentence must not be read as a brand suffix."""
    assert app_module._value_without_template_affix("Blog | Make Avis", "Blog") == ("", 0)


def test_the_longest_real_split_wins() -> None:
    """`A - B | Brand` splits at the brand, not at the first separator it meets."""
    content = "title: Guide complet - webhooks et automatisation"
    rendered = "Guide complet - webhooks et automatisation | Make Avis"
    base, affix = app_module._value_without_template_affix(rendered, content)
    assert base == "Guide complet - webhooks et automatisation"
    assert affix == len(" | Make Avis")


def test_the_bounded_rewriter_no_longer_stands_down(monkeypatch) -> None:
    """The defect itself: with a suffix, `rendered not in content` sent the whole family to the
    free-form patch. It must now reach the measured rewriter."""
    seen: dict[str, object] = {}

    def _fake(*, current, kind, url, site_name, model_override="", affix_len=0):
        seen["current"], seen["affix"] = current, affix_len
        return "Make vs Zapier (2026) : le comparatif"

    monkeypatch.setattr(app_module, "_length_value_for_page", _fake)
    assert len(RENDERED) > app_module._LENGTH_CEILINGS["title"], "the sample must be over the ceiling"
    out, n = app_module._rewrite_length_values(
        FILE, {"https://x.fr/a": {"rendered": RENDERED, "len": len(RENDERED)}}, "title")
    assert n == 1, "the rewriter stood down on a templated title"
    assert seen["current"] == 'Make vs Zapier (2026) : quel outil d automatisation choisir vraiment aujourd hui ?'
    assert seen["affix"] == len(" | Make Avis")
    assert "le comparatif" in out and "Make Avis" not in out
