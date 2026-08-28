"""The corrector's target window must stay inside the crawler's own thresholds.

The first model-written PR on a customer account (voiceoverstudioai.com #2) shortened titles
from 81 to 47-50 characters. The model was not misbehaving: nothing is flagged until 70, and the
hint asked for "50-60". It obeyed a badly calibrated instruction, and every patched page lost
~20 characters of keyword surface for no correctness gain.

The window and the threshold live in two different files — `_LENGTH_WINDOWS` in backend/app.py,
`TITLE_TOO_LONG` / `DESC_TOO_LONG` in the crawler — so nothing stopped them drifting apart. These
tests read the REAL constants out of the crawler source and pin the relationship, so raising a
threshold without retuning the prompt fails here instead of in a customer's pull request.

seo_audit.py is imported by path (no playwright at module level), the same way
test_crawler_evidence.py does it.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402

CRAWLER = (
    WEB_ROOT.parent / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
)


def _crawler_threshold(name: str) -> int:
    """Read a threshold from the crawler source rather than duplicating its value here."""
    src = CRAWLER.read_text(encoding="utf-8", errors="replace")
    m = re.search(rf"^\s*{name}\s*=\s*(\d+)", src, re.M)
    assert m, f"{name} not found in {CRAWLER}"
    return int(m.group(1))


@pytest.mark.parametrize(
    "kind, too_long, too_short",
    [
        ("title", "TITLE_TOO_LONG", "TITLE_TOO_SHORT"),
        ("description", "DESC_TOO_LONG", "DESC_TOO_SHORT"),
    ],
)
def test_the_window_sits_inside_the_thresholds_it_is_meant_to_satisfy(
    kind: str, too_long: str, too_short: str
) -> None:
    low, high = app_module._LENGTH_WINDOWS[kind]
    ceiling = _crawler_threshold(too_long)
    floor = _crawler_threshold(too_short)

    assert high <= ceiling, (
        f"the corrector aims {kind}s at up to {high} chars, but the crawler flags above "
        f"{ceiling}: it would produce the very issue it is fixing"
    )
    assert low > floor, (
        f"the corrector may write a {kind} of {low} chars, which the crawler flags as too short "
        f"(< {floor}): the fix would trade one anomaly for another"
    )
    assert low < high, f"the {kind} window is inverted or empty: {(low, high)}"


@pytest.mark.parametrize(
    "kind, too_long", [("title", "TITLE_TOO_LONG"), ("description", "DESC_TOO_LONG")]
)
def test_the_window_does_not_waste_the_room_it_is_given(kind: str, too_long: str) -> None:
    """Aiming far below the ceiling is what cost PR#2 ~20 characters per page.

    A margin is needed (the rendered string can carry a template suffix), but a window whose top
    sits far under the threshold throws away keyword surface on every page it touches.
    """
    _low, high = app_module._LENGTH_WINDOWS[kind]
    ceiling = _crawler_threshold(too_long)
    margin = ceiling - high

    assert margin >= 2, f"{kind}: no room left for a template suffix (margin {margin})"
    assert margin <= ceiling * 0.10, (
        f"{kind}: the window stops {margin} chars below the {ceiling}-char threshold, so every "
        f"patched page is shortened further than anything requires"
    )


def test_the_hint_tells_the_model_to_aim_high_and_keep_the_language() -> None:
    """The window alone did not stop it: PR#2 landed at the BOTTOM of the range it was given."""
    issues = {
        "title_too_long_indexable": {
            "count": 1,
            "length_samples": {
                "https://exemple.fr/a": {
                    "rendered": "Un titre beaucoup trop long qui dépasse la limite du crawler de loin",
                    "len": 81,
                }
            },
        }
    }
    hint = app_module._build_length_hint(issues, {"title_too_long_indexable"}, "title")

    low, high = app_module._LENGTH_WINDOWS["title"]
    assert f"{low}-{high} caractères" in hint
    assert "NE RACCOURCIS PAS PLUS QUE NÉCESSAIRE" in hint
    assert f"vise le HAUT de la fenêtre ({high} car.)" in hint
    assert "garde la langue de la page" in hint, (
        "nothing forbade the model translating a German title into French"
    )
    assert "https://exemple.fr/a" in hint and "81 car." in hint, (
        "the hint lost the per-page evidence it is built from"
    )


def test_no_hint_at_all_when_the_crawl_carries_no_samples() -> None:
    # A hint invented without evidence is how a corrector patches pages it was never shown.
    assert app_module._build_length_hint({}, {"title_too_long_indexable"}, "title") == ""
