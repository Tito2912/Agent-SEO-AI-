"""An image explicitly marked decorative is not an image missing its alt text.

Measured on prosperfactory.com: 70 `missing_alt_text` occurrences, all one logo, rendered as
`<img src=... alt="" aria-hidden="true">` followed by `<span class="sr-only">Prosper Factory</span>`.
That is the textbook decorative pattern — the link's accessible name comes from the span. The
corrector was about to write alt text onto an element assistive technology is told to ignore:
dead text that clears a counter and undoes a correct decision by the developer.

`alt=""` is a declaration, not an omission — but only where the markup says so.
"""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[2] / "skills" / "public" / "seo-autopilot" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import seo_audit  # noqa: E402


def _count(html: str) -> int:
    parser = seo_audit.PageHTMLExtractor()
    parser.feed(html)
    return parser.images_missing_alt


def test_an_aria_hidden_image_with_empty_alt_is_decorative() -> None:
    assert _count('<img src="/logo.png" alt="" aria-hidden="true">') == 0


def test_role_presentation_counts_too() -> None:
    assert _count('<img src="/logo.png" alt="" role="presentation">') == 0
    assert _count('<img src="/logo.png" alt="" role="none">') == 0


def test_a_bare_empty_alt_is_still_flagged() -> None:
    """Ambiguous is worth reporting: nothing says this image is decorative."""
    assert _count('<img src="/photo.png" alt="">') == 1


def test_no_alt_attribute_at_all_is_always_flagged() -> None:
    """The real defect. aria-hidden does not excuse a missing attribute."""
    assert _count('<img src="/photo.png">') == 1
    assert _count('<img src="/photo.png" aria-hidden="true">') == 1


def test_a_real_image_with_text_is_never_counted() -> None:
    assert _count('<img src="/photo.png" alt="Un graphique">') == 0


def test_aria_hidden_false_is_not_a_marker() -> None:
    assert _count('<img src="/photo.png" alt="" aria-hidden="false">') == 1
