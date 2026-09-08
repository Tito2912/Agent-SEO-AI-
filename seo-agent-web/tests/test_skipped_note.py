"""What the corrector could NOT fix belongs in the pull request.

Measured on make-avis.com: 13 pages flagged for a meta description outside the 140-155 window,
5 corrected, 8 left alone because the model could not reach the window in two attempts — thin
pages (contact, about) with little to expand from. Refusing to write an out-of-window value is
right. Saying nothing is not: the pull request announced 5 files, and nothing told the owner that
8 pages stay flagged or why.
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

SKIPPED = ["content/contact.mdx", "content/en/contact.mdx"]
TARGETS = SKIPPED + ["content/es/methodology.mdx"]


def test_it_names_the_files_and_the_count() -> None:
    note = app_module._skipped_note("meta_description_too_short_indexable", SKIPPED, TARGETS)
    assert "2 fichier(s) sur 3" in note
    assert "content/contact.mdx" in note and "content/en/contact.mdx" in note
    assert "resteront signalées" in note


def test_a_length_family_gets_the_reason_that_actually_applies() -> None:
    """"The model returned nothing" is true but useless. On a thin page the real answer is that
    reaching the window would mean inventing content."""
    note = app_module._skipped_note("title_too_long", SKIPPED, TARGETS)
    assert "fenêtre" in note and "inventer du contenu" in note


def test_other_families_get_the_plain_reason() -> None:
    note = app_module._skipped_note("missing_alt_text", SKIPPED, TARGETS)
    assert "rien renvoyé d'exploitable" in note
    assert "inventer du contenu" not in note


def test_nothing_skipped_says_nothing() -> None:
    assert app_module._skipped_note("missing_title", [], TARGETS) == ""
    assert app_module._skipped_note("missing_title", None, TARGETS) == ""


def test_the_note_reaches_the_pull_request_body() -> None:
    import inspect
    src = inspect.getsource(app_module.api_issue_deep_fix)
    assert "_skipped_note(issue_key, skipped, targets)" in src
    assert src.index("_skipped_note") < src.index("pr_body = (") + len(src)
