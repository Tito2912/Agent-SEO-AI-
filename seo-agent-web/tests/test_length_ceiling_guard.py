"""A correction must not create the next anomaly.

Found by the defect gauntlet, not by a unit test: de-duplicating 26 identical meta descriptions
produced unique ones — correct — and two came out at 171 and 161 characters, over the 160
ceiling. The family that WRITES a description does not enforce the window the length family
enforces. Two families touch the same value; only one measured it.

The ceiling now applies where the value is written, whatever family wrote it, and only to a value
that patch actually changed.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402

LONG_DESC = "Une description volontairement tres longue. " * 5
LONG_TITLE = "Un titre volontairement beaucoup trop long pour le plafond retenu par le crawler."
OLD = ('<html><head><title>Court</title>'
       '<meta name="description" content="Ancienne description." /></head></html>')


def _desc(html: str) -> str:
    return re.search(r'name="description" content="([^"]*)"', html).group(1)


def test_a_description_this_patch_wrote_is_trimmed() -> None:
    new = OLD.replace("Ancienne description.", LONG_DESC)
    out, notes = app_module._enforce_length_ceilings(new, OLD)
    assert app_module._rendered_len(_desc(out)) <= app_module._LENGTH_CEILINGS["description"]
    assert notes and "plafond" in notes[0]


def test_a_title_this_patch_wrote_is_trimmed() -> None:
    new = OLD.replace("<title>Court</title>", f"<title>{LONG_TITLE}</title>")
    out, _ = app_module._enforce_length_ceilings(new, OLD)
    value = re.search(r"<title>(.*?)</title>", out).group(1)
    assert app_module._rendered_len(value) <= app_module._LENGTH_CEILINGS["title"]


def test_a_value_the_patch_did_not_touch_is_left_alone() -> None:
    """A pre-existing over-long title belongs to its own family, with its own pull request and
    its own note — not to a silent edit inside someone else's fix."""
    already = OLD.replace("Ancienne description.", LONG_DESC)
    out, notes = app_module._enforce_length_ceilings(already, already)
    assert out == already and notes == []


def test_front_matter_is_covered_too() -> None:
    """MDX and Jekyll write the same values as YAML; the gauntlet is HTML but customers are not."""
    out, notes = app_module._enforce_length_ceilings(
        '---\ntitle: "' + LONG_TITLE + '"\n---\n', '---\ntitle: "Court"\n---\n')
    value = re.search(r'title: "(.*)"', out).group(1)
    assert app_module._rendered_len(value) <= app_module._LENGTH_CEILINGS["title"]
    assert notes


def test_a_value_already_inside_the_ceiling_is_untouched() -> None:
    new = OLD.replace("Ancienne description.", "Une description neuve et parfaitement calibree.")
    out, notes = app_module._enforce_length_ceilings(new, OLD)
    assert out == new and notes == []


def test_the_guard_runs_on_every_patch_not_just_the_length_family() -> None:
    """The defect was precisely that only one family measured."""
    import inspect
    src = inspect.getsource(app_module._deep_patch_issue_files)
    assert "_enforce_length_ceilings(new_content, raw)" in src
    assert src.index("_enforce_length_ceilings") < src.index('patch.get("no_change")')
