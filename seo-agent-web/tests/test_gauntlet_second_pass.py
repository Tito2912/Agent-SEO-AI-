"""Two defects the gauntlet found on its second pass.

Both were invisible to unit tests because both need a correction to run against a page that
another family is also flagging.

1. Repairing a canonical that pointed at a non-canonical page COPIED that page's declared
   canonical — which was `http://` on an https site, flagged by another family in the same run.
   The page came out worse than it went in.

2. `title_too_short` was never repaired: the bounded rewriter only acted on values above the
   ceiling, so a four-character title was handed to nobody. The same asymmetry as the length
   ceiling, in the other direction.
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

OLD = ('<link rel="canonical" href="https://s.fr/a.html" />\n'
       '<meta property="og:url" content="https://s.fr/a.html" />\n'
       '<a href="http://un-tiers.example/page">tiers</a>\n')


def test_a_patch_may_not_downgrade_https_to_http() -> None:
    new = OLD.replace('href="https://s.fr/a.html"', 'href="http://s.fr/a.html"')
    out, notes = app_module._forbid_https_downgrade(new, OLD)
    assert 'href="https://s.fr/a.html"' in out
    assert notes and "https" in notes[0]


def test_a_third_party_that_was_always_http_is_left_alone() -> None:
    """The rule restores what WAS https. It does not go around upgrading other people's URLs."""
    out, notes = app_module._forbid_https_downgrade(OLD, OLD)
    assert out == OLD and notes == []
    assert "http://un-tiers.example/page" in out


def test_og_url_is_covered_as_well_as_canonical() -> None:
    new = OLD.replace('content="https://s.fr/a.html"', 'content="http://s.fr/a.html"')
    out, _ = app_module._forbid_https_downgrade(new, OLD)
    assert 'content="https://s.fr/a.html"' in out


def test_a_value_below_the_window_now_reaches_the_rewriter(monkeypatch) -> None:
    """The defect: a four-character title was skipped because it was under the ceiling."""
    seen: list[str] = []

    def _fake(*, current, kind, url, site_name, model_override="", affix_len=0):
        seen.append(current)
        return "Un titre reecrit a la bonne longueur pour la fenetre visee ici"

    monkeypatch.setattr(app_module, "_length_value_for_page", _fake)
    out, n = app_module._rewrite_length_values(
        "<title>Test</title>", {"https://x.fr/a": {"rendered": "Test", "len": 4}}, "title")
    assert n == 1 and seen == ["Test"], "a too-short value never reached the rewriter"
    assert "Test</title>" not in out


def test_a_value_inside_the_band_is_still_left_alone(monkeypatch) -> None:
    """Only what the CRAWLER flags is touched. A 120-character description is short of ideal and
    flagged by nobody — rewriting it would churn a value no crawl complains about. That is why
    the trigger is the crawler threshold (15 / 100), not the optimal window floor (60 / 140)."""
    monkeypatch.setattr(app_module, "_length_value_for_page",
                        lambda **kw: (_ for _ in ()).throw(AssertionError("must not be called")))
    inside = "Un titre parfaitement acceptable qui tient dans la fenetre visee"
    out, n = app_module._rewrite_length_values(
        f"<title>{inside}</title>",
        {"https://x.fr/a": {"rendered": inside, "len": len(inside)}}, "title")
    assert n == 0 and inside in out


def test_both_guards_run_on_every_patch() -> None:
    import inspect
    src = inspect.getsource(app_module._deep_patch_issue_files)
    assert "_forbid_https_downgrade(new_content, raw)" in src
    assert "_enforce_length_ceilings(new_content, raw)" in src


def test_the_trigger_is_the_crawler_threshold_not_the_window(monkeypatch) -> None:
    monkeypatch.setattr(app_module, "_length_value_for_page",
                        lambda **kw: (_ for _ in ()).throw(AssertionError("must not be called")))
    ok = "J" * 120  # under the 140 window floor, over the 100 crawler threshold: nobody flags it
    out, n = app_module._rewrite_length_values(
        f'description="{ok}"', {"https://x.fr/a": {"rendered": ok, "len": 120}}, "meta")
    assert n == 0 and ok in out
    assert app_module._LENGTH_FLOORS == {"title": 15, "description": 100}
