"""A "just in case" redirect rule that killed a homepage.

Measured on videocaptionstudio.com, whose home was returning 404 in production:

    //:splat    /:splat    301!      # "handle stray double slashes"

Netlify collapses repeated slashes in the source pattern, so the rule becomes `/:splat` ->
`/:splat`: a catch-all redirecting every URL to itself, with the placeholder emitted literally.
`https://videocaptionstudio.com/` -> 301 -> `/:splat` -> 404.

The per-path stripper could never see it: that one works from a list of flagged paths, and this
rule names no path at all — it matches everything.
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

REAL = "\n".join([
    "# 1) Force HTTPS + apex",
    "http://site.com/*           https://site.com/:splat 301!",
    "https://www.site.com/*      https://site.com/:splat 301!",
    "/index.html                 /                       301!",
    "/old/*                      /new/:splat             301",
    "/app/*                      /index.html             200",
    "# 3) Handle stray double slashes",
    "//:splat                    /:splat                 301!",
]) + "\n"


def test_it_removes_the_catch_all_that_loops() -> None:
    out, removed = app_module._strip_self_referential_wildcards(REAL)
    assert len(removed) == 1 and removed[0].startswith("//:splat")
    assert "//:splat" not in out


def test_the_apex_and_https_rules_survive() -> None:
    """Those look similar — wildcard source, :splat target — but they go to another HOST."""
    out, _ = app_module._strip_self_referential_wildcards(REAL)
    assert "http://site.com/*" in out and "https://www.site.com/*" in out


def test_a_rule_that_goes_somewhere_else_is_kept() -> None:
    out, _ = app_module._strip_self_referential_wildcards(REAL)
    assert "/old/*" in out, "a real move must not be dropped"
    assert "/app/*" in out, "an SPA rewrite must not be dropped"


def test_a_comment_is_never_parsed_as_a_rule() -> None:
    out, removed = app_module._strip_self_referential_wildcards(
        "# //:splat /:splat 301!\n/a /b 301\n")
    assert removed == [] and out.startswith("#")


def test_a_rule_without_a_wildcard_is_left_to_the_per_path_stripper() -> None:
    out, removed = app_module._strip_self_referential_wildcards("/de/ /de 301!\n")
    assert removed == [] and "/de/" in out


def test_an_identical_source_and_target_is_not_touched_here() -> None:
    """`/x/* /x/* 301` is already a no-op Netlify rejects; removing it is not this rule's job,
    and pretending otherwise would widen a bounded fix."""
    out, removed = app_module._strip_self_referential_wildcards("/x/* /x/* 301\n")
    assert removed == [] and "/x/*" in out
