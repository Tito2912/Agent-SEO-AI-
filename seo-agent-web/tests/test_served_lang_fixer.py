"""The first correction that CREATES a file — and the reasons it is allowed to.

`served_html_lang_mismatch` was measured on a real customer site: 93 pages ship `<html lang="en">`
and correct it to fr/de/es only after hydration. On a statically generated site the root layout
cannot know the route, so there is nothing in the source a bounded patch can change — the family's
first pull request proved it by reaching for `headers()` in a Next.js `output: 'export'` app and
failing the build.

The fix is post-build, and the file is written HERE rather than by the model. That is the whole
safety argument: an invented file reads worse in review than a modified line, and this one needs
no invention — every flagged page already declares its language through the hreflang pointing at
its own canonical, which is exactly how the anomaly was detected in the first place.

Verified with node against real pages from the customer's site before being embedded: 2 of 4
corrected, the English home and a page without canonical left untouched.
"""

from __future__ import annotations

import base64
import json
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

PKG = {"name": "site", "scripts": {"build": "next build", "dev": "next dev"}}


@pytest.fixture()
def github(monkeypatch):
    """Record what the fixer writes, and serve it a package.json."""
    written: dict[str, str] = {}

    def _get(path, **kw):
        if path.endswith("/package.json"):
            return {"content": base64.b64encode(json.dumps(PKG).encode()).decode(), "sha": "pkg-sha"}
        raise AssertionError(f"unexpected GET {path}")

    def _put(path, **kw):
        body = kw.get("json_body") or {}
        written[path.split("/contents/")[1]] = base64.b64decode(body["content"]).decode("utf-8")
        return {"content": {"sha": "new-sha"}}

    monkeypatch.setattr(app_module, "_github_api_get", _get)
    monkeypatch.setattr(app_module, "_github_api_put", _put)
    return written


def _run(all_paths):
    return app_module._deep_fix_served_html_lang(
        owner="o", repo_name="r", token="t", fix_branch="fix",
        all_paths=all_paths, file_state={},
    )


def test_it_adds_the_fixer_and_chains_it_after_the_existing_build(github) -> None:
    changed, notes = _run(["package.json", "app/layout.tsx"])
    assert changed == ["scripts/fix-html-lang.mjs", "package.json"]
    pkg = json.loads(github["package.json"])
    # The existing command is kept, not replaced: whatever the project already did still runs.
    assert pkg["scripts"]["build"] == "next build && node scripts/fix-html-lang.mjs"
    assert pkg["scripts"]["dev"] == "next dev", "an unrelated script was rewritten"
    assert notes and any("hreflang" in n for n in notes)


def test_the_script_it_writes_is_the_one_we_wrote(github) -> None:
    """Deterministic means deterministic: the bytes committed are the constant, not a rendering
    of it, and not something a model produced."""
    _run(["package.json"])
    assert github["scripts/fix-html-lang.mjs"] == app_module._HTML_LANG_FIXER_SCRIPT
    body = github["scripts/fix-html-lang.mjs"]
    # The two properties the whole approach rests on.
    assert "rel=[\"']canonical[\"']" in body, "it must locate the page's own canonical"
    assert "x-default" in body, "x-default is not a language and must be skipped"


def test_running_it_twice_does_not_chain_it_twice(github) -> None:
    already = dict(PKG, scripts={"build": "next build && node scripts/fix-html-lang.mjs"})

    def _get(path, **kw):
        return {"content": base64.b64encode(json.dumps(already).encode()).decode(), "sha": "s"}

    app_module._github_api_get = _get  # type: ignore[assignment]
    changed, notes = _run(["package.json", "scripts/fix-html-lang.mjs"])
    assert changed == [] and any("déjà branché" in n for n in notes)


def test_a_project_without_an_npm_build_is_refused_with_its_reason(github) -> None:
    """Hugo and Jekyll have no build script to chain onto. Saying so is the product; writing a
    file into a repository that cannot run it is not."""
    changed, notes = _run(["config.toml", "layouts/_default/baseof.html"])
    assert changed == []
    assert notes and "package.json" in notes[0]
    assert "gabarit" in notes[0], "the refusal must say where the fix belongs instead"


def test_a_package_without_a_build_script_is_refused(github, monkeypatch) -> None:
    monkeypatch.setattr(app_module, "_github_api_get", lambda path, **kw: {
        "content": base64.b64encode(json.dumps({"name": "x", "scripts": {"dev": "vite"}}).encode()).decode(),
        "sha": "s"})
    changed, notes = _run(["package.json"])
    assert changed == [] and any("build" in n for n in notes)


def test_the_family_never_goes_through_the_content_patcher() -> None:
    """Its targets would be the root layout — the file that cannot know the route, and the one
    whose patch broke a customer's build."""
    assert "served_html_lang_mismatch" in app_module._SERVED_LANG_FIX_KEYS
    source = __import__("inspect").getsource(app_module.api_issue_deep_fix)
    assert "_SERVED_LANG_FIX_KEYS" in source
    assert source.index("_SERVED_LANG_FIX_KEYS") < source.index("_deep_patch_issue_files")
