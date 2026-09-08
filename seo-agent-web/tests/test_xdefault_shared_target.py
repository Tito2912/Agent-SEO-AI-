"""x-default belongs to the group, so it is fixed where the group is built.

Measured on prosperfactory.com: 70 pages miss an `x-default`, and every alternates map comes from
one shared helper — `buildAlternatesForSlugPath` in lib/seo.ts — which never emits one. The
corrector was about to edit 4 MDX content files out of the 70, files whose frontmatter does not
control the languages map at all: four wrong files, seventy pages still broken, and a pull
request that looks like a fix.

Same shape as the served-lang family. When no shared builder exists — a hand-written site writes
x-default in each page — nothing changes.
"""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402

BUILDER = "export async function buildAlternates() { return { canonical, languages }; }"
BUILDER_OK = BUILDER.replace("languages }", "languages, 'x-default': '/' }")
UNRELATED = "export function formatDate(d) { return d.toISOString(); }"


def _serve(files: dict[str, str], monkeypatch):
    def _get(path, **kw):
        name = path.split("/contents/")[1]
        if name not in files:
            raise AssertionError(f"unexpected read: {name}")
        return {"content": base64.b64encode(files[name].encode()).decode(), "sha": "s"}
    monkeypatch.setattr(app_module, "_github_api_get", _get)


def _find(paths, monkeypatch, files):
    _serve(files, monkeypatch)
    return app_module._shared_alternates_file(
        owner="o", repo_name="r", token="t", branch="main", all_paths=paths)


def test_it_finds_the_helper_that_builds_every_alternates_map(monkeypatch) -> None:
    assert _find(["lib/seo.ts", "content/es/x.mdx"], monkeypatch,
                 {"lib/seo.ts": BUILDER}) == "lib/seo.ts"


def test_a_helper_that_already_emits_x_default_is_not_the_problem(monkeypatch) -> None:
    assert _find(["lib/seo.ts"], monkeypatch, {"lib/seo.ts": BUILDER_OK}) == ""


def test_a_file_that_never_touches_alternates_is_not_a_target(monkeypatch) -> None:
    """A repository can carry lib/seo.ts that only formats dates."""
    assert _find(["lib/seo.ts"], monkeypatch, {"lib/seo.ts": UNRELATED}) == ""


def test_a_hand_written_site_keeps_being_fixed_page_by_page(monkeypatch) -> None:
    assert _find(["index.html", "en/index.html"], monkeypatch, {}) == ""


def test_the_override_reaches_the_patcher(monkeypatch) -> None:
    _serve({"lib/seo.ts": BUILDER}, monkeypatch)
    prep = app_module._prepare_issue_fix(
        issue_key="x_default_hreflang_missing", issues={}, impacted=["https://x.fr/es/a"],
        all_paths=["lib/seo.ts", "content/es/a.mdx"], site_name="x.fr", owner="o",
        repo_name="r", branch="main", token="t", model_override="")
    assert prep["targets_override"] == ["lib/seo.ts"]
    assert "une seule fois" in prep["extra_hint"]
    import inspect
    src = inspect.getsource(app_module.api_issue_deep_fix)
    assert 'targets_override=_prep.get("targets_override")' in src


def test_without_a_shared_builder_nothing_is_forced(monkeypatch) -> None:
    _serve({}, monkeypatch)
    prep = app_module._prepare_issue_fix(
        issue_key="x_default_hreflang_missing", issues={}, impacted=["https://x.fr/a"],
        all_paths=["index.html"], site_name="x.fr", owner="o", repo_name="r",
        branch="main", token="t", model_override="")
    assert prep["targets_override"] is None
