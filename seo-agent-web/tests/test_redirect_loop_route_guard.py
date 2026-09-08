"""A self-redirect fixer that only knew hand-written sites.

Measured on easyshopbuilder.com: `/de`, `/en`, `/es` each 301 to themselves — an infinite loop —
and 61 pages link to them. The rule stripper handles it perfectly (12 lines, exactly the
canonical trio plus its 200-rewrite). It was never reached: the guard in front demanded a flat
`public/de.html`, while the route lives in `app/de/page.tsx`. A Next.js export — one of the nine
stacks the product advertises — got "nothing to fix" on a site that visibly loops.

The guard exists to be sure something still serves the clean URL once the rules are dropped. A
route source file proves that as well as a flat file, and the repo index already answers it.
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

import pytest  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import repo_index  # noqa: E402

REDIRECTS = "\n".join([
    "/de.html /de 301!",
    "/de/ /de 301!",
    "/de/index.html /de 301!",
    "/de /de.html 200",
    "/blog/* /articles/:splat 301",     # unrelated: must survive
]) + "\n"


@pytest.fixture()
def github(monkeypatch):
    written: dict[str, str] = {}

    def _get(path, **kw):
        import base64
        return {"content": base64.b64encode(REDIRECTS.encode()).decode(), "sha": "s"}

    def _put(path, **kw):
        import base64
        written[path.split("/contents/")[1]] = base64.b64decode(
            (kw.get("json_body") or {})["content"]).decode()
        return {"content": {"sha": "new"}}

    monkeypatch.setattr(app_module, "_github_api_get", _get)
    monkeypatch.setattr(app_module, "_github_api_put", _put)
    return written


NEXT_TREE = ["package.json", "next.config.mjs", "public/_redirects", "app/de/page.tsx"]


def test_a_framework_route_serves_the_url_just_as_well_as_a_flat_file(github) -> None:
    changed, notes = app_module._deep_fix_redirect_config_loops(
        owner="o", repo_name="r", token="t", fix_branch="fix", all_paths=NEXT_TREE,
        loop_paths=["/de"], file_state={}, index=repo_index.build_repo_index(NEXT_TREE))
    assert changed == ["public/_redirects"], notes
    body = github["public/_redirects"]
    assert "/de/ /de 301!" not in body and "/de /de.html 200" not in body
    assert "/blog/* /articles/:splat 301" in body, "an unrelated rule was dropped"
    assert any("app/de/page.tsx" in n for n in notes), "the note must name what serves the URL"


def test_without_the_index_the_old_blindness_returns(github) -> None:
    """Pins the actual defect: the same repository, no index, and the fixer walks away."""
    changed, _ = app_module._deep_fix_redirect_config_loops(
        owner="o", repo_name="r", token="t", fix_branch="fix", all_paths=NEXT_TREE,
        loop_paths=["/de"], file_state={}, index=None)
    assert changed == []


def test_an_unresolvable_path_is_refused_out_loud(github) -> None:
    """An empty result on a site that visibly loops reads as "nothing to fix" — the most
    misleading thing the corrector can say."""
    changed, notes = app_module._deep_fix_redirect_config_loops(
        owner="o", repo_name="r", token="t", fix_branch="fix",
        all_paths=["public/_redirects"], loop_paths=["/de"], file_state={},
        index=repo_index.build_repo_index(["public/_redirects"]))
    assert changed == []
    assert notes and "404" in notes[0]
