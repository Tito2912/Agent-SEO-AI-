"""A bounded rewriter is not necessarily a model-free one, and three decisions read that.

`_deep_patch_issue_files` treats any `link_rewriter` as a deterministic repair: the file is left
out of `ai_files`, which drives all three of

  * billing — `_correction_charge(user, len(ai_files))`;
  * the PR body — `_fix_nature_note(bool(ai_files))`, "✅ Correctif mécanique" vs "à relire";
  * auto-merge — Full Access merges when `not ai_files`.

That was exactly right while every bounded rewriter was a regex. It stopped being right on
2026-08-29 (commit 6d07d60), when the title/meta length families were rewired to ask the model
for the VALUE and enforce the length in code: still bounded, still safe in scope — but written
by the model, and paid for in tokens. Registered as a `link_rewriter`, those families billed
nothing, shipped a PR claiming the diff came from the crawl "sans modèle", and on a Full Access
project merged model-written titles into a customer's site with nobody reading them.

The keyword snippet rewrite has the same shape, which is how this was noticed.
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

LONG = "x" * 300
SOURCE = f'<title>{LONG}</title>\n'


def _patch(monkeypatch, **kwargs):
    """Drive `_deep_patch_issue_files` over one file, with GitHub replaced by a recorder."""
    import base64

    committed: list[str] = []
    monkeypatch.setattr(app_module, "_github_api_get", lambda path, **kw: {
        "content": base64.b64encode(SOURCE.encode()).decode(), "sha": "sha-1"})
    monkeypatch.setattr(app_module, "_github_api_put", lambda path, **kw: (
        committed.append(path), {"content": {"sha": "sha-2"}})[1])
    monkeypatch.setattr(app_module, "_github_tarball_grep", lambda *a, **kw: [])

    patched, skipped, targets, ai_files = app_module._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=["page.html"], issue_key="title_too_long_indexable",
        issue_label="Titres trop longs", impacted_urls=["https://site.fr/page"],
        site_name="site.fr", file_state={}, max_files=4,
        targets_override=["page.html"], **kwargs,
    )
    return patched, ai_files


def test_a_rewriter_that_calls_the_model_marks_its_files_as_model_written(monkeypatch) -> None:
    """`ai_files` is what billing, the PR badge and auto-merge all read."""
    patched, ai_files = _patch(
        monkeypatch,
        link_rewriter=lambda raw: (raw.replace(LONG, "Un titre court"), 1),
        rewriter_is_ai=True,
    )
    assert patched == ["page.html"]
    assert ai_files == ["page.html"], "a model-written file was recorded as a mechanical repair"


def test_a_regex_rewriter_stays_free_and_mechanical(monkeypatch) -> None:
    """The distinction has to cut both ways: charging a bounded regex rewrite would sell compute
    that was never spent, which is why the first customer PR was free."""
    patched, ai_files = _patch(
        monkeypatch,
        link_rewriter=lambda raw: (raw.replace(LONG, "Un titre court"), 1),
    )
    assert patched == ["page.html"]
    assert ai_files == [], "a rewrite that spends no tokens must cost the customer nothing"


def test_the_length_families_declare_themselves_model_written() -> None:
    """The regression that started this: `_rewrite_length_values` calls `_length_value_for_page`,
    which calls the model — the family is bounded in SCOPE, not free of the model."""
    issues = {
        "title_too_long_indexable": {
            "count": 1,
            "length_samples": {"https://site.fr/page": {"rendered": LONG, "len": 300}},
        }
    }
    prep = app_module._prepare_issue_fix(
        issue_key="title_too_long_indexable", issues=issues,
        impacted=["https://site.fr/page"], all_paths=["page.html"], site_name="site.fr",
        owner="o", repo_name="r", branch="main", token="t",
    )
    assert prep["link_rewriter"] is not None, "the family lost its bounded rewriter"
    assert prep["rewriter_is_ai"] is True, (
        "the length families ask the model for the value: billed, flagged for review, "
        "and never auto-merged"
    )


def test_the_keyword_rewrite_is_declared_model_written_too() -> None:
    """Same shape, same answer — this is the family the flag was found on."""
    assert app_module._KEYWORD_REWRITE_KEY == "keyword_snippet_rewrite"


def test_a_deterministic_family_still_declares_itself_mechanical() -> None:
    """`_rewrite_redirect_links` is a regex over href values: no call, no bill, and the
    "correctif mécanique" badge it carries is accurate."""
    issues = {
        "page_has_links_to_redirect_indexable": {
            "count": 1,
            "redirect_link_samples": [
                {"from": "https://site.fr/a/", "to": "https://site.fr/a"}
            ],
        }
    }
    prep = app_module._prepare_issue_fix(
        issue_key="page_has_links_to_redirect_indexable", issues=issues,
        impacted=["https://site.fr/page"], all_paths=["page.html"], site_name="site.fr",
        owner="o", repo_name="r", branch="main", token="t",
    )
    assert prep["link_rewriter"] is not None
    assert prep["rewriter_is_ai"] is False
