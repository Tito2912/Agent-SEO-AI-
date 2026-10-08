"""A legacy absence counter must not silently authorize an HTML canonical patch."""

from types import SimpleNamespace

import pytest

from backend import app as m, audit_dashboard as dash, fix_suggestions as fs

KEY = "missing_canonical"
KEYS = (KEY, KEY + "_indexable", KEY + "_not_indexable", KEY.upper(), " " + KEY + " ")
URL = "https://site.test/page"


def forbid_io(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("A retired diagnostic must not contact GitHub, HTTP or an AI provider")

    for name in ("_github_api_get", "_github_api_post", "_github_api_put", "_openai_generate_file_patch",
                 "_ai_pick_repo_files", "_ai_map_urls_to_files", "_sitemap_https_page"):
        monkeypatch.setattr(m, name, forbidden)


@pytest.mark.parametrize("key", KEYS)
def test_legacy_absence_is_neither_claimed_nor_offered(key):
    assert not m._github_issue_auto_fixable(key)
    assert key.strip().lower() not in m._handled_issue_keys()


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("paths", [[], ["page.html", "app/layout.tsx", "sitemap.xml"]])
def test_preparation_refuses_before_targeting_or_provider(monkeypatch, key, paths):
    forbid_io(monkeypatch)
    prep = m._prepare_issue_fix(issue_key=key, issues={key: {"count": 1, "examples": [URL]}},
        impacted=[URL], all_paths=paths, site_name="site.test", owner="fixture", repo_name="fixture",
        branch="main", token="unused", pages=[{"url": URL, "status_code": 200, "canonical": None}])
    assert prep["refusal"] and "diagnostic" in prep["refusal"]
    assert prep["link_rewriter"] is None and not prep["rewriter_ai_fallback"]
    assert not prep["rewriter_is_ai"] and prep["targets_override"] is None


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("forged_plan", [False, True])
def test_direct_patch_refuses_even_with_cached_sources_and_a_forged_rewriter(monkeypatch, key, forged_plan):
    forbid_io(monkeypatch)
    cache = {"page.html": {"sha": "cached", "content": "<html><head></head></html>"}}
    snapshot = {path: dict(row) for path, row in cache.items()}
    extra = {"link_rewriter": lambda raw: (raw + "patched", 1), "targets_override": ["page.html"]} if forged_plan else {}
    patched, skipped, targets, ai_files = m._deep_patch_issue_files(owner="fixture", repo_name="fixture",
        branch="main", token="unused", fix_branch="qa", all_paths=["page.html"], issue_key=key,
        issue_label=key, impacted_urls=[URL], site_name="site.test", file_state=cache, **extra)
    assert not patched and not targets and not ai_files
    assert skipped and "diagnostic" in skipped[0]
    assert cache == snapshot


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("prep", [{}, {"refusal": None, "loop_paths": ["netlify.toml"]},
                                  {"refusal": "old", "rewriter_ai_fallback": True}])
def test_executor_refuses_incomplete_or_forged_preparation_before_io(monkeypatch, key, prep):
    forbid_io(monkeypatch)
    monkeypatch.setattr(m, "_deep_patch_issue_files", lambda **kwargs: pytest.fail("Executor must refuse first"))
    result = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="main", token="unused",
        fix_branch="qa", all_paths=["page.html", "netlify.toml"], issue_key=key, issue_label=key,
        impacted=[URL], site_name="site.test", file_state={}, max_files=8, prep=prep, pages=[], index=None)
    assert result["error"] and "diagnostic" in result["error"]
    assert all(result[name] == [] for name in ("patched", "skipped", "targets", "ai_files", "config_changes", "config_notes"))


@pytest.mark.parametrize("key", KEYS)
def test_manual_advice_does_not_promise_an_automatic_self_canonical(key):
    out = fs.suggest_issue_fix(issue_key=key, label=key, category="Canonicals", severity="warning", count=1,
        report={"issues": {key: {"count": 1, "examples": [URL]}}}, site_name="site.test", base_url=URL)
    assert out.get("auto_fixable") is False and out.get("mode") == "suggest-only"
    assert "diagnostic" in out["auto_fix_note"]
    assert any("doublons" in line for line in out["fix"])
    assert not any("Ajouter une balise" in line for line in out["fix"])


@pytest.mark.parametrize("key", KEYS)
def test_old_report_variants_stay_hidden_and_do_not_become_bulk_candidates(key):
    report = {"issues": {key: {"count": 1, "examples": [URL]}}, "pages": []}
    summary = dash.summarize_report(report)
    assert not summary["issues"]
    project = SimpleNamespace(site_name="site.test", slug="fixture", base_url=URL)
    assert m._github_fixable_issue_candidates(report=report, proj=project, limit=8) == []
    assert report["issues"][key]["count"] == 1, "The measured diagnostic must not be erased from the report"


@pytest.mark.parametrize("key", ["duplicate_pages_without_canonical", "canonical_points_to_4xx",
                                "canonical_points_to_redirect", "canonical_from_https_to_http"])
def test_real_canonical_defects_keep_their_claim_and_visibility(key):
    assert m._github_issue_auto_fixable(key) and key in m._handled_issue_keys()
    report = {"issues": {key: {"count": 1, "examples": [URL]}}, "pages": []}
    assert [row["key"] for row in dash.summarize_report(report)["issues"]] == [key]


def test_inventory_retires_a_hidden_claim_without_certifying_a_repair():
    from ops.gauntlet.acceptance_inventory import inventory

    result = inventory(m, {})
    assert KEY not in {row["family"] for row in result["families"]}
    assert result["claimed_family_count"] == result["visible_offered_family_count"]
    assert not result["client_readiness_certified"]
