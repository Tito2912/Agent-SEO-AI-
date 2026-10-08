"""Local JSON-LD checks are not an external validator or a numeric-price repair."""

from types import SimpleNamespace

import pytest

from backend import app as m, audit_dashboard as dash, fix_suggestions as fs
from tests.test_crawler_evidence_scoring import seo_audit as audit, _page

SCHEMA = "structured_data_schema_org_validation_error"
RICH = "structured_data_google_rich_results_validation_error"
KEYS = [key for family in (SCHEMA, RICH) for key in
        (family, family + "_indexable", family + "_not_indexable", family.upper(), " " + family + " ")]
URL = "https://site.test/page"


def forbidden_io(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Local structured-data advice must not target files, rewrite prices or invoke a provider")

    for name in ("_github_api_get", "_github_api_post", "_github_api_put", "_openai_generate_file_patch",
                 "_ai_pick_repo_files", "_ai_map_urls_to_files", "_rewrite_jsonld_numeric_strings"):
        monkeypatch.setattr(m, name, forbidden)


@pytest.mark.parametrize("key", KEYS)
def test_no_automatic_claim_for_an_unproved_structured_data_repair(key):
    assert not m._github_issue_auto_fixable(key)
    assert key.strip().lower() not in m._handled_issue_keys()


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("paths", [[], ["page.html", "app/layout.tsx", "lib/schema.ts"]])
def test_preparation_refuses_before_io_even_when_an_old_report_claims_an_error(monkeypatch, key, paths):
    forbidden_io(monkeypatch)
    prep = m._prepare_issue_fix(issue_key=key, issues={key: {"count": 1, "examples": [URL]}},
        impacted=[URL], all_paths=paths, site_name="site.test", owner="fixture", repo_name="fixture",
        branch="main", token="unused", pages=[{"url": URL, "schema_org_errors": ["faq_answer_missing"]}])
    assert prep["refusal"] and "automatique" in prep["refusal"]
    assert prep["link_rewriter"] is None and not prep["rewriter_ai_fallback"] and not prep["rewriter_is_ai"]


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("forged", [False, True])
def test_direct_patch_cannot_coerce_a_valid_price_from_a_cached_or_forged_plan(monkeypatch, key, forged):
    forbidden_io(monkeypatch)
    raw = '<script type="application/ld+json">{"@type":"Offer","price":"0","priceCurrency":"EUR"}</script>'
    cache = {"page.html": {"sha": "old", "content": raw}}
    options = {"link_rewriter": lambda text: (text.replace('"0"', '0'), 1), "targets_override": ["page.html"]} if forged else {}
    patched, skipped, targets, ai_files = m._deep_patch_issue_files(owner="fixture", repo_name="fixture",
        branch="main", token="unused", fix_branch="qa", all_paths=["page.html"], issue_key=key,
        issue_label=key, impacted_urls=[URL], site_name="site.test", file_state=cache, **options)
    assert not patched and skipped and not targets and not ai_files
    assert cache == {"page.html": {"sha": "old", "content": raw}}


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("prep", [{}, {"refusal": None, "loop_paths": ["netlify.toml"]}])
def test_internal_execution_also_refuses_forged_or_incomplete_preparation(monkeypatch, key, prep):
    forbidden_io(monkeypatch)
    monkeypatch.setattr(m, "_deep_patch_issue_files", lambda **kw: pytest.fail("Executor must refuse first"))
    out = m._apply_prepared_issue_fix(owner="fixture", repo_name="fixture", branch="main", token="unused",
        fix_branch="qa", all_paths=["page.html"], issue_key=key, issue_label=key, impacted=[URL],
        site_name="site.test", file_state={}, max_files=8, prep=prep, pages=[], index=None)
    assert out["error"] and "automatique" in out["error"]
    assert all(out[field] == [] for field in ("patched", "skipped", "targets", "ai_files", "config_changes", "config_notes"))


@pytest.mark.parametrize("key", KEYS)
def test_manual_advice_requires_real_existing_data_not_numeric_coercion(key):
    out = fs.suggest_issue_fix(issue_key=key, label=key, category="Other", severity="notice", count=1,
        report={"issues": {key: {"count": 1, "examples": [URL]}}}, site_name="site.test", base_url=URL)
    assert out.get("auto_fixable") is False and out.get("mode") == "suggest-only"
    assert "local" in out["why"].lower() and "price" in out["auto_fix_note"]
    assert not any("éligible" in line or "eligible" in line for line in out["fix"])


@pytest.mark.parametrize("key", [SCHEMA, RICH])
def test_diagnostic_remains_visible_without_an_automatic_bulk_offer(key):
    report = {"issues": {key: {"count": 1, "examples": [URL]}}, "pages": []}
    rows = dash.summarize_report(report)["issues"]
    assert [row["key"] for row in rows] == [key] and rows[0]["count"] == 1
    assert "Google" not in rows[0]["label"] and "local" in rows[0]["label"].lower()
    project = SimpleNamespace(site_name="site.test", slug="fixture", base_url=URL)
    assert m._github_fixable_issue_candidates(report=report, proj=project, limit=8) == []


@pytest.mark.parametrize("code", ["faq_mainEntity_missing", "faq_question_invalid", "faq_question_name_missing", "faq_answer_missing"])
def test_faq_sub_errors_stay_tracked_as_a_local_check_not_a_google_verdict(code):
    page = _page(URL, schema_org_errors=[code])
    report = audit._score_issues([page], base_url="https://site.test")
    block = report[RICH]
    assert block["count"] == 1 and block["examples"] == [URL]
    assert block["validation_source"] == "local_faq_markup_check"
    assert block["external_google_validation"] is False and block["faq_google_rich_results_supported"] is False
    assert page.schema_org_errors == [code]
    assert report[SCHEMA]["count"] == 0


@pytest.mark.parametrize("code", ["invalid_json", "missing_type"])
def test_actual_parse_or_type_errors_are_kept_without_claiming_an_external_validator(code):
    report = audit._score_issues([_page(URL, schema_org_errors=[code])], base_url="https://site.test")
    assert report[SCHEMA]["count"] == 1
    assert report[SCHEMA]["validation_source"] == "local_jsonld_parse_and_type_check"
    assert report[SCHEMA]["external_schema_org_validation"] is False
    assert report[RICH]["count"] == 0


@pytest.mark.parametrize("price", ["0", "29.90", "119.99"])
@pytest.mark.parametrize("kind", ["Product", "SoftwareApplication"])
def test_valid_string_prices_are_not_the_error_the_corrector_must_repair(price, kind):
    import json

    payload = json.dumps({"@context": "https://schema.org", "@type": kind, "name": "Control",
                          "offers": {"@type": "Offer", "price": price, "priceCurrency": "EUR"}})
    errors = audit._schema_org_validation_errors([payload], page_url=URL)
    assert errors == []
    issues = audit._score_issues([_page(URL, schema_org_errors=errors)], base_url="https://site.test")
    assert issues[SCHEMA]["count"] == issues[RICH]["count"] == 0


def test_other_genuinely_protected_families_remain_offered():
    for key in ("viewport_not_set", "duplicate_pages_without_canonical", "meta_description_too_short"):
        assert m._github_issue_auto_fixable(key) and key in m._handled_issue_keys()
