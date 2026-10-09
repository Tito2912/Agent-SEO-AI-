"""An HTTP-served page with an HTTPS canonical is not an HTML canonical repair."""

import pytest

from backend import app as m, audit_dashboard as dash, fix_suggestions as fs

KEY = "canonical_from_http_to_https"
KEYS = (KEY, KEY + "_indexable", KEY + "_not_indexable", KEY.upper(), " " + KEY + " ")
HTTP, HTTPS = "http://site.test/page", "https://site.test/page"


@pytest.mark.parametrize("key", KEYS)
def test_hosting_problem_is_not_claimed_as_an_automatic_html_fix(key):
    assert not m._github_issue_auto_fixable(key)
    assert key not in m._handled_issue_keys()


@pytest.mark.parametrize("key", KEYS)
@pytest.mark.parametrize("paths", [[], ["index.html", "netlify.toml", "_redirects", "sitemap.xml"]])
def test_preparation_refuses_before_any_io_or_model(monkeypatch, key, paths):
    def forbidden(*args, **kwargs):
        pytest.fail("Hosting advice must not invoke GitHub or a provider")

    for name in ("_github_api_get", "_github_api_post", "_github_api_put", "_openai_generate_file_patch"):
        monkeypatch.setattr(m, name, forbidden)
    prep = m._prepare_issue_fix(issue_key=key, issues={key: {"count": 1, "examples": [HTTP]}},
        impacted=[HTTP], all_paths=paths, site_name="site.test", owner="fixture", repo_name="fixture",
        branch="main", token="unused", pages=[{"url": HTTP, "final_url": HTTP, "status_code": 200,
            "content_type": "text/html", "canonical": HTTPS}])
    assert prep["refusal"] and "HTTP" in prep["refusal"] and "canonical" in prep["refusal"]
    assert prep["link_rewriter"] is None and not prep["rewriter_ai_fallback"]


@pytest.mark.parametrize("key", KEYS)
def test_advice_preserves_the_canonical_and_requires_https_then_real_redirect_check(key):
    out = fs.suggest_issue_fix(issue_key=key, label=key, category="Canonicals", severity="warning", count=1,
        report={"issues": {key: {"count": 1, "examples": [HTTP]}}}, site_name="site.test", base_url=HTTP)
    assert out.get("auto_fixable") is False
    assert "hebergement" in out["auto_fix_note"]
    assert any("Conserver" in line and "canonical" in line for line in out["fix"])
    assert any("certificat" in line and "200" in line for line in out["fix"])
    assert any("301" in line and "308" in line for line in out["verify"])


def test_issue_remains_visible_and_real_bad_https_canonical_remains_repairable():
    assert KEY in dash.ISSUE_CATALOG and KEY not in dash.NON_ISSUE_KEYS
    assert m._github_issue_auto_fixable("canonical_from_https_to_http")


def test_bulk_candidates_do_not_include_the_hosting_advice():
    from types import SimpleNamespace

    report = {"issues": {KEY: {"count": 1, "examples": [HTTP]}}, "pages": []}
    proj = SimpleNamespace(site_name="site.test", slug="fixture", base_url=HTTP)
    assert m._github_fixable_issue_candidates(report=report, proj=proj, limit=8) == []
