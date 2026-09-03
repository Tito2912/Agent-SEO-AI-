"""/docs and /ressources-seo, end to end.

The routes are public and unauthenticated, which is exactly why they need a test: nobody on the
team visits them, so a 500 there can survive a long time.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-content-test-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")
os.environ.setdefault("APP_NAME", "Noyaru")

from fastapi.testclient import TestClient  # noqa: E402

from backend import content_library as cl  # noqa: E402
from backend.app import app  # noqa: E402


def test_docs_index_renders_every_section() -> None:
    with TestClient(app) as client:
        response = client.get("/docs")
        assert response.status_code == 200
        body = response.text
        for section in cl.docs_sections():
            assert section["name"] in body
        for page in cl.docs_pages():
            assert page["path"] in body


def test_every_doc_page_renders() -> None:
    with TestClient(app) as client:
        for page in cl.docs_pages():
            response = client.get(page["path"])
            assert response.status_code == 200, f"{page['path']} returned {response.status_code}"
            assert 'class="content-body"' in response.text


def test_every_blog_page_renders() -> None:
    with TestClient(app) as client:
        for page in cl.blog_pages():
            response = client.get(page["path"])
            assert response.status_code == 200, f"{page['path']} returned {response.status_code}"


def test_blog_index_renders() -> None:
    with TestClient(app) as client:
        response = client.get("/ressources-seo")
        assert response.status_code == 200
        for page in cl.blog_pages():
            assert page["path"] in response.text


def test_unknown_slug_is_a_404_not_a_500() -> None:
    with TestClient(app) as client:
        assert client.get("/docs/cette-page-nexiste-pas").status_code == 404
        assert client.get("/ressources-seo/cette-page-nexiste-pas").status_code == 404


def test_content_pages_are_public() -> None:
    """No redirect to /auth/login: these pages exist to be found by people without an account."""
    with TestClient(app) as client:
        for path in ("/docs", "/docs/demarrer", "/ressources-seo"):
            response = client.get(path, follow_redirects=False)
            assert response.status_code == 200, f"{path} is not publicly reachable"


def test_swagger_is_not_served_under_the_public_docs_prefix() -> None:
    """`/docs` is customer documentation. FastAPI's schema UI lives elsewhere, behind auth.

    The public-path allowlist matches the whole /docs prefix, so leaving Swagger on its default
    URL would have published every internal route to anyone who asked.
    """
    with TestClient(app) as client:
        assert "swagger" not in client.get("/docs").text.lower()
        explorer = client.get("/internal/api-explorer", follow_redirects=False)
        assert explorer.status_code == 303
        assert explorer.headers["location"].startswith("/auth/login")


def test_no_token_placeholder_leaks_into_a_rendered_page() -> None:
    """A visible `{{app_name}}` on a live page is the bug this whole token layer exists to avoid."""
    with TestClient(app) as client:
        for page in cl.all_pages():
            body = client.get(page["path"]).text
            assert "{{app_name}}" not in body, f"{page['path']} leaks an app_name token"
            assert "{{support_email}}" not in body
            assert "{{corrections_" not in body
            assert "{{price_" not in body


def test_plan_numbers_come_from_the_billing_catalogue() -> None:
    """The quota page must show the values the product actually enforces."""
    from backend import billing

    catalog = billing.plan_catalog()
    expected = str(catalog["business"]["limits"]["ai_corrections_month"])
    with TestClient(app) as client:
        body = client.get("/docs/plans-et-quotas").text
    assert expected in body


def test_head_requests_are_answered() -> None:
    with TestClient(app) as client:
        assert client.head("/docs").status_code == 200
        assert client.head("/docs/demarrer").status_code == 200
        assert client.head("/ressources-seo").status_code == 200


def test_canonical_and_json_ld_are_present() -> None:
    with TestClient(app) as client:
        body = client.get("/docs/corrections-automatiques").text
        assert '<link rel="canonical" href="http://testserver/docs/corrections-automatiques"' in body
        assert 'type="application/ld+json"' in body
        assert '"TechArticle"' in body
        assert '"FAQPage"' in body


def test_sitemap_lists_every_content_page() -> None:
    with TestClient(app) as client:
        body = client.get("/sitemap.xml").text
    assert "<loc>http://testserver/docs</loc>" in body
    for page in cl.all_pages():
        assert f"<loc>http://testserver{page['path']}</loc>" in body, f"{page['path']} missing from sitemap"


def test_sitemap_lastmod_follows_the_page_not_the_deploy() -> None:
    """A sitemap that stamps today on every page teaches crawlers to ignore lastmod."""
    page = cl.get_doc("demarrer")
    assert page is not None
    with TestClient(app) as client:
        body = client.get("/sitemap.xml").text
    assert f"<loc>http://testserver/docs/demarrer</loc><lastmod>{page['updated_at']}</lastmod>" in body


def test_robots_allows_both_hubs() -> None:
    with TestClient(app) as client:
        body = client.get("/robots.txt").text
    assert "Allow: /docs" in body
    assert "Allow: /ressources-seo" in body


def test_public_header_links_to_both_hubs() -> None:
    """The regression this replaces: both hubs existed and nothing in the chrome pointed at them."""
    with TestClient(app) as client:
        for path in ("/", "/pricing", "/support", "/status", "/terms", "/privacy"):
            body = client.get(path).text
            assert 'href="/docs"' in body, f"{path} does not link to /docs"
            assert 'href="/ressources-seo"' in body, f"{path} does not link to /ressources-seo"


def test_home_teaser_uses_featured_articles() -> None:
    with TestClient(app) as client:
        body = client.get("/").text
    for page in cl.featured_articles(3):
        assert page["path"] in body
