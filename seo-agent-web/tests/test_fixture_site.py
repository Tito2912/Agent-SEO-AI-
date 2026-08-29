"""The defect fixture must still carry its defects.

`fixture_site.py` is the tool that reproduced the worst bug this project shipped — `_score_issues`
reading `PageData.h1` as a string when it is a list, which killed the ENTIRE crawl of any site
with a 2+ h1 page. It only works if each route still carries exactly the flaw it is supposed to
carry, and a fixture nobody exercises rots silently: someone tidies a template, the site becomes
clean, and every crawl against it passes while proving nothing.

A full crawl cannot run here — the crawler has no browserless path and CI installs no Chromium
(see the module docstring for the manual command). What CI can do is serve the site and check the
defects at the HTTP and HTML level, in milliseconds. That is not a crawl, and it is not claimed
to be: it guards the premises the crawl depends on.
"""

from __future__ import annotations

import re
import sys
import threading
from pathlib import Path

import pytest
import requests

TESTS_DIR = Path(__file__).resolve().parent
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

import fixture_site  # noqa: E402


@pytest.fixture(scope="module")
def site():
    server = fixture_site.make_server(0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _get(base: str, path: str, **kw) -> requests.Response:
    return requests.get(base + path, timeout=10, **kw)


def _head_of(html: str) -> str:
    return html.split("</head>", 1)[0]


def test_the_site_answers_at_all(site: str) -> None:
    # If this fails, every other assertion below would fail for the wrong reason.
    assert _get(site, "/").status_code == 200
    assert _get(site, "/robots.txt").status_code == 200
    assert "sitemap.xml" in _get(site, "/robots.txt").text


def test_the_sitemap_lists_every_page(site: str) -> None:
    locs = re.findall(r"<loc>([^<]+)</loc>", _get(site, "/sitemap.xml").text)
    assert sorted(locs) == sorted(site + p for p in fixture_site.SITEMAP_PATHS)


@pytest.mark.parametrize("path", sorted(fixture_site.REDIRECTS))
def test_every_declared_redirect_still_redirects(site: str, path: str) -> None:
    """canonical_points_to_redirect, hreflang_to_redirect and image_redirects all rest on these.

    A server that started answering 200 here would make three families silently untestable.
    """
    response = _get(site, path, allow_redirects=False)
    assert response.status_code == 301, f"{path} no longer redirects"
    assert response.headers["Location"] == site + fixture_site.REDIRECTS[path]
    # And the destination must be a real 200, or the crawler is right to emit no evidence.
    assert _get(site, fixture_site.REDIRECTS[path]).status_code == 200


def test_the_home_canonical_points_at_a_redirecting_url(site: str) -> None:
    head = _head_of(_get(site, "/").text)
    canonical = re.search(r'<link rel="canonical" href="([^"]+)"', head)
    assert canonical, "the home page lost its canonical"
    assert canonical.group(1) == site + "/home-old"
    assert _get(site, "/home-old", allow_redirects=False).status_code == 301


def test_the_home_page_still_has_two_h1(site: str) -> None:
    """The one that mattered: a 2+ h1 page is what made the scorer raise and kill the crawl."""
    assert len(re.findall(r"<h1>", _get(site, "/").text)) == 2


def test_the_home_lang_is_still_invalid(site: str) -> None:
    assert 'lang="en_US"' in _get(site, "/").text, "html_lang_attribute_invalid lost its subject"


def test_the_open_graph_block_is_still_incomplete(site: str) -> None:
    # The point of the family: og:title IS present, so a fixer must add what is missing without
    # regenerating (and clobbering) the tag that is already right.
    head = _head_of(_get(site, "/").text)
    assert 'property="og:title"' in head
    assert 'property="og:description"' not in head


def test_the_french_page_points_an_hreflang_at_a_redirect(site: str) -> None:
    head = _head_of(_get(site, "/fr").text)
    alt = re.search(r'<link rel="alternate" hreflang="en" href="([^"]+)"', head)
    assert alt and alt.group(1) == site + "/fr-old"
    assert _get(site, "/fr-old", allow_redirects=False).status_code == 301


def test_the_spanish_page_points_an_hreflang_at_a_non_canonical_page(site: str) -> None:
    head = _head_of(_get(site, "/es-source").text)
    alt = re.search(r'<link rel="alternate" hreflang="es" href="([^"]+)"', head)
    assert alt and alt.group(1) == site + "/es-dup"

    # /es-dup answers 200 but declares a different canonical — that gap IS the defect.
    dup = _get(site, "/es-dup")
    assert dup.status_code == 200
    dup_canonical = re.search(r'<link rel="canonical" href="([^"]+)"', _head_of(dup.text))
    assert dup_canonical and dup_canonical.group(1) == site + "/es"


def test_the_home_page_loads_a_redirecting_image(site: str) -> None:
    assert f'<img src="{site}/img/logo.png"' in _get(site, "/").text
    assert _get(site, "/img/logo.png", allow_redirects=False).status_code == 301
    final = _get(site, "/img/logo-v2.png")
    assert final.status_code == 200 and final.headers["Content-Type"] == "image/png"


def test_the_pages_the_defects_point_at_are_themselves_clean(site: str) -> None:
    """Redirect destinations must be healthy.

    A canonical pointing at a redirect that ends in a 404 yields NO evidence by design, so a
    broken destination would quietly disable the very families this site exists to exercise.
    """
    for path in ("/home", "/fr", "/es"):
        head = _head_of(_get(site, path).text)
        assert "<title>" in head, f"{path} lost its title"
        assert 'name="description"' in head, f"{path} lost its description"


def test_every_route_the_module_advertises_is_reachable(site: str) -> None:
    # DEFECTS is documentation; documentation that names a dead route is worse than none.
    for path in fixture_site.DEFECTS:
        response = _get(site, path, allow_redirects=False)
        assert response.status_code in (200, 301), f"{path} is advertised but answers {response.status_code}"
