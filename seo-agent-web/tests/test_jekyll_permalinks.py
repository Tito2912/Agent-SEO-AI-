"""Jekyll's per-page `permalink:` is invisible to the route map, on purpose.

Found by running the full loop on a real Jekyll build (tests/fixtures/jekyll). The loop passed —
3 anomalies to 0 — but the routes did not come from any Jekyll-specific understanding: they came
from the static-HTML fallback, which maps by FILENAME. That happens to be right when the
permalink matches the filename, which is the common case and the fixture's case.

It is wrong when they differ. A page `blog.html` declaring `permalink: /articles/actus` serves
`/articles/actus`, while the map claims `/blog`.

**This is a design boundary, not a bug.** `repo_index` is built from the file tree alone and
never opens a file, which is exactly why it costs zero extra API calls (see its module
docstring). Reading front matter would change that contract. These tests pin the boundary so it
stays a known limit rather than a surprise on a customer's repo, and they are the tests to change
first if the contract is ever revisited.
"""

from __future__ import annotations

import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import repo_index as ri  # noqa: E402

TREE = [
    "_config.yml",
    "_layouts/default.html",
    "index.html",
    "blog.html",
    "a-propos.html",
]


def test_a_jekyll_page_maps_by_filename() -> None:
    # The common case, and the one the fixture exercises: permalink matches the filename.
    routes = ri.build_repo_index(TREE)["routes"]
    assert routes["/blog"] == ["blog.html"]
    assert routes["/a-propos"] == ["a-propos.html"]
    assert routes["/"] == ["index.html"]


def test_the_layout_is_never_a_page() -> None:
    # `_layouts/` holds the shared template; a per-page value must never be written there.
    routes = ri.build_repo_index(TREE)["routes"]
    flat = [f for files in routes.values() for f in files]
    assert not any(f.startswith("_layouts/") for f in flat)


def test_a_custom_permalink_is_not_seen_and_the_real_url_is_unmapped() -> None:
    """The boundary, stated as an assertion rather than left to be discovered in production.

    If this ever starts passing differently, the file-tree-only contract has changed and that is
    a deliberate decision, not an accident.
    """
    index = ri.build_repo_index(TREE)
    # The site really serves /articles/actus; nothing in the tree can say so.
    assert index["routes"].get("/articles/actus") is None, (
        "the route map now resolves a custom permalink — the file-tree-only contract changed"
    )
    # And it still claims the filename-derived route, which that site does not serve.
    assert index["routes"].get("/blog") == ["blog.html"]


def test_the_phantom_route_cannot_misdirect_a_real_fix() -> None:
    """Why the boundary is tolerable: the phantom names a URL no crawl will ever report.

    The corrector only ever looks up URLs the CRAWL produced. A site serving /articles/actus
    never yields `/blog`, so the stale entry is never consulted; targeting simply falls through
    to its later steps for the URL it does have.
    """
    index = ri.build_repo_index(TREE)
    crawled_urls = ["https://exemple.fr/articles/actus"]
    resolved = [u for u in crawled_urls if ri.route_files(index, u)]
    assert resolved == [], (
        "a real crawled URL resolved through the filename map — verify it is the right file"
    )


def test_the_committed_fixture_is_the_case_that_works() -> None:
    """The fixture must stay in the safe case, or its measured result stops meaning anything."""
    import re

    root = WEB_ROOT / "tests" / "fixtures" / "jekyll"
    if not root.exists():  # pragma: no cover - the fixture is committed alongside this test
        return
    for name, expected in (("blog.html", "/blog"), ("a-propos.html", "/a-propos"), ("index.html", "/")):
        text = (root / name).read_text(encoding="utf-8")
        match = re.search(r"^permalink:\s*(\S+)\s*$", text, re.M)
        assert match, f"{name} lost its permalink"
        assert match.group(1) == expected, (
            f"{name} declares {match.group(1)} but is mapped as {expected} by filename — "
            "the fixture has moved into the case this module documents as unsupported"
        )
