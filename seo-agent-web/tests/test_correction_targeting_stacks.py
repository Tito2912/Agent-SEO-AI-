"""Which file the corrector would actually patch, on each supported generator.

test_repo_index_stacks proves a URL maps to the right source file. That is only half the
promise: `_resolve_issue_targets` is what turns a flagged URL into the file a pull request
edits, and it has never run against Astro, Nuxt, Hugo, Jekyll or next-pages.

The failure that matters here is not "no fix" but "a fix in the wrong file". Editing
`_layouts/post.html` to give one article a title rewrites the title of every article on the
site — a silent, site-wide regression shipped as a helpful PR.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-stacks-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402
from backend import repo_index as ri  # noqa: E402
from tests.test_repo_index_stacks import ASTRO, HUGO, JEKYLL, NEXT_PAGES, NUXT  # noqa: E402

SITE = "https://exemple.fr"


def _resolve(issue_key: str, tree: list[str], urls: list[str], **kw):
    """Resolve targets with both AI fallbacks stubbed, recording whether they were reached."""
    calls: list[str] = []

    def _ai_map() -> list[str]:
        calls.append("map")
        return []

    def _ai_pick() -> list[str]:
        calls.append("pick")
        return []

    targets = app_module._resolve_issue_targets(
        all_paths=tree,
        index=ri.build_repo_index(tree),
        issue_key=issue_key,
        issue_label=issue_key,
        impacted_urls=[f"{SITE}{u}" for u in urls],
        located=kw.pop("located", []),
        max_files=kw.pop("max_files", 8),
        ai_map=_ai_map,
        ai_pick=_ai_pick,
        **kw,
    )
    return targets, calls


# The per-page source behind one flagged URL, per generator.
PER_PAGE_CASES = [
    pytest.param(ASTRO, "/blog/premier-article", "src/content/blog/premier-article.md",
                 "src/pages/blog/[slug].astro", id="astro-content-collection"),
    pytest.param(ASTRO, "/a-propos", "src/pages/a-propos.astro",
                 "src/layouts/Base.astro", id="astro-page"),
    pytest.param(NUXT, "/blog/premier-article", "content/blog/premier-article.md",
                 "pages/blog/[slug].vue", id="nuxt-content"),
    pytest.param(NUXT, "/a-propos", "pages/a-propos.vue",
                 "layouts/default.vue", id="nuxt-page"),
    pytest.param(HUGO, "/blog/premier-article", "content/blog/premier-article.md",
                 "layouts/_default/single.html", id="hugo-post"),
    pytest.param(HUGO, "/blog/page-bundle", "content/blog/page-bundle/index.md",
                 "layouts/_default/single.html", id="hugo-page-bundle"),
    pytest.param(JEKYLL, "/2026/08/26/premier-article", "_posts/2026-08-26-premier-article.md",
                 "_layouts/post.html", id="jekyll-post"),
    pytest.param(JEKYLL, "/a-propos", "a-propos.md",
                 "_layouts/default.html", id="jekyll-page"),
    pytest.param(NEXT_PAGES, "/blog/premier-article", "content/blog/premier-article.mdx",
                 "pages/blog/[slug].tsx", id="next-pages-mdx"),
]


@pytest.mark.parametrize("tree,url,expected,forbidden", PER_PAGE_CASES)
@pytest.mark.parametrize("issue_key", ["title_too_long", "open_graph_url_not_matching_canonical"])
def test_a_per_page_issue_patches_the_page_and_not_the_template(
    tree: list[str], url: str, expected: str, forbidden: str, issue_key: str
) -> None:
    targets, _ = _resolve(issue_key, tree, [url])
    assert targets and targets[0] == expected, f"{url} would be fixed in {targets or '(nothing)'}"
    assert forbidden not in targets, (
        f"a fix for {url} would edit {forbidden}, changing every page it renders"
    )


@pytest.mark.parametrize("tree,url,expected,forbidden", PER_PAGE_CASES)
def test_the_route_map_answers_without_asking_an_llm(
    tree: list[str], url: str, expected: str, forbidden: str
) -> None:
    # An AI fallback here is not just slower and billable: it is the step that has produced
    # every wrong-file patch this corrector has ever shipped.
    _, calls = _resolve("title_too_long", tree, [url])
    assert calls == [], f"{url} fell through to the AI fallback ({calls})"


@pytest.mark.parametrize(
    "tree,urls,expected",
    [
        (JEKYLL, ["/2026/08/26/premier-article", "/2026/08/20/second-article"],
         {"_posts/2026-08-26-premier-article.md", "_posts/2026-08-20-second-article.markdown"}),
        (ASTRO, ["/blog/premier-article", "/blog/second-article"],
         {"src/content/blog/premier-article.md", "src/content/blog/second-article.mdx"}),
    ],
)
def test_several_flagged_pages_each_get_their_own_file(
    tree: list[str], urls: list[str], expected: set[str]
) -> None:
    targets, _ = _resolve("title_too_long", tree, urls)
    assert expected.issubset(set(targets))


@pytest.mark.parametrize("tree", [ASTRO, NUXT, HUGO, JEKYLL, NEXT_PAGES])
def test_a_url_the_map_cannot_place_does_not_invent_a_target(tree: list[str]) -> None:
    # A URL from a custom permalink or a route we do not model must reach the AI fallback
    # rather than being silently attached to whatever file looked closest.
    targets, calls = _resolve("title_too_long", tree, ["/une-url-que-le-site-ne-declare-pas"])
    assert "map" in calls or targets == [], (
        f"an unmapped URL resolved to {targets} with no fallback"
    )
