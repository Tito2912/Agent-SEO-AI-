"""The deterministic rewriters against the file formats each generator actually uses.

Targeting the right file is half the promise; the other half is that the patch written into it
is correct. These rewriters were built and validated on JSX and hand-written HTML. A Jekyll
post is Markdown under YAML front-matter, a Hugo post can use TOML, an `.astro` file opens with
a block of JavaScript, and a `.vue` file is three blocks in one file — all places where a
regex that assumes HTML can do damage that no test would have caught.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-rewrite-stacks-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

HOST = "exemple.fr"

JEKYLL_POST = """---
layout: post
title: "Premier article"
canonical_url: http://exemple.fr/2026/08/26/premier-article
image: http://exemple.fr/assets/couverture.png
---

Un paragraphe qui renvoie vers [notre page a propos](/a-propos/) et vers
[un partenaire](http://partenaire-externe.com/page).

    # bloc de code indente
    curl http://exemple.fr/api/ping
"""

HUGO_POST = """+++
title = "Premier article"
canonical = "http://exemple.fr/blog/premier-article"
[params]
  cover = "http://exemple.fr/img/cover.png"
+++

Texte avec un lien [interne](http://exemple.fr/a-propos) et un [externe](http://autre-site.org/x).
"""

ASTRO_PAGE = """---
import Base from '../layouts/Base.astro';
const canonical = 'http://exemple.fr/a-propos';
const partners = ['http://partenaire-externe.com/'];
---

<Base title="A propos">
  <a href="http://exemple.fr/contact">Contact</a>
  <img src="http://exemple.fr//img//logo.png" alt="Logo" />
</Base>
"""

VUE_PAGE = """<template>
  <div>
    <a href="http://exemple.fr/contact">Contact</a>
    <img src="http://exemple.fr//img//logo.png" alt="Logo" />
  </div>
</template>

<script setup>
const canonical = 'http://exemple.fr/a-propos'
</script>

<style scoped>
.hero { background: url('http://exemple.fr/img/hero.jpg'); }
</style>
"""


@pytest.mark.parametrize(
    "name,content",
    [
        ("jekyll-yaml-frontmatter", JEKYLL_POST),
        ("hugo-toml-frontmatter", HUGO_POST),
        ("astro-js-frontmatter", ASTRO_PAGE),
        ("vue-sfc", VUE_PAGE),
    ],
)
def test_the_site_s_own_http_urls_are_upgraded_everywhere_in_the_file(name: str, content: str) -> None:
    # Front matter is where canonical and og:image live, so a rewriter that only looked at
    # markup would silently leave the worst mixed-content offenders in place.
    new, count = app_module._rewrite_http_to_https(content, [HOST])
    assert count > 0
    assert f"http://{HOST}" not in new, f"{name}: an http URL for the site's own host survived"


@pytest.mark.parametrize(
    "name,content,external",
    [
        ("jekyll-yaml-frontmatter", JEKYLL_POST, "http://partenaire-externe.com/page"),
        ("hugo-toml-frontmatter", HUGO_POST, "http://autre-site.org/x"),
        ("astro-js-frontmatter", ASTRO_PAGE, "http://partenaire-externe.com/"),
    ],
)
def test_third_party_http_urls_are_left_alone(name: str, content: str, external: str) -> None:
    # We cannot know whether someone else's host serves https; upgrading it would break the link.
    new, _ = app_module._rewrite_http_to_https(content, [HOST])
    assert external in new, f"{name}: rewrote a third-party URL we have no right to change"


@pytest.mark.parametrize("name,content", [("astro", ASTRO_PAGE), ("vue", VUE_PAGE)])
def test_double_slashes_are_collapsed_without_breaking_the_scheme(name: str, content: str) -> None:
    new, count = app_module._rewrite_double_slash(content)
    assert count > 0
    assert "//img//logo.png" not in new
    assert "http://" in new or "https://" in new, f"{name}: the scheme separator was collapsed"


def test_a_redirecting_link_is_rewritten_inside_markdown_body_text() -> None:
    new, count = app_module._rewrite_redirect_links(
        JEKYLL_POST, [{"from": "/a-propos/", "to": "/a-propos"}]
    )
    assert count == 1
    assert "(/a-propos)" in new


def test_a_redirect_pair_never_rewrites_a_longer_link_that_merely_starts_the_same() -> None:
    # The bug this rewriter exists to prevent: a redirecting `/blog/` eating `/blog/article`.
    content = "[index](/blog/) et [article](/blog/premier-article)"
    new, count = app_module._rewrite_redirect_links(content, [{"from": "/blog/", "to": "/blog"}])
    assert count == 1
    assert "/blog/premier-article" in new


@pytest.mark.parametrize(
    "name,content",
    [
        ("jekyll", JEKYLL_POST),
        ("hugo", HUGO_POST),
        ("astro", ASTRO_PAGE),
        ("vue", VUE_PAGE),
    ],
)
def test_a_rewriter_with_nothing_to_do_returns_the_file_untouched(name: str, content: str) -> None:
    # A no-op that still reformats the file would produce a PR full of noise, and on these
    # formats a stray change in front matter can stop the page building at all.
    upgraded, _ = app_module._rewrite_http_to_https(content, [HOST])
    again, count = app_module._rewrite_http_to_https(upgraded, [HOST])
    assert count == 0 and again == upgraded, f"{name}: rewriting twice was not idempotent"

    same, n = app_module._rewrite_redirect_links(content, [{"from": "/inexistant", "to": "/autre"}])
    assert n == 0 and same == content


def test_http_urls_inside_markdown_code_samples_are_rewritten_too() -> None:
    """Documented, because it is a decision and not an oversight.

    JEKYLL_POST contains an indented code block with `curl http://exemple.fr/api/ping`, and
    the https upgrade reaches it. The rewriter has no Markdown parser and does not know a code
    fence from a paragraph. That is acceptable *only* because this rewriter is restricted to
    the site's own host, which is guaranteed to serve https: the sample keeps working and the
    displayed text becomes correct rather than wrong.

    It would NOT be acceptable for a rewriter that touched third-party URLs or arbitrary text,
    and it is the reason _rewrite_http_to_https takes an explicit host list instead of matching
    every `http://`.
    """
    new, _ = app_module._rewrite_http_to_https(JEKYLL_POST, [HOST])
    assert "curl https://exemple.fr/api/ping" in new
    # The same block's third-party URL, had there been one, would have been left alone — see
    # test_third_party_http_urls_are_left_alone.
