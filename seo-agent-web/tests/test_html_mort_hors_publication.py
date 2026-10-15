# -*- coding: utf-8 -*-
"""Un .html que le framework ne publie pas n'est pas une page, et ne se corrige pas.

PR #1 de cityscootlab, 15/10/2026. Le site est en Next (export statique, Netlify publie `out/`),
mais le depot garde a la racine les .html de son ancienne version : `de/index.html`,
`fr/blog.html`… La carte des routes les prenait pour des pages — « Static HTML is additive on
every stack » —, et le correcteur en a modifie sept sur 32 fichiers, a cote des vraies sources
.mdx. Rien ne les sert : aucun build de framework ne recopie un .html hors de son dossier public.

Les generateurs qui publient leurs .html ou qu'ils soient (HTML statique, Jekyll) ne changent pas,
et les gabarits ou coquilles d'un framework (Hugo `layouts/`, SvelteKit `src/app.html`, Nuxt 2
`app.html`) ne sont ni des pages ni des fichiers morts.
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

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-html-mort-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402
from backend import repo_index as ri  # noqa: E402

B = "https://site.test"

# La forme du depot de cityscootlab.
NEXT = [
    "package.json", "next.config.mjs", "app/(fr)/layout.tsx", "app/(fr)/fr/page.tsx",
    "app/(fr)/fr/[...slug]/page.tsx", "content/fr/index.mdx", "content/fr/blog.mdx",
    "fr/index.html", "fr/blog.html", "public/merci.html",
]


def _index(chemins: list[str]) -> dict:
    return ri.build_repo_index(chemins)


def test_next_les_html_de_l_ancienne_version_sont_morts() -> None:
    index = _index(NEXT)

    assert index["stack"] == ri.STACK_NEXT_APP
    assert ri.route_files(index, f"{B}/fr/blog") == ["content/fr/blog.mdx"]
    assert "fr/blog.html" not in ri.route_files(index, f"{B}/fr")
    assert ri.is_dead_path(index, "fr/blog.html") and ri.is_dead_path(index, "fr/index.html")


def test_next_un_html_de_public_reste_une_page() -> None:
    index = _index(NEXT)

    assert ri.route_files(index, f"{B}/merci") == ["public/merci.html"]
    assert not ri.is_dead_path(index, "public/merci.html")


def test_le_html_statique_publie_tout() -> None:
    index = _index(["index.html", "fr/blog.html", "fr/index.html"])

    assert index["stack"] == ri.STACK_STATIC
    assert ri.route_files(index, f"{B}/fr/blog") == ["fr/blog.html"]
    assert index["morts"] == []


def test_jekyll_publie_ses_html_de_racine() -> None:
    index = _index(["_config.yml", "_layouts/default.html", "a-propos.html"])

    assert index["stack"] == ri.STACK_JEKYLL
    assert ri.route_files(index, f"{B}/a-propos") == ["a-propos.html"]
    assert not ri.is_dead_path(index, "_layouts/default.html")


def test_hugo_static_publie_layouts_est_un_gabarit_la_racine_est_morte() -> None:
    index = _index(["hugo.toml", "layouts/_default/single.html", "static/landing.html", "vieux.html"])

    assert index["stack"] == ri.STACK_HUGO
    assert ri.route_files(index, f"{B}/landing") == ["static/landing.html"]
    assert ri.is_dead_path(index, "vieux.html")
    assert not ri.is_dead_path(index, "layouts/_default/single.html")


def test_nuxt_app_html_est_la_coquille_pas_un_fichier_mort() -> None:
    index = _index(["nuxt.config.ts", "app.html", "static/ancien.html", "public/neuf.html"])

    assert index["stack"] == ri.STACK_NUXT
    assert not ri.is_dead_path(index, "app.html")
    assert ri.route_files(index, f"{B}/ancien") == ["static/ancien.html"]
    assert ri.route_files(index, f"{B}/neuf") == ["public/neuf.html"]


def test_sveltekit_la_coquille_n_est_pas_morte() -> None:
    index = _index(["svelte.config.js", "src/app.html", "src/routes/+page.svelte", "static/a.html"])

    assert not ri.is_dead_path(index, "src/app.html")
    assert ri.route_files(index, f"{B}/a") == ["static/a.html"]


@pytest.mark.parametrize(("marqueurs", "stack", "publie"), [
    (["astro.config.mjs"], ri.STACK_ASTRO, "public/"),
    (["gatsby-config.js"], ri.STACK_GATSBY, "static/"),
    (["remix.config.js"], ri.STACK_REMIX, "public/"),
    (["docusaurus.config.js"], ri.STACK_DOCUSAURUS, "static/"),
    (["svelte.config.js", "src/app.html"], ri.STACK_SVELTEKIT, "static/"),
    (["package.json", "pages/index.tsx", "next.config.js"], ri.STACK_NEXT_PAGES, "public/"),
])
def test_chaque_framework_publie_son_seul_dossier(marqueurs: list[str], stack: str, publie: str) -> None:
    index = _index(marqueurs + [f"{publie}garde.html", "vieux.html"])

    assert index["stack"] == stack
    assert ri.route_files(index, f"{B}/garde") == [f"{publie}garde.html"]
    assert ri.is_dead_path(index, "vieux.html")


def test_un_site_vite_pris_pour_next_garde_ses_pages() -> None:
    """`pages/` + `package.json` suffit a la detection de Next (pages) : c'est aussi la forme
    d'un Vite multi-pages, dont `index.html` a la racine EST publie. Sans fichier propre a
    Next, rien n'est declare mort."""
    index = _index(["package.json", "vite.config.js", "pages/contact.html", "index.html"])

    assert index["stack"] == ri.STACK_NEXT_PAGES
    assert index["morts"] == []
    assert ri.route_files(index, f"{B}/") == ["index.html"]


@pytest.mark.parametrize("marqueur", ["pages/_app.tsx", "pages/_document.jsx", "src/app/layout.tsx",
                                      "next.config.ts"])
def test_un_fichier_propre_a_next_suffit(marqueur: str) -> None:
    index = _index(["package.json", "pages/index.tsx", marqueur, "vieux.html"])

    assert ri.is_dead_path(index, "vieux.html")


def test_un_fichier_mort_trouve_par_la_preuve_n_est_ni_cible_ni_annonce() -> None:
    """Meme quand la carte ne resout pas toutes les pages, un fichier que rien ne publie ne
    corrige rien : il sort de la liste avant le plafond, donc des « Non traites » aussi."""
    ecartes: list[str] = []
    cibles = m._resolve_issue_targets(
        all_paths=NEXT, index=_index(NEXT), issue_key="page_has_links_to_redirect_indexable",
        issue_label="L", impacted_urls=[f"{B}/fr/blog", f"{B}/fr/inconnue"],
        located=["fr/index.html", "fr/blog.html", "content/fr/blog.mdx"], max_files=1,
        evidence=[f"{B}/fr/"], wants_page_targeting=True, allow_ai=False, ecartes=ecartes)

    assert cibles == ["content/fr/blog.mdx"]
    assert not any(p.endswith(".html") for p in ecartes)
