# -*- coding: utf-8 -*-
"""Un dossier de langue n'est un segment d'URL que si une route le sert.

MESURE DU 27/09/2026 SUR UN SITE CLIENT EN PRODUCTION. Le depot porte quatre dossiers de
contenu — `content/de`, `content/en`, `content/es`, `content/fr` — mais son arborescence de
routage n'en connait que trois :

    app/(site)/[...slug]/page.tsx        <- aucun prefixe : la langue par defaut
    app/(site)/de/[...slug]/page.tsx     <- /de
    app/(site)/es/[...slug]/page.tsx     <- /es
    app/(site)/fr/[...slug]/page.tsx     <- /fr

L'anglais est servi A LA RACINE. Verifie sur le site en ligne : `/guides/crypto-dca/` rend 200,
`/en/guides/crypto-dca/` rend 404. On en deduisait pourtant `/en/guides/crypto-dca`, parce que
l'attrape-tout de racine accepte tout ce qui n'a pas trouve mieux.

DEUX CONSEQUENCES, LES DEUX SILENCIEUSES :
  * correcteur — les 23 pages anglaises n'etaient rattachables a aucune URL du crawl, donc
    INCORRIGIBLES, sans que rien ne le dise ;
  * redacteur — une page anglaise aurait porte un canonical vers une 404.

MEME GRAMMAIRE QUE POUR LES COLLECTIONS ASTRO, et meme cause : un segment de route qui vient du
NOM D'UN DOSSIER et que rien ne confirme.
"""
from __future__ import annotations

import pytest

from backend import repo_index


def routes(paths: list[str]) -> dict[str, list[str]]:
    return repo_index.build_repo_index(paths).get("routes") or {}


def route_de(paths: list[str], fichier: str) -> str:
    for r, f in routes(paths).items():
        if fichier in (f or []):
            return r
    return ""


# La forme reelle du depot client, reduite a ce qui compte.
CLIENT = [
    "package.json", "next.config.js",
    "app/layout.tsx",
    "app/(site)/[...slug]/page.tsx",
    "app/(site)/de/[...slug]/page.tsx",
    "app/(site)/es/[...slug]/page.tsx",
    "app/(site)/fr/[...slug]/page.tsx",
    "content/en/guides/crypto-dca.mdx",
    "content/en/guides/how-to-read-a-chart.mdx",
    "content/fr/guides/dca-crypto.mdx",
    "content/de/guides/krypto-dca.mdx",
    "content/es/guides/dca-cripto.mdx",
]


def test_la_langue_SANS_route_dediee_sort_a_la_racine() -> None:
    assert route_de(CLIENT, "content/en/guides/crypto-dca.mdx") == "/guides/crypto-dca"


@pytest.mark.parametrize("langue,fichier,attendue", [
    ("fr", "content/fr/guides/dca-crypto.mdx", "/fr/guides/dca-crypto"),
    ("de", "content/de/guides/krypto-dca.mdx", "/de/guides/krypto-dca"),
    ("es", "content/es/guides/dca-cripto.mdx", "/es/guides/dca-cripto"),
])
def test_les_langues_QUI_ONT_une_route_gardent_leur_prefixe(
        langue: str, fichier: str, attendue: str) -> None:
    """Le temoin de l'autre bord : sans lui, « retirer tous les prefixes » passerait le test
    precedent et casserait trois langues sur quatre."""
    assert route_de(CLIENT, fichier) == attendue


def test_aucune_route_en_ne_subsiste() -> None:
    assert [r for r in routes(CLIENT) if r.startswith("/en/")] == []


def test_un_dossier_qui_n_est_PAS_une_langue_garde_son_segment() -> None:
    """`content/blog/article.md` chez Nuxt : `blog` EST un segment d'URL. Le retirer casserait
    un mappage juste — c'est le risque exact que la forme du code de langue borne."""
    paths = ["package.json", "nuxt.config.ts", "pages/[...slug].vue",
             "content/blog/premier.md", "content/blog/second.md"]
    assert route_de(paths, "content/blog/premier.md") == "/blog/premier"


def test_un_code_REGIONAL_est_reconnu_comme_une_langue() -> None:
    """`pt-br`, `zh-cn` : sans eux, le dossier resterait dans l'URL et la page serait
    introuvable. Une mutation reduisant le motif a deux lettres a survecu faute de ce cas."""
    paths = ["package.json", "next.config.js", "app/[...slug]/page.tsx",
             "content/pt-br/guias/dca.mdx", "content/pt-br/guias/graficos.mdx"]
    assert route_de(paths, "content/pt-br/guias/dca.mdx") == "/guias/dca"


def test_un_site_monolingue_a_dossier_de_langue_unique() -> None:
    """Une seule langue, son dossier, et aucune route dediee : tout sort a la racine."""
    paths = ["package.json", "next.config.js", "app/[...slug]/page.tsx",
             "content/en/about.mdx", "content/en/contact.mdx"]
    assert route_de(paths, "content/en/about.mdx") == "/about"


# creativeai-tools, 19/10/2026 : UN SEUL attrape-tout de racine sert toutes les langues AVEC leur
# prefixe, et la langue par defaut vit hors de tout dossier. On retirait `de`/`es`/`fr` : les 63
# pages traduites tombaient sur les routes anglaises (`/blog-akool` -> 4 fichiers), incorrigibles,
# et offertes aux corrections de la page anglaise.
UN_SEUL_ATTRAPE_TOUT = [
    "package.json", "next.config.mjs", "app/layout.tsx", "app/page.tsx", "app/[...slug]/page.tsx",
    "content/index.mdx", "content/blog-akool/index.mdx",
    "content/de/index.mdx", "content/de/blog-akool/index.mdx", "content/de/nur-auf-deutsch/index.mdx",
    "content/es/blog-akool/index.mdx",
    "content/it/solo/index.mdx",
]


@pytest.mark.parametrize("fichier,attendue", [
    ("content/de/blog-akool/index.mdx", "/de/blog-akool"),
    ("content/es/blog-akool/index.mdx", "/es/blog-akool"),
    ("content/de/index.mdx", "/de"),
    # Aucune page anglaise en face, mais la LANGUE est prouvee par une autre de ses pages.
    ("content/de/nur-auf-deutsch/index.mdx", "/de/nur-auf-deutsch"),
    ("content/blog-akool/index.mdx", "/blog-akool"),
])
def test_une_collision_prouve_que_la_langue_garde_son_prefixe(fichier: str, attendue: str) -> None:
    assert route_de(UN_SEUL_ATTRAPE_TOUT, fichier) == attendue


def test_la_page_anglaise_ne_porte_plus_ses_traductions() -> None:
    index = repo_index.build_repo_index(UN_SEUL_ATTRAPE_TOUT)
    assert repo_index.route_files(index, "https://site.test/blog-akool/") == ["content/blog-akool/index.mdx"]
    assert repo_index.route_files(index, "https://site.test/") == ["app/page.tsx", "content/index.mdx"]


def test_une_langue_sans_collision_garde_l_ancienne_regle() -> None:
    """`it` n'a aucune page qui heurte la racine : rien ne prouve son prefixe, il sort a la racine."""
    assert route_de(UN_SEUL_ATTRAPE_TOUT, "content/it/solo/index.mdx") == "/solo"


def test_sans_attrape_tout_de_racine_rien_ne_change() -> None:
    """La correction ne s'applique QUE sur la branche de l'attrape-tout. Quand chaque langue a
    sa route, la question ne se pose pas — et on ne doit rien toucher."""
    paths = ["package.json", "next.config.js",
             "app/fr/[...slug]/page.tsx", "app/en/[...slug]/page.tsx",
             "content/fr/guides/a.mdx", "content/en/guides/a.mdx"]
    assert route_de(paths, "content/en/guides/a.mdx") == "/en/guides/a"
    assert route_de(paths, "content/fr/guides/a.mdx") == "/fr/guides/a"
