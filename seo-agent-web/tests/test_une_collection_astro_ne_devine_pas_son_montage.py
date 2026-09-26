# -*- coding: utf-8 -*-
"""Une route fausse est pire qu'une route absente.

MESURE DU 26/09/2026, premier essai hors des neuf fixtures. Fork de `withastro/docs`, 3004
fichiers, une collection de contenu a schema et huit langues — tout ce que les fixtures, que
nous engendrons nous-memes, n'ont pas.

L'index en deduisait 2612 routes `/docs/fr/guides/...` pour un site qui sert `/fr/guides/...`.
Le segment `docs` est le nom de la COLLECTION Astro, pas un segment d'URL : c'est Starlight
qui la monte, a la racine.

Consequences, differentes selon le chemin et toutes deux silencieuses :
  * correcteur — aucune URL du crawl ne retrouve son fichier, 2612 pages deviennent
    incorrigibles sans que rien ne le dise ;
  * redacteur — le canonical porte un segment qui n'existe pas, et le build reste VERT.

LE GARDE-FOU EXISTAIT ET NE GARDAIT RIEN. La regle disait « seulement si une route dynamique
couvre le prefixe ». `src/pages/[...enRedirectSlug].astro` — un attrape-tout de racine pose
pour les redirections — donne le prefixe `/`, qui couvre tout. Une condition que n'importe quoi
satisfait n'est pas une condition.
"""
from __future__ import annotations

import pytest

from backend import repo_index


def routes(paths: list[str]) -> list[str]:
    idx = repo_index.build_repo_index(paths)
    return sorted(r for r in (idx.get("routes") or {}) if r != "/")


COMMUN_ASTRO = ["astro.config.mjs", "package.json", "src/content.config.ts"]
COLLECTION = ["src/content/docs/fr/guides/authentification.mdx",
              "src/content/docs/fr/guides/deploiement.mdx",
              "src/content/docs/fr/guides/images.mdx"]


def test_un_attrape_tout_de_racine_ne_suffit_pas_a_monter_une_collection() -> None:
    """Le cas reel. `[...slug].astro` a la racine ne dit RIEN du point de montage."""
    assert routes(COMMUN_ASTRO + COLLECTION + ["src/pages/[...slug].astro"]) == []


def test_une_route_dynamique_qui_NOMME_la_collection_la_monte() -> None:
    """`src/pages/docs/[...slug].astro` dit ou la collection sort : la route est sure."""
    obtenu = routes(COMMUN_ASTRO + COLLECTION + ["src/pages/docs/[...slug].astro"])
    assert obtenu == ["/docs/fr/guides/authentification",
                      "/docs/fr/guides/deploiement",
                      "/docs/fr/guides/images"], obtenu


def test_sans_aucune_route_dynamique_rien_n_est_monte() -> None:
    """Une collection sans route qui la sert n'a pas d'URL. Le temoin de l'autre bord."""
    assert routes(COMMUN_ASTRO + COLLECTION) == []


def test_les_pages_classiques_d_astro_ne_sont_pas_touchees() -> None:
    """Le garde-fou ne vise QUE les collections : `src/pages/` garde son mappage direct."""
    obtenu = routes(COMMUN_ASTRO + ["src/pages/blog/premier.astro",
                                    "src/pages/blog/second.astro",
                                    "src/pages/[...slug].astro"])
    assert "/blog/premier" in obtenu and "/blog/second" in obtenu, obtenu


@pytest.mark.parametrize("config,contenu", [
    ("nuxt.config.ts", ["content/blog/premier.md", "content/blog/second.md"]),
    ("next.config.js", ["content/blog/premier.mdx", "content/blog/second.mdx"]),
])
def test_next_et_nuxt_gardent_leur_attrape_tout(config: str, contenu: list[str]) -> None:
    """LA DISTINCTION EST GRAMMATICALE, PAS UN CAS PARTICULIER D'ASTRO.

    Chez Next et Nuxt, `content/blog/premier.md` porte directement la route : il n'y a pas de
    niveau de collection au-dessus. Le premier segment EST un segment d'URL, et un attrape-tout
    de racine — `pages/[...slug].vue`, l'idiome meme de Nuxt Content — suffit donc a le monter.
    Restreindre ces deux-la casserait un mappage juste.
    """
    dynamique = "pages/[...slug].vue" if "nuxt" in config else "pages/[...slug].tsx"
    obtenu = routes([config, "package.json", dynamique] + contenu)
    assert "/blog/premier" in obtenu and "/blog/second" in obtenu, obtenu


def test_le_depot_reel_qui_a_revele_le_defaut() -> None:
    """La forme exacte de `withastro/docs`, reduite a ce qui compte.

    L'attrape-tout n'y est pas decoratif : il sert les redirections vers l'anglais. C'est une
    forme courante, pas une bizarrerie — d'ou le fait qu'elle nous ait echappe sur neuf
    fixtures que nous avons ecrites nous-memes.
    """
    paths = COMMUN_ASTRO + [
        "src/pages/404.astro", "src/pages/index.astro",
        "src/pages/[...enRedirectSlug].astro",
        "src/pages/[lang]/index.astro", "src/pages/[lang]/tutorial.astro",
        "src/content/docs/fr/guides/deploiement.mdx",
        "src/content/docs/en/guides/deploy.mdx",
    ]
    obtenu = routes(paths)
    assert not any(r.startswith("/docs/") for r in obtenu), (
        "une route inventee depuis le nom de la collection : %s"
        % [r for r in obtenu if r.startswith("/docs/")])
