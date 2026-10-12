# -*- coding: utf-8 -*-
"""La carte Twitter d'un framework : le modele l'ECRIT, le code en impose les VALEURS.

12/10/2026, banc complet des dix stacks : `twitter_card_missing` restait intacte sur SEPT d'entre
elles (astro, gatsby, hugo, jekyll, next-pages, nuxt, sveltekit), alors que le banc du 17/09 la
resolvait sur huit sur neuf. Le plan protege du 05/10 (6bb40a1) ne lit qu'une page HTML
litterale : face a un `.astro`, un `.vue` ou un front matter, « AUCUNE CIBLE ».

Decision du proprietaire, sur le modele de celle des descriptions : litteral d'abord (HTML pur,
gabarits Jinja) ; sinon le modele ecrit la carte dans l'idiome du fichier, avec les valeurs que
`twitter_card.addition` a tirees de la page servie. Une valeur qu'il inventerait fait refuser le
fichier.
"""
from __future__ import annotations

import base64
from urllib.parse import unquote

import pytest

from backend import app as m
from backend import repo_index
from tests.test_verified_sitemap_https import row

A = "https://site.test/gauntlet/twitter-missing"
B = "https://site.test/gauntlet/autre"
IMAGE = "https://site.test/og.png"
DESC = "Page du parcours d'obstacles : correcte partout sauf sur un point."
TWITTER = dict(twitter_card=None, twitter_title="Page A", twitter_description=None, twitter_image=IMAGE,
               og_title="Page A", og_description=DESC, og_image=IMAGE, title="Page A", meta_description=DESC)
ASTRO = ["astro.config.mjs", "package.json", "src/pages/gauntlet/twitter-missing.astro",
         "src/pages/gauntlet/autre.astro", "src/layouts/Base.astro"]
PAGE = ("---\n---\n<html lang=\"fr\">\n  <head>\n    <title>Page A</title>\n"
        "    <meta property=\"og:description\" content=\"" + DESC.replace("'", "&#x27;") + "\" />\n"
        "    <meta name=\"twitter:title\" content=\"Page A\" />\n"
        "    <meta name=\"twitter:image\" content=\"" + IMAGE + "\" />\n  </head>\n</html>\n")
AJOUT = ("    <meta name=\"twitter:card\" content=\"summary_large_image\" />\n"
         "    <meta name=\"twitter:description\" content=\"" + DESC + "\" />\n")
CORRIGEE = PAGE.replace("  </head>", AJOUT + "  </head>")


def prepare(paths, rows, urls=(A,)):
    key = "twitter_card_missing"
    urls = list(urls)
    return m._prepare_issue_fix(issue_key=key, issues={key: {"count": len(urls), "examples": urls}},
                                impacted=urls, all_paths=paths, site_name="site.test", owner="o",
                                repo_name="r", branch="main", token="t", pages=rows)


def test_a_framework_page_gets_a_plan_with_the_values_the_page_already_proves() -> None:
    plan = prepare(ASTRO, [row(A, **TWITTER)])
    assert not plan["refusal"] and plan["twitter_missing_urls"] is None
    assert plan["targets_override"] == ["src/pages/gauntlet/twitter-missing.astro"]
    assert plan["twitter_values"] == {"src/pages/gauntlet/twitter-missing.astro": {
        "twitter:card": "summary_large_image", "twitter:description": DESC}}
    assert 'twitter:description = "' + DESC + '"' in plan["extra_hint"]
    assert "EXACTEMENT ces valeurs" in plan["extra_hint"]


def test_literal_stacks_keep_the_protected_plan_and_never_reach_the_model() -> None:
    for paths in (["index.html", "gauntlet/twitter-missing.html"],
                  ["app.py", "requirements.txt", "templates/gauntlet/twitter-missing.html"]):
        plan = prepare(paths, [row(A, **TWITTER)])
        assert plan["twitter_missing_urls"] == [A] and "twitter_values" not in plan


def test_a_page_without_its_own_file_is_left_not_guessed() -> None:
    # B n'a pas de fichier : sa carte se poserait dans une mise en page commune.
    paths = [p for p in ASTRO if p != "src/pages/gauntlet/autre.astro"]
    plan = prepare(paths, [row(A, **TWITTER), row(B, **TWITTER)], urls=(A, B))
    assert list(plan["twitter_values"]) == ["src/pages/gauntlet/twitter-missing.astro"]
    assert B in plan["side_effects"] and plan["evidence"] == []
    seule = prepare(paths, [row(B, **TWITTER)], urls=(B,))
    assert seule["refusal"] and "twitter_values" not in seule
    # Une route dynamique rend toute une famille d'URL : ecrire la carte d'UNE page dedans la
    # poserait sur toutes.
    article = "https://site.test/blog/un-article"
    dynamique = prepare(ASTRO + ["src/pages/blog/[slug].astro"], [row(article, **TWITTER)], urls=(article,))
    assert dynamique["refusal"] and "twitter_values" not in dynamique


def test_a_shared_file_is_never_a_page_target_even_if_the_route_map_named_one(monkeypatch) -> None:
    # La carte des routes exclut deja les mises en page ; ce refus ne depend pas d'elle.
    monkeypatch.setattr(m.repo_index, "route_files", lambda index, url: ["src/layouts/Layout.astro"])
    plan = prepare(ASTRO + ["src/layouts/Layout.astro"], [row(A, **TWITTER)])
    assert plan["refusal"] and "twitter_values" not in plan


def run(monkeypatch, ecrite):
    written = {}
    monkeypatch.setattr(m, "_github_api_get", lambda api, **kw: {
        "sha": "s", "encoding": "base64", "content": base64.b64encode(PAGE.encode()).decode()})

    def put(api, **kw):
        written[unquote(api.split("/contents/", 1)[1])] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "n"}}
    monkeypatch.setattr(m, "_github_api_put", put)
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kw: {"patched_content": ecrite})
    for name in ("_ai_map_urls_to_files", "_ai_pick_repo_files", "_github_tarball_grep"):
        monkeypatch.setattr(m, name, lambda *a, **kw: pytest.fail("la cible est connue"))
    rows = [row(A, **TWITTER)]
    applied = m._apply_prepared_issue_fix(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="qa", all_paths=list(ASTRO),
        issue_key="twitter_card_missing", issue_label="Twitter", impacted=[A], site_name="site.test",
        file_state={}, max_files=3, prep=prepare(ASTRO, rows), pages=rows,
        index=repo_index.build_repo_index(ASTRO))
    return applied, written


def test_the_card_written_with_the_imposed_values_is_committed(monkeypatch) -> None:
    applied, written = run(monkeypatch, CORRIGEE)
    assert written == {"src/pages/gauntlet/twitter-missing.astro": CORRIGEE}
    assert applied["ai_files"] == ["src/pages/gauntlet/twitter-missing.astro"]


@pytest.mark.parametrize("ecrite", [
    # Une description reformulee : le modele a choisi la valeur.
    CORRIGEE.replace(DESC + "\" />", "Un parcours d'obstacles pour tester le SEO.\" />"),
    # Une autre carte que celle imposee.
    CORRIGEE.replace("summary_large_image", "summary"),
    # Une image inventee.
    CORRIGEE.replace("  </head>", "    <meta name=\"twitter:site\" content=\"@noyaru\" />\n  </head>"),
])
def test_a_value_the_model_chose_refuses_the_whole_file(monkeypatch, ecrite) -> None:
    applied, written = run(monkeypatch, ecrite)
    assert written == {} and applied["patched"] == []


@pytest.mark.parametrize("ligne, ancien, imposees, attendu", [
    # Objet de metadonnees : la valeur est seule sur sa ligne, sans le mot « twitter ».
    ("    description: 'Texte libre',", "", {"twitter:description": "Texte impose"}, "Texte libre"),
    ("    description: 'Texte impose',", "", {"twitter:description": "Texte impose"}, ""),
    # Echappements JS et HTML : une meme valeur, trois ecritures.
    ("    description: 'L\\'accueil',", "", {"twitter:description": "L'accueil"}, ""),
    ('    <meta name="twitter:description" content="L&#x27;accueil" />', "", {"twitter:description": "L'accueil"}, ""),
    # Ce que le fichier contenait deja (l'image Open Graph reprise) n'est pas une invention.
    ("    images: ['/og.png'],", "openGraph: { images: ['/og.png'] }", {"twitter:card": "summary_large_image"}, ""),
    ("    card: 'summary',", "", {"twitter:card": "summary_large_image"}, "summary"),
    # Objet a cles entre guillemets (JSON, front matter) : une cle n'est pas une valeur.
    ('  "twitter": {"card": "summary_large_image", "description": "Texte impose"},', "",
     {"twitter:card": "summary_large_image", "twitter:description": "Texte impose"}, ""),
])
def test_only_imposed_known_or_existing_literals_may_be_added(ligne, ancien, imposees, attendu) -> None:
    nouveau = ancien + "\n" + ligne + "\n"
    assert m._valeur_twitter_non_imposee(nouveau, ancien, imposees) == (
        "valeur non imposee : " + attendu if attendu else "")
