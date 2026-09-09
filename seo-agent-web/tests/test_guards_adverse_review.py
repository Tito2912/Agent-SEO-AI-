"""Relecture adverse des trois garde-fous permanents (2026-09-09).

Ces trois fonctions tournent sur CHAQUE fichier que le correcteur ecrit, toutes familles
confondues, et n'avaient jamais eu de relecture dediee. Quatre defauts mesures, tous de la meme
forme que les onze precedents : le garde-fou agit sans un fait que le pipeline avait deja sous la
main — le type de fichier qu'il modifie, et la ligne que le patch a reellement ecrite.

1. `_FRONTMATTER_VALUE_RE` n'est pas borne au bloc de front matter. Il matche n'importe quelle
   ligne `title:` / `description:`, donc les objets JavaScript — precisement la forme de
   `export const metadata` de Next.js App Router, d'Astro et de Nuxt. Le groupe de valeur avale
   le guillemet fermant et la virgule, et la coupe rend le fichier non parsable.
2. Consequence du meme defaut : les guillemets et la virgule sont comptes dans la longueur, donc
   un titre de 70 caracteres — pile au plafond, donc legal — est mesure a 73 et coupe.
3. `old_fm` est un dict indexe par nom de champ : seule la DERNIERE ligne `title:` de l'ancien
   fichier est memorisee. Toutes les precedentes passent pour modifiees et sont coupees alors que
   le patch ne les a jamais touchees — ce que le docstring du garde-fou interdit explicitement.
4. `_forbid_https_downgrade` ne compare jamais avec l'ancien contenu : il reecrit tout `http://`
   dont la forme https existe ailleurs dans le fichier, y compris un lien que le patch n'a pas
   touche. Le docstring dit « any URL this patch turned from https into http » ; le code fait
   autre chose.

Et un cinquieme, dans le reecriveur a deux extremites : cote court, seul le plafond est verifie
sur la valeur produite, jamais le plancher du crawler. Une valeur toujours trop courte est
comptee comme une correction — une unite facturee pour une anomalie qui reste.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")

from backend import app as app_module  # noqa: E402


NEXT_OLD = """export const metadata = {
  title: 'Ancien titre',
  description: 'Ancienne description.',
};
"""

TOO_LONG_TITLE = ("Chaussures de randonnee impermeables pour homme et femme "
                  "en cuir pleine fleur - Boutique Alpine")

# Exactement 70 caracteres : la valeur est AU plafond, donc parfaitement legale.
TITLE_AT_CEILING = "Chaussures de randonnee impermeables en cuir pour homme - Alpine Store"


def _line(text: str, needle: str) -> str:
    for line in text.splitlines():
        if needle in line:
            return line
    return ""


def test_a_javascript_metadata_object_is_not_left_unparseable() -> None:
    """Le defaut le plus grave : le fichier livre dans la PR ne compile plus.

    `export const metadata` de Next.js App Router s'ecrit exactement comme ca. La coupe emporte
    le guillemet fermant et la virgule, et la branche part au commit sans que rien en aval ne
    s'en apercoive — le seul filet, `_github_patched_content_error`, ne regarde que la taille.
    """
    new = NEXT_OLD.replace("Ancien titre", TOO_LONG_TITLE)
    out, _notes = app_module._enforce_length_ceilings(new, NEXT_OLD)
    line = _line(out, "title:")
    assert line.rstrip().endswith("',"), f"chaine JavaScript non terminee : {line!r}"


def test_a_javascript_title_at_the_ceiling_is_not_trimmed_by_its_own_quotes() -> None:
    """Les guillemets et la virgule ne font pas partie de la valeur que le crawler mesure."""
    assert app_module._rendered_len(TITLE_AT_CEILING) == app_module._LENGTH_CEILINGS["title"]
    new = NEXT_OLD.replace("Ancien titre", TITLE_AT_CEILING)
    out, notes = app_module._enforce_length_ceilings(new, NEXT_OLD)
    assert out == new and notes == []


TWO_TITLES_OLD = """export const home = {
  title: 'Un titre daccueil parfaitement legitime et deja en place sur le site',
};
export const about = {
  title: 'Ancien',
};
"""


def test_a_line_the_patch_never_wrote_is_left_alone() -> None:
    """« Only values that CHANGED are considered » — le dict indexe par champ ne le tient pas."""
    untouched = "'Un titre daccueil parfaitement legitime et deja en place sur le site',"
    new = TWO_TITLES_OLD.replace("  title: 'Ancien',", "  title: 'A propos de la boutique',")
    out, _notes = app_module._enforce_length_ceilings(new, TWO_TITLES_OLD)
    assert untouched in out, "une ligne que le patch n'a pas ecrite a ete coupee"


BODY_OLD = """---
title: Court
---

Pour declarer le titre de la page, ecrivez la ligne suivante :

    title: un exemple court
"""

# Une ligne de prose que le patch ecrit : ce n'est pas une balise, sa longueur ne regarde
# personne, et la couper mutile la documentation du client.
LONG_EXAMPLE = ("    title: un exemple de titre volontairement long recopie tel quel dans la "
                "documentation du theme")


def test_a_body_line_that_looks_like_front_matter_is_not_front_matter() -> None:
    """Le front matter est le bloc de tete, pas n'importe quelle ligne du document."""
    new = BODY_OLD.replace("    title: un exemple court", LONG_EXAMPLE)
    out, _notes = app_module._enforce_length_ceilings(new, BODY_OLD)
    assert LONG_EXAMPLE in out, "une ligne du corps du document a ete coupee"


HTTPS_OLD = """<link rel="canonical" href="https://exemple.fr/page" />
<a href="http://exemple.fr/page">lien historique</a>
"""


def test_the_scheme_guard_leaves_a_link_the_patch_never_wrote() -> None:
    """Le garde-fou restaure ce que le patch a degrade ; il ne fait pas la tournee du fichier.

    Un lien deja en http avant le patch appartient a sa propre famille et a sa propre PR. Le
    reecrire ici elargit le diff hors de la famille annoncee, et suffit a transformer un patch
    sans effet en commit — le test `no_change` du site d'appel a lieu APRES les garde-fous.
    """
    new = HTTPS_OLD.replace("lien historique", "lien historique du site")
    out, notes = app_module._forbid_https_downgrade(new, HTTPS_OLD)
    assert 'href="http://exemple.fr/page"' in out, "un lien non touche par le patch a ete reecrit"
    assert notes == []


def test_a_rewrite_that_stays_under_the_crawler_floor_is_refused(monkeypatch) -> None:
    """Cote court, seul le plafond etait verifie. Une valeur toujours sous le plancher du
    crawler passait pour une correction : l'anomalie reste et l'unite est facturee."""
    monkeypatch.setattr(app_module, "_length_value_for_page", lambda **kw: "Trop court")
    out, n = app_module._rewrite_length_values(
        "<title>Test</title>", {"https://x.fr/a": {"rendered": "Test", "len": 4}}, "title")
    assert n == 0, "une valeur sous le plancher a ete comptee comme corrigee"
    assert "<title>Test</title>" in out
