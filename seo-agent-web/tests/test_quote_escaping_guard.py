"""Une apostrophe dans une chaine a guillemets simples casse le fichier.

Mesure sur le passage des neuf stacks (09/09/2026) : le correcteur a livre HUIT fichiers
JS/TS non parsables, sur nuxt (3) et next-app (5). Toujours la meme ligne :

    title: 'Page de test du parcours d'obstacles Noyaru - Duplicate A',

`node --check` repond `SyntaxError: Unexpected identifier 'obstacles'` — l'apostrophe de
`d'obstacles` TERMINE la chaine. En francais c'est le cas COURANT, pas le cas limite, et le
produit corrige des sites francais.

Les quatre autres stacks JS sont indemnes parce que leurs valeurs atterrissent dans des
attributs a guillemets doubles, ou une apostrophe ne signifie rien. Le defaut ne frappe que la
ou la valeur tombe dans un litteral JS a guillemets simples.

Rien en aval ne l'arretait : `_github_patched_content_error` ne regarde que la taille et le
vide. Un fichier qui ne compile plus partait au commit, dans la pull request, et chez le client.

Le garde-fou n'agit QUE sur les lignes que ce patch a ecrites — meme regle que les trois autres,
pour la meme raison : une apostrophe deja presente avant le patch appartient au fichier, pas a
nous.
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

OLD = ("export const metadata = {\n"
       "  title: 'Ancien titre',\n"
       "  description: 'Ancienne description.',\n"
       "};\n")


def test_an_apostrophe_written_by_the_patch_is_escaped() -> None:
    """Le cas mesure sur next-app et nuxt."""
    new = OLD.replace("Ancien titre", "Page de test du parcours d'obstacles Noyaru")
    out, notes = app_module._escape_quotes_in_written_values(new, OLD)
    line = next(ln for ln in out.splitlines() if "title:" in ln)
    assert line.count("'") == 3, f"la chaine n'est pas refermee proprement : {line!r}"
    assert "d\\'obstacles" in line, f"apostrophe non echappee : {line!r}"
    assert notes


def test_a_double_quoted_value_is_left_alone() -> None:
    """Une apostrophe dans une chaine a guillemets DOUBLES est parfaitement legale — c'est
    pourquoi astro, gatsby, sveltekit et next-pages sont sortis indemnes du passage."""
    old = '  title: "Ancien",\n'
    new = '  title: "Le parcours d\'obstacles",\n'
    out, notes = app_module._escape_quotes_in_written_values(new, old)
    assert out == new and notes == []


def test_a_line_the_patch_did_not_write_is_left_alone() -> None:
    """Meme regle que les trois autres garde-fous : ce qui etait la avant ne nous appartient
    pas. Un fichier deja casse a sa propre famille et sa propre pull request."""
    already = "  title: 'Un titre d'origine',\n"
    out, notes = app_module._escape_quotes_in_written_values(already, already)
    assert out == already and notes == []


def test_an_already_escaped_apostrophe_is_not_doubled() -> None:
    """Reechapper produirait `d\\\\'obstacles`, soit un antislash litteral dans le titre servi."""
    new = OLD.replace("Ancien titre", "Le parcours d\\'obstacles")
    out, _ = app_module._escape_quotes_in_written_values(new, OLD)
    assert "d\\\\'" not in out
    assert "d\\'obstacles" in out


def test_an_expression_is_never_touched() -> None:
    """`title: 'a' + suffixe` et un gabarit `${...}` sont du CODE, pas une valeur. Y coller un
    antislash casserait une ligne qui marchait."""
    old = "  title: 'x',\n"
    for expr in ("  title: 'Debut d' + suffixe,\n", "  title: `Debut ${marque} d'ici`,\n"):
        out, notes = app_module._escape_quotes_in_written_values(expr, old)
        assert out == expr and notes == [], f"expression modifiee : {expr!r}"


def test_the_guard_runs_on_every_patch() -> None:
    import inspect
    src = inspect.getsource(app_module._deep_patch_issue_files)
    assert "_escape_quotes_in_written_values(new_content, raw)" in src
    assert src.index("_escape_quotes_in_written_values") < src.index('patch.get("no_change")')


NUXT_OLD = ("useHead({\n"
            "  meta: [\n"
            "    { name: 'description', content: 'Ancienne.' },\n"
            "  ],\n"
            "});\n")


def test_a_value_inside_an_inline_object_is_escaped_too() -> None:
    """Le trou de la premiere version, mesure sur nuxt.

    Le garde-fou exigeait que la valeur occupe TOUTE la ligne (`key: 'value',`). Chez Nuxt les
    valeurs vivent dans `{ name: 'description', content: '...' }`, et il les laissait passer —
    le build a echoue. Mon scan de verification avait le MEME filtre, donc il annoncait zero
    fichier casse : deux instruments avec le meme angle mort se confirmaient l'un l'autre.
    """
    new = NUXT_OLD.replace("Ancienne.", "Le parcours d'obstacles Noyaru")
    out, notes = app_module._escape_quotes_in_written_values(new, NUXT_OLD)
    line = next(ln for ln in out.splitlines() if "content:" in ln)
    assert "d\\'obstacles" in line, f"apostrophe non echappee : {line!r}"
    assert line.count("'") == 5, f"les deux chaines ne sont plus refermees : {line!r}"
    assert notes


def test_the_other_values_on_the_same_line_are_preserved() -> None:
    """`name: 'description'` ne doit pas fusionner avec la valeur voisine."""
    new = NUXT_OLD.replace("Ancienne.", "Le parcours d'obstacles")
    out, _ = app_module._escape_quotes_in_written_values(new, NUXT_OLD)
    assert "name: 'description'" in out


JSX_LINE = ('      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: '
            '`{"@context":"https://schema.org","@type":"SoftwareApplication"}` }} />\n')


def test_a_jsx_attribute_is_never_touched() -> None:
    """Trouve en appliquant le garde-fou aux VRAIS fichiers d'une branche, avant de rebatir.

    La version elargie escaladait de `type="..."` jusqu'a un guillemet bien plus loin sur la
    ligne, et echappait au passage des guillemets qui sont de la SYNTAXE : attribut JSX,
    gabarit `${...}`, litteral de gabarit. Le fichier ne compilait plus. Un attribut JSX
    s'ecrit `nom="valeur"` sans espace autour du `=` ; une entree d'objet s'ecrit `cle: valeur`.
    """
    out, notes = app_module._escape_quotes_in_written_values(JSX_LINE, "")
    assert out == JSX_LINE, f"ligne JSX modifiee : {out!r}"
    assert notes == []


def test_a_template_literal_value_is_never_touched() -> None:
    """Un litteral de gabarit peut contenir tout ce qu'il veut ; y coller un antislash le casse."""
    line = "  const html = `{\"a\": \"b\"}`;\n"
    out, notes = app_module._escape_quotes_in_written_values(line, "")
    assert out == line and notes == []


def test_a_const_declaration_is_still_handled() -> None:
    """SvelteKit, Gatsby et Nuxt ecrivent leurs valeurs ainsi : le `=` reste couvert la."""
    old = "  const title = 'Ancien';\n"
    new = "  const title = 'Le parcours d'obstacles';\n"
    out, notes = app_module._escape_quotes_in_written_values(new, old)
    assert "d\\'obstacles" in out and notes
