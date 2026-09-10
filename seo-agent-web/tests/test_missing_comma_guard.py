"""Une propriete inseree sans virgule casse le fichier.

Mesure sur le cycle complet du 10/09/2026, reproduite en local sur la branche next-app :

    Expected ',', got 'title'
      description: 'Decouvrez le parcours d'obstacles Noyaru...'    <- pas de virgule
      title: 'Page de test du parcours d'obstacles Noyaru',

Le modele a omis la virgule en inserant une propriete. Verifie que le garde-fou d'echappement
la preserve, teste dans les deux sens : ce n'est pas lui.

CELUI-CI est sur, contrairement aux trois autres garde-fous JS de la journee. Ajouter une
virgule manquante entre deux proprietes ne peut changer AUCUNE semantique — c'est la seule
lecture possible du texte. On n'agit que dans ce cas precis et lisible :

* la ligne se termine par une chaine complete (guillemet fermant) ;
* la ligne SUIVANTE, a la meme indentation, commence une nouvelle cle.

Rien d'autre. Une valeur qui s'etale sur plusieurs lignes, une expression, la derniere propriete
avant l'accolade : on n'y touche pas, parce qu'on ne saurait pas.
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


def _join(*lines: str) -> str:
    return "\n".join(lines) + "\n"


def test_the_next_app_case_is_repaired() -> None:
    broken = _join(
        "export const metadata = {",
        "  description: 'Decouvrez le parcours Noyaru.'",
        "  title: 'Page de test',",
        "};")
    out, notes = app_module._add_missing_object_commas(broken)
    assert "'Decouvrez le parcours Noyaru.'," in out
    assert notes


def test_a_line_already_ending_with_a_comma_is_untouched() -> None:
    ok = _join(
        "export const metadata = {",
        "  description: 'Une description.',",
        "  title: 'Un titre',",
        "};")
    out, notes = app_module._add_missing_object_commas(ok)
    assert out == ok and notes == []


def test_the_last_property_before_the_brace_is_left_alone() -> None:
    """Une virgule finale serait legale, mais l'ajouter ne repare rien : on ne touche pas a ce
    qui n'est pas casse."""
    ok = _join(
        "export const metadata = {",
        "  title: 'Un titre'",
        "};")
    out, notes = app_module._add_missing_object_commas(ok)
    assert out == ok and notes == []


def test_a_value_spanning_several_lines_is_never_touched() -> None:
    """On ne saurait pas ou la valeur finit — donc on s'abstient."""
    multi = _join(
        "export const metadata = {",
        "  images: [",
        "    'https://s.fr/a.png'",
        "  ],",
        "};")
    out, notes = app_module._add_missing_object_commas(multi)
    assert out == multi and notes == []


def test_jsx_is_never_touched() -> None:
    """Le JSX est plein de lignes qui ressemblent a des proprietes et n'en sont pas."""
    jsx = _join(
        "export default function Page() {",
        "  return (",
        "    <main>",
        "      <p>{texte}</p>",
        "    </main>",
        "  );",
        "}")
    out, notes = app_module._add_missing_object_commas(jsx)
    assert out == jsx and notes == []


def test_a_nested_object_keeps_its_own_indentation_rule() -> None:
    """La cle suivante doit etre au MEME niveau : sinon elle appartient a un autre objet et la
    virgule manquante n'est pas la ou on croit."""
    nested = _join(
        "export const metadata = {",
        "  openGraph: {",
        "    title: 'A'",
        "    url: 'https://s.fr/a',",
        "  },",
        "};")
    out, notes = app_module._add_missing_object_commas(nested)
    assert "title: 'A'," in out and notes


def test_the_guard_runs_before_the_commit() -> None:
    import inspect
    src = inspect.getsource(app_module._deep_patch_issue_files)
    assert "_add_missing_object_commas(new_content)" in src
    assert src.index("_add_missing_object_commas") < src.index("put_body")
