"""Une cle ajoutee deux fois dans le meme objet casse le build TypeScript.

Mesure sur le passage des neuf stacks, reproduite en local sur la branche next-app :

    ./app/gauntlet/missing-meta-description/page.tsx:12:5
    Type error: An object literal cannot have multiple properties with the same name.

Le correcteur, charge d'ajouter une description, l'a ajoutee a l'objet `openGraph` qui en avait
DEJA une. En JS pur c'est legal — la derniere gagne — mais TypeScript le refuse et le site ne se
construit plus. **`node --check` ne peut pas l'attraper** : erreur de TYPE, pas de syntaxe. Seul
un vrai build la voit, et c'est Netlify qui l'a dite.

Le garde-fou garde la DERNIERE occurrence, comme le ferait un moteur JS, et ne supprime que les
doublons que CE patch a introduits : deux cles deja en double avant lui appartiennent au fichier
et a leur propre correction.
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
       "  title: 'Un titre',\n"
       "  openGraph: {\n"
       "    type: 'article',\n"
       "    title: 'Un titre',\n"
       "    description: '',\n"
       "    url: 'https://s.fr/a',\n"
       "  },\n"
       "};\n")

# La forme exacte livree par le correcteur : une description ajoutee AVANT celle qui existait.
NEW = ("export const metadata = {\n"
       "  title: 'Un titre',\n"
       "  openGraph: {\n"
       "    description: 'La nouvelle description de la page.',\n"
       "    type: 'article',\n"
       "    title: 'Un titre',\n"
       "    description: 'La nouvelle description de la page.',\n"
       "    url: 'https://s.fr/a',\n"
       "  },\n"
       "};\n")


def _keys(text: str, key: str) -> int:
    return sum(1 for ln in text.splitlines() if ln.strip().startswith(key + ":"))


def test_the_added_duplicate_is_removed() -> None:
    out, notes = app_module._drop_duplicate_object_keys(NEW, OLD)
    assert _keys(out, "description") == 1, out
    assert notes


def test_the_last_occurrence_is_the_one_kept() -> None:
    """Semantique JS : la derniere gagne. Garder la premiere changerait la valeur servie."""
    new = NEW.replace("    description: 'La nouvelle description de la page.',\n"
                      "    type: 'article',",
                      "    description: 'PREMIERE',\n    type: 'article',")
    out, _ = app_module._drop_duplicate_object_keys(new, OLD)
    assert "PREMIERE" not in out
    assert "La nouvelle description de la page." in out


def test_a_key_repeated_in_two_different_objects_is_not_a_duplicate() -> None:
    """`title` dans `openGraph` et dans `twitter` est parfaitement legal — ce sont deux objets."""
    old = ("export const metadata = {\n"
           "  openGraph: {\n    title: 'A',\n  },\n"
           "  twitter: {\n    title: 'B',\n  },\n"
           "};\n")
    out, notes = app_module._drop_duplicate_object_keys(old, old)
    assert out == old and notes == []


def test_a_duplicate_is_collapsed_whoever_wrote_it() -> None:
    """EXCEPTION assumee a la regle des autres garde-fous, pour deux raisons.

    La question n'est pas decidable : mesure sur `missing-title/page.tsx`, le patch avait
    RECOPIE une ligne existante a l'identique, si bien que les deux occurrences paraissaient
    preexistantes et le doublon survivait a un garde-fou qui ne touchait qu'aux lignes ajoutees.

    Et elle n'a pas d'interet : une cle en double est du TypeScript invalide. Si le fichier
    l'avait deja, il ne compilait pas davantage avant nous. Garder la derniere reproduit ce que
    ferait un moteur JS, donc la valeur servie ne change pas.
    """
    out, notes = app_module._drop_duplicate_object_keys(NEW, NEW)
    assert _keys(out, "description") == 1 and notes


def test_a_duplicated_block_key_is_reported_not_collapsed() -> None:
    """`twitter: { ... }` deux fois dans le meme objet : supprimer la ligne emporterait le bloc
    et laisserait une accolade orpheline. Le fichier est refuse en aval, pas repare."""
    doubled = "\n".join([
        "export const metadata = {",
        "  openGraph: {",
        "    twitter: {",
        "      card: 'a',",
        "    },",
        "    twitter: {",
        "      card: 'b',",
        "    },",
        "  },",
        "};",
        "",
    ])
    assert "twitter" in app_module._object_key_conflicts(doubled)


def test_a_clean_file_reports_no_conflict() -> None:
    clean = "\n".join([
        "export const metadata = {",
        "  openGraph: {",
        "    title: 'A',",
        "  },",
        "  twitter: {",
        "    title: 'B',",
        "  },",
        "};",
        "",
    ])
    assert app_module._object_key_conflicts(clean) == []


def test_the_conflict_check_refuses_the_file() -> None:
    import inspect
    src = inspect.getsource(app_module._deep_patch_issue_files)
    assert "_object_key_conflicts(new_content)" in src
    assert "skipped.append(path)" in src


def test_a_file_without_object_literals_is_untouched() -> None:
    """Le garde-fou ne suit QUE les blocs ouverts par `cle: {` ou `const x = {`. Le JSX est
    plein d'accolades qui n'ouvrent aucun objet, et les compter serait le meilleur moyen de
    mutiler un fichier valide."""
    jsx = ("export default function Page() {\n"
           "  return (\n    <main>\n      <p>{texte}</p>\n"
           "      <div style={{ color: 'red' }}>{enfant}</div>\n"
           "    </main>\n  );\n}\n")
    out, notes = app_module._drop_duplicate_object_keys(jsx, "")
    assert out == jsx and notes == []


def test_the_guard_runs_on_every_patch() -> None:
    import inspect
    src = inspect.getsource(app_module._deep_patch_issue_files)
    assert "_drop_duplicate_object_keys(new_content, raw)" in src
    assert src.index("_drop_duplicate_object_keys") < src.index('patch.get("no_change")')
