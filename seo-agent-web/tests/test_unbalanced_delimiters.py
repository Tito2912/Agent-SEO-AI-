"""Un fichier dont les delimiteurs ne se referment pas n'est pas commite.

CINQUIEME facon dont le modele casse un fichier JS, mesuree sur nuxt au cycle complet :

    useHead({
        title: '...',
        meta: [
          { name: 'description', content: '...' },
        htmlAttrs: { lang: 'fr' },      <- le tableau `meta: [` n'est JAMAIS ferme
        meta: [ ...

Ni le garde-fou de cles dupliquees ni celui de virgules ne pouvaient le voir : tous deux ne
suivent que les litteraux d'OBJET, et `meta: [` ouvre un tableau.

On ne REPARE pas : refermer un tableau demanderait de deviner ou. On REFUSE, ce qui ne demande
rien — compter les delimiteurs hors chaines et hors commentaires est deterministe. Un refus se
voit ; un deploiement casse, non.

Les faux refus sont le vrai risque, puisqu'un refus coute une correction perdue. D'ou les cas
ci-dessous : gabarit `${...}`, accolade dans une chaine, commentaire, JSX. Aucun ne doit
declencher.
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


def _refuse_avant_commit(monkeypatch, chemin: str, contenu: str) -> tuple[list, list, list]:
    """Fait passer `contenu` par la boucle de correction et rend (patches, refuses, commits).

    MESURE LE RESULTAT, PAS LA FORME DU CODE. La version precedente de ce test cherchait un
    appel a l'interieur de `_deep_patch_issue_files` : elle interdisait de deplacer cet appel,
    et a casse sur un refactor qui ne changeait rien au comportement — les refus ont ete
    regroupes dans `_refus_de_format`, partagee avec la route par URL qui, elle, ne les
    appliquait pas. Un test qui decrit OU le code appelle quelque chose empeche de reparer
    ailleurs.
    """
    commits: list = []
    monkeypatch.setattr(app_module, "_github_api_put",
                        lambda c, **kw: commits.append(c) or {"content": {"sha": "n"}})
    patched, skipped, _t, _a = app_module._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=[chemin], issue_key="k", issue_label="L", impacted_urls=[], site_name="s",
        file_state={chemin: {"sha": "v", "content": "x"}},
        max_files=2, targets_override=[chemin], allow_ai_targeting=False,
        link_rewriter=lambda _raw: (contenu, 1), rewriter_ai_fallback=False)
    return patched, skipped, commits


def test_the_nuxt_case_an_array_never_closed() -> None:
    broken = _join(
        "useHead({",
        "  title: 'Un titre',",
        "  meta: [",
        "    { name: 'description', content: 'Une description.' },",
        "  htmlAttrs: { lang: 'fr' },",
        "});")
    assert app_module._unbalanced_delimiters(broken)


def test_a_balanced_file_passes() -> None:
    ok = _join(
        "useHead({",
        "  title: 'Un titre',",
        "  meta: [",
        "    { name: 'description', content: 'Une description.' },",
        "  ],",
        "});")
    assert app_module._unbalanced_delimiters(ok) == ""


def test_a_brace_inside_a_string_is_not_a_delimiter() -> None:
    """Le JSON-LD d'une page en est plein."""
    ok = _join(
        "const data = {",
        "  html: '{\"@type\":\"Product\"}',",
        "};")
    assert app_module._unbalanced_delimiters(ok) == ""


def test_a_template_literal_is_not_scanned() -> None:
    """`${...}` s'equilibre tout seul, mais une accolade de contenu ne doit pas compter."""
    ok = _join(
        "const html = `{\"a\": ${valeur}}`;",
        "const autre = `du texte avec { une accolade seule`;")
    assert app_module._unbalanced_delimiters(ok) == ""


def test_a_comment_is_not_scanned() -> None:
    """Un commentaire peut contenir n'importe quoi — le refuser serait un faux refus."""
    ok = _join(
        "// une accolade orpheline { dans un commentaire",
        "/* et un crochet [ dans un bloc */",
        "const x = 1;")
    assert app_module._unbalanced_delimiters(ok) == ""


def test_jsx_passes() -> None:
    jsx = _join(
        "export default function Page() {",
        "  return (",
        "    <main>",
        "      <div style={{ color: 'red' }}>{enfant}</div>",
        "    </main>",
        "  );",
        "}")
    assert app_module._unbalanced_delimiters(jsx) == ""


def test_the_message_names_the_delimiter() -> None:
    broken = _join("const x = {", "  a: 1,")
    err = app_module._unbalanced_delimiters(broken)
    assert "{" in err


def test_the_check_refuses_the_file_before_commit(monkeypatch) -> None:
    """Un fichier aux delimiteurs desequilibres ne part pas chez le client.

    L'analyseur de litteral passe avant et nomme souvent la cause plus precisement ; ce qui
    est verrouille ici est le RESULTAT — rien n'est commite — et non lequel des deux a parle.
    """
    # L'echantillon de ce fichier. Ma premiere version prenait du JSX non ferme — mais ce
    # controle compte les ACCOLADES, pas les balises, et mon echantillon etait parfaitement
    # equilibre : le test accusait le code de laisser passer un fichier valide.
    casse = _join("const x = {", "  a: 1,")
    patched, skipped, commits = _refuse_avant_commit(monkeypatch, "app/page.tsx", casse)
    assert patched == [] and skipped == ["app/page.tsx"], (patched, skipped)
    assert commits == [], commits
