"""Analyser le litteral de valeurs de tete, au lieu d'en deviner les erreurs.

SIX formes distinctes dont le modele a casse un fichier JS le 10/09/2026, toutes trouvees par le
deploiement reel, toutes au MEME endroit : le litteral d'objet des valeurs de tete
(`export const metadata = {…}`, `useHead({…})`). Chacune a demande son garde-fou, et cinq des
sept ecrits ce jour-la ont du etre revises — trop etroits, puis dangereux. La septieme forme
serait arrivee.

Cette region ne contient que des DONNEES : chaines, tableaux, objets. Ni types TypeScript, ni
JSX. Un analyseur strict de litteral suffit donc, sans dependance et sans se heurter au TSX.

MESURE QUI A DECIDE DE L'APPROCHE : 73 % des corrections sur les stacks JS passent par la
reecriture complete par le modele — 111 sur 152. Lui retirer ce droit couterait les trois quarts
du produit sur ces stacks, pour un ou deux fichiers casses par cycle. On valide donc au lieu
d'interdire.

REGLE CARDINALE : EN CAS DE DOUTE, ACCEPTER. Un faux refus coute une correction reelle. Tout ce
que l'analyseur ne sait pas lire — un gabarit, un appel, une expression — passe sans controle.
On ne refuse que sur une erreur CERTAINE.
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


def _j(*lines: str) -> str:
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


def test_a_valid_metadata_object_passes() -> None:
    ok = _j("export const metadata = {",
            "  title: 'Un titre',",
            "  openGraph: { type: 'article', images: ['https://s.fr/a.png'] },",
            "};")
    assert app_module._object_literal_error(ok, "app/page.tsx") == ""


def test_a_valid_usehead_passes() -> None:
    ok = _j("useHead({",
            "  title: 'Un titre',",
            "  meta: [",
            "    { name: 'description', content: 'Une description.' },",
            "    { property: 'og:title', content: 'Un titre' },",
            "  ],",
            "});")
    assert app_module._object_literal_error(ok, "pages/a.vue") == ""


# ── Les six formes reelles ─────────────────────────────────────────────────────────────────

def test_form1_unescaped_apostrophe() -> None:
    bad = _j("export const metadata = {",
             "  title: 'Page de test du parcours d'obstacles Noyaru',",
             "};")
    assert app_module._object_literal_error(bad, "app/page.tsx")


def test_form2_duplicate_key() -> None:
    bad = _j("export const metadata = {",
             "  openGraph: { description: 'A', type: 'article', description: 'B' },",
             "};")
    assert app_module._object_literal_error(bad, "app/page.tsx")


def test_form3_missing_comma_between_properties() -> None:
    bad = _j("export const metadata = {",
             "  description: 'Une description.'",
             "  title: 'Un titre',",
             "};")
    assert app_module._object_literal_error(bad, "app/page.tsx")


def test_form4_missing_comma_between_array_items() -> None:
    bad = _j("useHead({",
             "  meta: [",
             "    { property: 'og:image', content: 'https://s.fr/og.png' }",
             "    { name: 'twitter:card', content: 'summary' },",
             "  ],",
             "});")
    assert app_module._object_literal_error(bad, "pages/a.vue")


def test_form5_array_never_closed() -> None:
    bad = _j("useHead({",
             "  meta: [",
             "    { name: 'description', content: 'Une description.' },",
             "  htmlAttrs: { lang: 'fr' },",
             "});")
    assert app_module._object_literal_error(bad, "pages/a.vue")


def test_form6_orphan_item_after_the_array_closed() -> None:
    bad = _j("useHead({",
             "  meta: [",
             "    { name: 'twitter:image', content: 'https://s.fr/og.png' }",
             "  ],",
             "    { rel: 'alternate', hreflang: 'x-default', href: 'https://s.fr/a' }",
             "});")
    assert app_module._object_literal_error(bad, "pages/a.vue")


# ── En cas de doute, ACCEPTER ──────────────────────────────────────────────────────────────

def test_a_template_literal_is_accepted_unchecked() -> None:
    """On ne sait pas lire un gabarit ; le refuser couterait une correction reelle."""
    expr = _j("export const metadata = {",
              "  title: `Un titre ${marque}`,",
              "};")
    assert app_module._object_literal_error(expr, "app/page.tsx") == ""


def test_a_function_call_is_accepted_unchecked() -> None:
    expr = _j("export const metadata = {",
              "  title: buildTitle('accueil'),",
              "};")
    assert app_module._object_literal_error(expr, "app/page.tsx") == ""


def test_a_spread_is_accepted_unchecked() -> None:
    expr = _j("export const metadata = {",
              "  ...base,",
              "  title: 'Un titre',",
              "};")
    assert app_module._object_literal_error(expr, "app/page.tsx") == ""


def test_a_file_without_head_values_is_out_of_scope() -> None:
    assert app_module._object_literal_error("const x = 1;\n", "app/util.ts") == ""


def test_a_non_js_file_is_out_of_scope() -> None:
    assert app_module._object_literal_error("title: 'x'\n", "content/a.md") == ""


def test_the_validator_refuses_before_commit(monkeypatch) -> None:
    """Un litteral de valeurs de tete illisible ne part pas chez le client."""
    casse = "export const metadata = {\n  title: 'A',\n  openGraph: {\n    url: '/x',\n};\n"
    patched, skipped, commits = _refuse_avant_commit(monkeypatch, "app/page.tsx", casse)
    assert patched == [] and skipped == ["app/page.tsx"], (patched, skipped)
    assert commits == [], commits
