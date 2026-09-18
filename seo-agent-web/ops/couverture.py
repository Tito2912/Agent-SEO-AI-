# -*- coding: utf-8 -*-
"""Ou en est la COUVERTURE du correcteur, et que reste-t-il qui puisse encore etre corrige ?

    python seo-agent-web/ops/couverture.py

Trois mesures, dans cet ordre, et la troisieme est celle qui a manque deux fois :

  1. combien de familles le produit MONTRE au client, et combien il se declare capable de
     corriger (`_handled_issue_keys`) ;
  2. parmi les autres, lesquelles sont declarees CONSULTATIVES — un choix assume — et lesquelles
     sont simplement en attente ;
  3. parmi celles en attente, lesquelles peuvent REELLEMENT se lever.

LE POINT 3 EXISTE PARCE QUE LE TRI PAR NOM A ECHOUE. Il avait produit au moins sept mauvais
classements : `robots_txt_not_found` et `llms_txt_not_found` semblaient de bonnes candidates, et
sont emises avec une liste vide en dur ; `hreflang_and_html_lang_mismatch` aussi. Une famille
peut porter un nom parfaitement corrigeable et n'etre jamais levee. Cette mesure-ci lit le CODE
d'emission du crawler au lieu du nom :

    MUETTE   emise avec une liste litteralement vide — elle ne parlera jamais ;
    ABSENTE  la cle n'est jamais emise par le crawler ;
    VIVANTE  emise depuis une variable, donc capable de se lever.

Ce que ce script ne dit PAS, et qu'il faut mesurer avant d'ouvrir un chantier : parmi les
vivantes, lesquelles se reparent par une EDITION DE FICHIER. Beaucoup ne le peuvent pas (Bing,
GSC, certificats, TLS, 5xx serveur, minification de build) et d'autres ne le DOIVENT pas — voir
`test_ai_bot_discoverability.py`, ou une decision du proprietaire l'emporte sur la reparabilite.
"""

from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

RACINE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RACINE / "seo-agent-web"))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402
from backend import audit_dashboard as dash  # noqa: E402

CRAWLER = RACINE / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"


def racine_cle(k: str) -> str:
    """Les variantes d'indexabilite sont une seule famille pour qui compte la couverture."""
    return k.removesuffix("_not_indexable").removesuffix("_indexable")


def _emissions(arbre: ast.AST) -> dict[str, list[ast.AST]]:
    """Toutes les affectations `issues["cle"] = ...`, avec la forme de leur valeur."""
    out: dict[str, list[ast.AST]] = {}
    for n in ast.walk(arbre):
        if not isinstance(n, ast.Assign):
            continue
        for c in n.targets:
            if (isinstance(c, ast.Subscript) and isinstance(c.value, ast.Name)
                    and c.value.id == "issues" and isinstance(c.slice, ast.Constant)
                    and isinstance(c.slice.value, str)):
                out.setdefault(c.slice.value, []).append(n.value)
    return out


def _muette(valeur: ast.AST) -> bool:
    """`_issue_block("k", [])`, ou un dict `{"count": 0, ...}` ecrit en dur."""
    if isinstance(valeur, ast.Call) and len(valeur.args) >= 2:
        a = valeur.args[1]
        return isinstance(a, (ast.List, ast.Tuple, ast.Set)) and not a.elts
    if isinstance(valeur, ast.Dict):
        for c, v in zip(valeur.keys, valeur.values):
            if isinstance(c, ast.Constant) and c.value == "count":
                return isinstance(v, ast.Constant) and v.value == 0
    return False


def _avec_preuve(arbre: ast.AST) -> set[str]:
    """Les familles dotees d'un `_attach_evidence`.

    Mesure du 18/09/2026 : elles coincidaient EXACTEMENT avec celles qui ont un correcteur. Ce
    n'est pas un hasard — la preuve existe parce qu'un correcteur en avait besoin. Ajouter une
    famille est donc toujours DEUX chantiers, le crawler d'abord.
    """
    out: set[str] = set()
    for n in ast.walk(arbre):
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                and n.func.id == "_attach_evidence" and n.args):
            for e in ast.walk(n.args[0]):
                if isinstance(e, ast.Constant) and isinstance(e.value, str):
                    out.add(racine_cle(e.value))
    return out


def main() -> None:
    arbre = ast.parse(CRAWLER.read_text(encoding="utf-8"))
    emissions = _emissions(arbre)
    preuves = _avec_preuve(arbre)

    montrees = {k for k in dash.ISSUE_CATALOG
                if k not in dash.NON_ISSUE_KEYS and not dash.is_delta_issue_key(k)}
    gerees = set(m._handled_issue_keys())
    montrees_r = {racine_cle(k) for k in montrees}
    gerees_r = {racine_cle(k) for k in gerees}

    print("familles montrees au client : %d brut, %d racines"
          % (len(montrees), len(montrees_r)))
    print("  dont un correcteur les revendique : %d brut, %d racines"
          % (len(montrees & gerees), len(montrees_r & gerees_r)))

    consultatives, attente = [], []
    for k in sorted(montrees_r - gerees_r):
        if k in m._ADVISORY_ISSUE_KEYS or any(t in k for t in m._ADVISORY_ISSUE_TOKENS):
            consultatives.append(k)
        else:
            attente.append(k)
    print("  CONSULTATIVES declarees          : %d" % len(consultatives))
    print("  sans correcteur ET non declarees : %d" % len(attente))

    groupes: dict[str, list[str]] = {"MUETTE": [], "ABSENTE": [], "VIVANTE": []}
    for k in attente:
        formes = [f for cle, fs in emissions.items() if racine_cle(cle) == k for f in fs]
        if not formes:
            groupes["ABSENTE"].append(k)
        elif all(_muette(f) for f in formes):
            groupes["MUETTE"].append(k)
        else:
            groupes["VIVANTE"].append(k)

    for nom, aide in (("MUETTE", "emise vide en dur : ne se levera jamais"),
                      ("ABSENTE", "jamais emise par le crawler"),
                      ("VIVANTE", "peut se lever ; reste a savoir si un FICHIER la repare")):
        print("\n=== %s : %d  (%s) ===" % (nom, len(groupes[nom]), aide))
        for k in groupes[nom]:
            print("   %s%s" % (k, "   [preuve deja posee]" if k in preuves else ""))


if __name__ == "__main__":
    main()
