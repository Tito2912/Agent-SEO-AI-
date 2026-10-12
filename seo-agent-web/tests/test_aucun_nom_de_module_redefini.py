# -*- coding: utf-8 -*-
"""Un nom défini deux fois au niveau d'un module écrase le premier pour TOUT le module.

12/10/2026 : un nouveau garde-fou a déclaré `_META_CONTENU_RE` (forme `content="..."`) alors que
le nom existait déjà, 6 000 lignes plus haut, pour la forme objet `content: '...'`. La seconde
définition remplaçait la première partout : la coupe des descriptions Vue/Nuxt cassait, et
douze tests d'un autre fichier tombaient sans que rien ne pointe la cause. Ruff ne le voit pas
(F811 ne vise que les fonctions et imports inutilisés). Ces trois modules n'ont aucune
redéfinition voulue : en introduire une est toujours une erreur.
"""
from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULES = ["seo-agent-web/backend/app.py", "seo-agent-web/backend/audit_dashboard.py",
           "skills/public/seo-autopilot/scripts/seo_audit.py"]


def _redefinis(source: str) -> list[str]:
    noms: Counter[str] = Counter()
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign):
            noms.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            noms[node.target.id] += 1
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            noms[node.name] += 1
    return sorted(n for n, c in noms.items() if c > 1)


@pytest.mark.parametrize("module", MODULES)
def test_no_module_level_name_is_defined_twice(module) -> None:
    assert _redefinis((ROOT / module).read_text(encoding="utf-8")) == []


def test_the_witness_sees_the_collision_of_12_october() -> None:
    source = "import re\n_META_CONTENU_RE = re.compile('a')\nx = 1\n_META_CONTENU_RE = re.compile('b')\n"
    assert _redefinis(source) == ["_META_CONTENU_RE"]
