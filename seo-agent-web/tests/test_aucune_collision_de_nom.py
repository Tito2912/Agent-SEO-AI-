# -*- coding: utf-8 -*-
"""Deux fois le meme nom dans un module, c'est la premiere definition qui disparait.

Trouve le 18/09/2026 en ECRIVANT, pas en relisant. Une regex nouvelle s'appelait `_HREF_ATTR_RE`
et app.py en avait deja une. Meme module, derniere definition gagnante : la voisine, qui rendait
trois groupes, s'est mise a en rendre deux, et `_canonical_ecrit_dans` a leve un IndexError. Les
dix-huit tests de la famille qu'on ajoutait passaient tous ; QUARANTE-CINQ autres sont tombes.

Ce qui rend la forme dangereuse, c'est qu'elle est muette :

  - `ruff --select=F` couvre F811 pour les fonctions, classes et imports, mais une simple
    affectation de module lui echappe — la barriere CI n'a rien dit ;
  - le fichier compile, s'importe et s'execute sans broncher ;
  - rien ne rattache la panne a sa cause : elle se manifeste loin, dans un code qu'on n'a pas
    touche, et la lecture du diff n'en montre rien.

Le test lit donc le niveau SUPERIEUR des deux gros modules et compte. Il ne regarde pas dans les
fonctions : un nom local qui en masque un autre est une pratique courante et sans danger.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

RACINE = Path(__file__).resolve().parents[2]
MODULES = {
    "app.py": RACINE / "seo-agent-web" / "backend" / "app.py",
    "seo_audit.py": RACINE / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py",
}

# Collision PREEXISTANTE, laissee telle quelle le 18/09/2026 et a traiter a part.
#
# `_QUOTED_VALUE_RE` est defini deux fois dans app.py. La premiere version, sans lookahead, est
# ecrite juste sous `_JS_LANGUAGES_RE` — le bloc JS qu'elle sert. La seconde, plus restrictive,
# la remplace avant tout usage. Les deux consommateurs tournent donc avec la restrictive, et
# celui du bloc hreflang peut manquer des alternates que la premiere aurait pris.
#
# Elle n'est PAS corrigee ici : la reparer change le comportement d'une famille hreflang dont la
# parite Ahrefs est validee, et cela se mesure sur les sites de reference avant de se decider.
# L'exception est nommee pour que le test protege tout le reste des maintenant, et pour que la
# dette reste visible au lieu de disparaitre avec le test qu'on n'aurait pas ecrit.
TOLEREES = {("app.py", "_QUOTED_VALUE_RE")}


def _definis_au_niveau_superieur(chemin: Path) -> Counter:
    arbre = ast.parse(chemin.read_text(encoding="utf-8"))
    compte: Counter = Counter()
    for n in arbre.body:
        if isinstance(n, ast.Assign):
            for cible in n.targets:
                if isinstance(cible, ast.Name):
                    compte[cible.id] += 1
        elif isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name):
            compte[n.target.id] += 1
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            compte[n.name] += 1
    return compte


def test_aucun_nom_de_module_n_est_defini_deux_fois() -> None:
    collisions = []
    for nom_module, chemin in MODULES.items():
        for nom, fois in sorted(_definis_au_niveau_superieur(chemin).items()):
            if fois > 1 and (nom_module, nom) not in TOLEREES:
                collisions.append("%s : %s defini %d fois" % (nom_module, nom, fois))
    assert collisions == [], (
        "la seconde definition ecrase la premiere en silence :\n  " + "\n  ".join(collisions))


def test_la_dette_toleree_existe_encore() -> None:
    """Le jour ou `_QUOTED_VALUE_RE` sera reparee, cette exception doit partir avec elle.

    Sans ce bord, la liste des tolerances survit a ce qu'elle tolerait et finit par couvrir une
    collision neuve portant le meme nom. C'est le meme piege que les verdicts perimes de
    `ops/reparabilite.py`, et il se declenche au meme moment : juste apres une reussite.
    """
    for nom_module, nom in TOLEREES:
        assert _definis_au_niveau_superieur(MODULES[nom_module])[nom] > 1, (
            "%s : %s n'est plus en collision, retire-le de TOLEREES" % (nom_module, nom))
