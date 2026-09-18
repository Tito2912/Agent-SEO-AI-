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

# Aucune tolerance a ce jour, et c'est un etat, pas un acquis.
#
# La seule qu'il y ait eu concernait `_QUOTED_VALUE_RE`, defini deux fois dans app.py : la version
# sans lookahead, ecrite pour le bloc d'alternates, etait ecrasee par une version restrictive qui
# ne voit pas une valeur suivie d'un commentaire. Reparee le jour meme sous le nom
# `_JS_PROP_VALUE_RE` (voir test_alternates_suivies_d_un_commentaire), et la tolerance est partie
# avec elle — c'est le second test ci-dessous qui l'a exige, au moment prevu.
#
# Une entree ajoutee ici doit porter sa raison ET la mesure qui la justifie, sans quoi elle
# survivra a ce qu'elle tolerait.
TOLEREES: set[tuple[str, str]] = set()


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
    """Une tolerance doit disparaitre avec ce qu'elle tolerait.

    Sans ce bord, la liste survit a sa raison et finit par couvrir une collision neuve portant le
    meme nom. Meme piege que les verdicts perimes de `ops/reparabilite.py`, et meme moment de
    declenchement : juste apres une reussite.

    IL A DEJA SERVI, le jour meme ou il a ete ecrit. `_QUOTED_VALUE_RE` etait la seule tolerance ;
    sa reparation a fait tomber ce test, qui a exige le retrait de l'exception au lieu de la
    laisser dormir. La liste est vide aujourd'hui, et le test passe donc a vide — c'est voulu : il
    n'a rien a dire tant que personne n'ajoute de tolerance, et il redevient exigeant des qu'on en
    ajoute une.
    """
    for nom_module, nom in TOLEREES:
        assert _definis_au_niveau_superieur(MODULES[nom_module])[nom] > 1, (
            "%s : %s n'est plus en collision, retire-le de TOLEREES" % (nom_module, nom))
