# -*- coding: utf-8 -*-
"""La liste des familles a traiter ne doit pas pouvoir vieillir en silence.

`ops/reparabilite.py` dit, pour chaque famille VIVANTE, si une edition de fichier la repare. Une
telle table est utile le jour ou on l'ecrit et trompeuse trois commits plus tard : une famille
corrigee y reste classee « a faire », une famille nouvellement detectee n'y figure pas du tout,
et on choisit le chantier suivant sur une photo perimee. C'est exactement ainsi que le tri par
nom avait produit sept mauvais classements.

Ce test tient les deux bords a la fois :

  - toute vivante doit avoir un verdict, sinon une famille ajoutee au crawler passe inapercue ;
  - tout verdict doit porter sur une vivante, sinon une famille qu'on vient de CORRIGER continue
    de figurer dans la liste des chantiers.

Le second bord est le plus utile des deux : il se declenche precisement au moment ou l'on vient
de reussir quelque chose, c'est-a-dire au moment ou personne ne pense a relire la table.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops import reparabilite  # noqa: E402


def test_chaque_famille_vivante_a_un_verdict() -> None:
    orphelines, _ = reparabilite.verdicts_manquants()
    assert orphelines == [], (
        "familles vivantes sans verdict dans ops/reparabilite.py : %s" % orphelines)


def test_aucun_verdict_ne_survit_a_la_famille_qu_il_classait() -> None:
    _, perimes = reparabilite.verdicts_manquants()
    assert perimes == [], (
        "verdicts sans objet (famille corrigee, renommee ou devenue muette) : %s" % perimes)


def test_chaque_verdict_est_l_un_des_cinq() -> None:
    """Une sixieme classe inventee au fil de l'eau viderait le classement de son sens."""
    inconnus = sorted({v for v, _ in reparabilite.VERDICTS.values()} - set(reparabilite.ORDRE))
    assert inconnus == [], "verdicts hors nomenclature : %s" % inconnus


def test_chaque_verdict_dit_POURQUOI() -> None:
    """Une raison vide rend la table illisible et la decision non rejouable."""
    muets = sorted(k for k, (_, raison) in reparabilite.VERDICTS.items() if len(raison) < 20)
    assert muets == [], "verdicts sans raison utilisable : %s" % muets


def test_une_cle_morte_du_catalogue_ne_masque_pas_une_decision() -> None:
    """Une famille dont toutes les variantes EMISES sont consultatives est tranchee, point.

    Le catalogue montre parfois une cle nue a cote de ses variantes d'indexabilite — `orphan_page`
    a cote de `orphan_page_indexable` et `orphan_page_not_indexable`. Cette cle nue n'est emise
    par personne : elle ne decide de rien, mais elle suffisait a faire ressortir la famille parmi
    les chantiers a ouvrir, alors que ses deux variantes vivantes sont declarees consultatives
    depuis longtemps. On aurait rouvert une decision deja prise en croyant combler un trou.

    Le test tient la propriete, pas le nom : aucune famille classee VIVANTE ne doit avoir toutes
    ses variantes emises declarees consultatives.
    """
    from backend import app as m
    from ops import couverture

    mes = couverture.mesurer()
    emises = mes["emises"]

    def consultative(cle: str) -> bool:
        return cle in m._ADVISORY_ISSUE_KEYS or any(t in cle for t in m._ADVISORY_ISSUE_TOKENS)

    fautives = [k for k in mes["groupes"]["VIVANTE"]
                if emises.get(k) and all(consultative(v) for v in emises[k])]
    assert fautives == [], (
        "familles deja tranchees, remontees comme chantiers a ouvrir : %s" % fautives)
