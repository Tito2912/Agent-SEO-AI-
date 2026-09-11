# -*- coding: utf-8 -*-
"""Une famille que le correcteur revendique doit pouvoir LUI PARVENIR.

Le 11/09/2026, sept familles de base etaient revendiquees par le correcteur sans pouvoir
atteindre l'ecran : les six familles de redirection de ressources n'avaient aucune entree dans
`ISSUE_CATALOG`. Le crawler les levait, le correcteur savait les traiter, et l'utilisateur ne les
voyait jamais — donc aucune tache de correction ne naissait. Rien ne le signalait : le banc
d'essai lui-meme ne pouvait pas les exercer, puisque son filtre passe par le meme `shown()`.

Ce test refuse le silence : toute clef revendiquee est soit montrable, soit DECLAREE
inatteignable avec sa raison.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend import app as m  # noqa: E402
from backend import audit_dashboard as dash  # noqa: E402

# Inatteignables VOULUES, avec la raison. Toute autre est un trou.
INATTEIGNABLES_ASSUMEES: dict[str, str] = {
    "missing_canonical":
        "parite Ahrefs : Ahrefs n'a aucune anomalie « canonical manquant ». Le cas nuisible est "
        "`duplicate_pages_without_canonical`, leve separement. Le gestionnaire reste en place.",
}


def _montrable(key: str) -> bool:
    """La meme question que se pose l'ecran — et que se pose le banc d'essai."""
    return (key in dash.ISSUE_CATALOG
            and key not in dash.NON_ISSUE_KEYS
            and not dash.is_delta_issue_key(key))


def _clefs_de_base() -> set[str]:
    """Sans les jumelles d'indexabilite, fabriquees en bloc et souvent sans entree propre."""
    return {k for k in m._handled_issue_keys()
            if not k.endswith("_indexable") and not k.endswith("_not_indexable")}


def test_toute_famille_revendiquee_est_montrable_ou_declaree():
    inatteignables = {k for k in _clefs_de_base() if not _montrable(k)}
    inattendues = sorted(inatteignables - set(INATTEIGNABLES_ASSUMEES))
    assert not inattendues, (
        "ces familles sont revendiquees par le correcteur mais ne peuvent jamais lui parvenir : "
        + ", ".join(inattendues)
        + " — ajoute-les a ISSUE_CATALOG, ou declare-les dans INATTEIGNABLES_ASSUMEES."
    )


def test_les_six_familles_de_redirection_de_ressources_sont_montrables():
    """Elles ont ete prouvees au crawl le 11/09/2026 ; elles doivent atteindre l'ecran."""
    for key in ("image_redirects", "page_has_redirected_image",
                "javascript_redirects", "page_has_redirected_javascript",
                "css_redirects", "page_has_redirected_css"):
        assert _montrable(key), key


def test_une_inatteignable_assumee_porte_sa_raison():
    for key, raison in INATTEIGNABLES_ASSUMEES.items():
        assert key in m._handled_issue_keys(), key
        assert not _montrable(key), key
        assert len(raison) > 40, key
