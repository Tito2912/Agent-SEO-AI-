# -*- coding: utf-8 -*-
"""Sur un téléphone, chaque écran garde un chemin vers les autres.

04/10/2026 : sous 980 px, la barre latérale de l'application était masquée (`display: none`) sans
rien pour la remplacer — un client sur téléphone ne pouvait atteindre ni son abonnement, ni ses
comptes, ni les sections d'un projet. Sur les pages publiques, les liens Tarifs, Documentation,
Guides et Support étaient masqués eux aussi sous 640 px. Mesuré à 360 px, sept écrans débordaient
(jusqu'à 782 px) : barre du haut sans retour à la ligne, éléments de grille qui ne descendent pas
sous leur largeur de contenu, titres de carte et leurs boutons.
"""
from __future__ import annotations

import re
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
CSS = (WEB_ROOT / "static" / "styles.css").read_text(encoding="utf-8")


def _blocs_media(css: str, largeur: int) -> str:
    """Le contenu de chaque `@media (max-width: <largeur>px) { ... }`, accolades équilibrées."""
    morceaux = []
    for m in re.finditer(r"@media\s*\(max-width:\s*%dpx\)\s*\{" % largeur, css):
        profondeur, i = 1, m.end()
        while profondeur:
            profondeur += {"{": 1, "}": -1}.get(css[i], 0)
            i += 1
        morceaux.append(css[m.end():i - 1])
    return "\n".join(morceaux)


def _regle(css: str, selecteur: str) -> str:
    # le sélecteur ENTIER, en tête de ligne ou après une accolade : « .sidebar » ne doit pas
    # trouver « .nav-mobile-case:checked ~ .sidebar »
    m = re.search(r"(?:^|\})[ \t]*%s\s*\{([^}]*)\}" % re.escape(selecteur), css, flags=re.M)
    return m.group(1) if m else ""


def test_la_barre_laterale_masquee_a_un_tiroir_pour_la_remplacer() -> None:
    mobile = _blocs_media(CSS, 980)
    assert "display: none" in _regle(mobile, ".sidebar")
    tiroir = _regle(mobile, ".nav-mobile-case:checked ~ .sidebar")
    assert "display: flex" in tiroir and "position: fixed" in tiroir, tiroir
    assert "overflow-y: auto" in tiroir, "une barre plus haute que l'écran doit défiler"
    assert "display: inline-flex" in _regle(mobile, ".nav-mobile-bouton")
    # hors mobile, ni bouton ni voile
    assert "display: none" in _regle(CSS, ".nav-mobile-case, .nav-mobile-voile, .nav-mobile-bouton")


def test_le_bouton_menu_est_dans_la_barre_du_haut_et_la_case_precede_la_barre_laterale() -> None:
    layout = (WEB_ROOT / "templates" / "layout.html").read_text(encoding="utf-8")
    app = layout.index('<div class="app">')
    case = layout.index('id="nav-mobile"')
    barre = layout.index('<aside class="sidebar"')
    # le sélecteur `~` exige une sœur PRÉCÉDENTE, dans le même parent .app
    assert app < case < barre
    assert layout.count("<div", app, case) == 1
    debut = layout.index('<header class="topbar">')
    entete = layout[debut:layout.index("</header>", debut)]
    assert 'for="nav-mobile"' in entete
    assert 'class="nav-mobile-bouton"' in entete


def test_la_barre_du_haut_et_les_grilles_ne_debordent_pas() -> None:
    mobile = _blocs_media(CSS, 980)
    assert "flex-wrap: wrap" in _regle(mobile, ".topbar")
    assert "flex-wrap: wrap" in _regle(CSS, ".topbar-actions")
    assert "min-width: 0" in _regle(CSS, ".crumbs")
    assert "min-width: 0" in _regle(mobile, ".grid > *, .grid.two > *, .grid.three > *")
    assert "flex-wrap: wrap" in _regle(mobile, ".card-title-row")


def test_les_liens_publics_restent_visibles_sur_telephone() -> None:
    public = (WEB_ROOT / "templates" / "public_layout.html").read_text(encoding="utf-8")
    style = public[public.index("<style>"):public.index("</style>")]
    telephone = _blocs_media(style, 640)
    assert telephone, "le bloc téléphone de l'en-tête public a disparu"
    liens = _regle(telephone, ".pub-nav-links")
    assert "display: none" not in liens
    assert "overflow-x: auto" in liens, "les liens défilent au lieu d'élargir la page"


def test_le_temoin_lit_bien_les_blocs_media() -> None:
    css = "@media (max-width: 980px) {\n  .a { display: none; }\n  .b > * { min-width: 0; }\n}\n.a { color: red; }"
    bloc = _blocs_media(css, 980)
    assert _regle(bloc, ".a").strip() == "display: none;"
    assert "min-width: 0" in _regle(bloc, ".b > *")
    assert _blocs_media(css, 640) == ""
    assert _regle("  .x ~ .a { display: flex; }", ".a") == ""
