# -*- coding: utf-8 -*-
"""Les écrans de l'application restent sobres : ni graisse 800/900, ni émoji en couleur, ni
pastille violette, ni étiquette encadrée dans le menu.

Demande du propriétaire, 29/09/2026, après la refonte de la page d'accueil (« on dirait un site
pour enfant », « les badges et les icônes sont moches ») : la même relecture pour les écrans
connectés. Relevé avant correction : titres et grands chiffres en graisse 800/900 (Segoe UI
Black sous Windows), pastilles pleines — « notice » en violet, une couleur étrangère à la marque —
jusque dans le menu (« Pro+ », « Solo+ »), et des émojis en couleur (❌ ⚡ ⏳ 🔗 🔍).

Les signes de statut monochromes (✓ ✗ ⚠) restent permis : ce sont des caractères de texte,
utilisés dans des cases à cocher et des messages, pas des pictogrammes.
"""
from __future__ import annotations

import re
from pathlib import Path

WEB = Path(__file__).resolve().parents[1]
FICHIERS = sorted((WEB / "templates").glob("*.html")) + [WEB / "static" / "styles.css"]
# Émojis à présentation colorée par défaut : blocs pictographiques et les quelques caractères
# isolés qu'on a trouvés dans ces gabarits.
EMOJI_COULEUR = re.compile("[\U0001F300-\U0001FAFF❌⚡⏳✅✨⭐]")


def _lignes(motif: re.Pattern) -> list[str]:
    return ["%s:%d" % (f.name, n) for f in FICHIERS
            for n, l in enumerate(f.read_text(encoding="utf-8").splitlines(), 1) if motif.search(l)]


def test_aucune_graisse_800_ni_900() -> None:
    assert not _lignes(re.compile(r"font-weight:\s*(800|900)\b")), _lignes(re.compile(r"font-weight:\s*(800|900)\b"))


def test_aucun_emoji_en_couleur() -> None:
    assert not _lignes(EMOJI_COULEUR), _lignes(EMOJI_COULEUR)


def test_la_pastille_notice_n_est_plus_violette() -> None:
    css = (WEB / "static" / "styles.css").read_text(encoding="utf-8")
    regle = re.search(r"\.badge\.notice\s*\{([^}]*)\}", css).group(1)
    assert "129,140,248" not in regle.replace(" ", "") and "a5b4fc" not in regle.lower(), regle


def test_les_pastilles_n_ont_plus_de_fond_plein() -> None:
    css = (WEB / "static" / "styles.css").read_text(encoding="utf-8")
    for variante in ("ok", "warning", "error"):
        regle = re.search(r"\.badge\.%s\s*\{([^}]*)\}" % variante, css).group(1)
        assert "background" not in regle, (variante, regle)


def test_le_menu_n_encadre_plus_l_offre_requise() -> None:
    menu = (WEB / "templates" / "layout.html").read_text(encoding="utf-8")
    assert 'class="badge' not in menu
    assert menu.count('<span class="nav-plan">') == 3
