# -*- coding: utf-8 -*-
"""Deux annotations hreflang pour le meme code : on RETIRE celle en trop, dans la page.

Jusqu'au 11/09/2026, cette moitie de `more_than_one_page_for_same_language_in_hreflang` n'avait
aucune preuve cote crawler : seul le conflit page-contre-sitemap etait decrit. Le correcteur
routait donc toute la famille vers `sitemap.xml`, ou il n'y avait rien a changer quand le doublon
vivait dans la tete de la page — « aucun patch » mesure sur huit stacks sur neuf, pour une
anomalie pourtant affichee au client.

On supprime plutot que de reecrire : reecrire l'URL en trop vers celle qu'on garde produirait
deux annotations IDENTIQUES, que le controle compte toujours comme un defaut.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402

H = "https://exemple.test"


def _paire(code: str, frm: str, to: str) -> dict[str, str]:
    return {"page": H + "/fr/", "code": code, "from": frm, "to": to, "where": "page"}


def test_retire_le_doublon_et_garde_l_auto_reference():
    page = (
        '<head>\n'
        '  <link rel="canonical" href="https://exemple.test/fr/" />\n'
        '  <link rel="alternate" hreflang="fr" href="https://exemple.test/fr/" />\n'
        '  <link rel="alternate" hreflang="fr" href="https://exemple.test/autre/" />\n'
        '</head>\n'
    )
    out, n = m._drop_duplicate_hreflang(page, [_paire("fr", H + "/autre/", H + "/fr/")])
    assert n == 1
    assert "/autre/" not in out
    assert out.count('hreflang="fr"') == 1
    assert 'rel="canonical"' in out


def test_ne_touche_pas_un_autre_code():
    """`en` pointe legitimement ailleurs : le retrait doit etre keye sur le CODE aussi."""
    page = (
        '  <link rel="alternate" hreflang="fr" href="https://exemple.test/autre/" />\n'
        '  <link rel="alternate" hreflang="en" href="https://exemple.test/autre/" />\n'
    )
    out, n = m._drop_duplicate_hreflang(page, [_paire("fr", H + "/autre/", H + "/fr/")])
    assert n == 1
    assert 'hreflang="en"' in out
    assert 'hreflang="fr"' not in out


def test_reconnait_l_ecriture_relative():
    """Une page qui stocke ses alternates en relatif doit etre traitee comme les autres."""
    page = '  <link rel="alternate" hreflang="fr" href="/autre/" />\n'
    out, n = m._drop_duplicate_hreflang(page, [_paire("fr", H + "/autre/", H + "/fr/")])
    assert n == 1
    assert "alternate" not in out


def test_sans_paire_le_contenu_est_rendu_intact():
    page = '  <link rel="alternate" hreflang="fr" href="/autre/" />\n'
    assert m._drop_duplicate_hreflang(page, []) == (page, 0)


def test_ne_retire_rien_si_l_url_ne_correspond_pas():
    page = '  <link rel="alternate" hreflang="fr" href="/encore-autre/" />\n'
    out, n = m._drop_duplicate_hreflang(page, [_paire("fr", H + "/autre/", H + "/fr/")])
    assert (out, n) == (page, 0)
