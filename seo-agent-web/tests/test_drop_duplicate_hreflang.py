# -*- coding: utf-8 -*-
"""Deux annotations hreflang pour le meme code : on RETIRE celles en trop, dans la page.

Jusqu'au 11/09/2026, cette moitie de `more_than_one_page_for_same_language_in_hreflang` n'avait
aucune preuve cote crawler : seul le conflit page-contre-sitemap etait decrit. Le correcteur
routait donc toute la famille vers `sitemap.xml`, ou il n'y avait rien a changer quand le doublon
vivait dans la tete de la page — « aucun patch » mesure sur huit stacks sur neuf, pour une
anomalie pourtant affichee au client.

Deux regles, toutes deux apprises a la mesure :

* **On supprime, on ne reecrit pas.** Reecrire l'URL en trop vers celle qu'on garde produirait
  deux annotations IDENTIQUES, que le controle compte toujours comme un defaut.
* **On raisonne sur l'annotation a GARDER, jamais sur celle a retirer.** Une famille passee avant
  dans le meme passage avait deja reecrit l'URL en trop, si bien que le `from` de la preuve ne
  designait plus rien. Le `to` — l'auto-reference de la page — est stable.
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


def _lien(code: str, href: str) -> str:
    return '  <link rel="alternate" hreflang="%s" href="%s" />\n' % (code, href)


def test_retire_le_doublon_et_garde_l_auto_reference():
    page = ('<head>\n  <link rel="canonical" href="https://exemple.test/fr/" />\n'
            + _lien("fr", H + "/fr/") + _lien("fr", H + "/autre/") + "</head>\n")
    out, n = m._drop_duplicate_hreflang(page, [_paire("fr", H + "/autre/", H + "/fr/")])
    assert n == 1
    assert "/autre/" not in out
    assert out.count('hreflang="fr"') == 1
    assert 'rel="canonical"' in out


def test_un_autre_code_n_est_jamais_touche():
    page = (_lien("fr", H + "/fr/") + _lien("fr", H + "/autre/")
            + _lien("en", H + "/en/") + _lien("x-default", H + "/fr/"))
    out, n = m._drop_duplicate_hreflang(page, [_paire("fr", H + "/autre/", H + "/fr/")])
    assert n == 1
    assert 'hreflang="en"' in out
    assert 'hreflang="x-default"' in out
    assert out.count('hreflang="fr"') == 1


def test_l_auto_reference_ecrite_en_relatif_est_reconnue():
    """Une page qui stocke ses alternates en relatif garde bien la bonne."""
    page = _lien("fr", "/fr/") + _lien("fr", H + "/autre/")
    out, n = m._drop_duplicate_hreflang(page, [_paire("fr", H + "/autre/", H + "/fr/")])
    assert n == 1
    assert '"/fr/"' in out
    assert "/autre/" not in out


def test_si_l_url_a_garder_a_bouge_on_garde_la_premiere_et_pas_zero():
    """Le cas qui a motive la regle : un autre correctif a deplace l'annotation entre-temps.

    Retirer les deux laisserait la page SANS auto-reference — un second defaut, pire que le
    premier. On en garde donc une, et une seule.
    """
    page = _lien("fr", H + "/deplacee/") + _lien("fr", H + "/autre/")
    out, n = m._drop_duplicate_hreflang(page, [_paire("fr", H + "/autre/", H + "/fr/")])
    assert n == 1
    assert out.count('hreflang="fr"') == 1


def test_une_seule_annotation_n_est_jamais_retiree():
    page = _lien("fr", H + "/autre/")
    assert m._drop_duplicate_hreflang(page, [_paire("fr", H + "/autre/", H + "/fr/")]) == (page, 0)


def test_sans_paire_le_contenu_est_rendu_intact():
    page = _lien("fr", H + "/autre/")
    assert m._drop_duplicate_hreflang(page, []) == (page, 0)
