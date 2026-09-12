# -*- coding: utf-8 -*-
"""Une correction n'a pas le droit de RETIRER une balise que la page portait deja.

Mesure du 12/09/2026, cycle complet sur les neuf stacks. La reparation de `missing_title` — une
reecriture complete du fichier par le modele — a emporte `twitter:card` et `og:url`, et duplique
`og:description`. Une anomalie reparee, trois creees. Le recrawl des previews a vu
`twitter_card_missing` remonter sur quatre stacks et `twitter_card_incomplete` passer de 3 a 5
sur next-app ; le verdict par famille, lui, annoncait « ok » partout.

Ce garde-fou ne devine rien : il repose la balise que l'ancien contenu portait, a l'octet pres.
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

AVANT = (
    "<head>\n"
    '  <meta name="twitter:card" content="summary_large_image" />\n'
    '  <meta property="og:url" content="https://exemple.test/p" />\n'
    '  <meta name="description" content="une description" />\n'
    "</head>\n"
)


def test_les_balises_emportees_sont_rendues_a_l_octet_pres():
    apres = '<head>\n  <meta name="description" content="une description" />\n</head>\n'
    rendu, notes = m._keep_head_meta_tags(apres, AVANT)
    assert '<meta name="twitter:card" content="summary_large_image" />' in rendu
    assert '<meta property="og:url" content="https://exemple.test/p" />' in rendu
    assert len(notes) == 2


def test_une_page_intacte_n_est_pas_touchee():
    assert m._keep_head_meta_tags(AVANT, AVANT) == (AVANT, [])


def test_une_valeur_changee_n_est_pas_une_perte():
    """Corriger une description, c'est changer sa valeur — la balise est toujours la."""
    apres = AVANT.replace("une description", "une bien meilleure description")
    assert m._keep_head_meta_tags(apres, AVANT) == (apres, [])


def test_un_doublon_retire_n_est_pas_une_perte():
    """`multiple_meta_description_tags` retire la meta EN TROP : l'identite subsiste."""
    avant = AVANT.replace("</head>", '  <meta name="description" content="doublon" />\n</head>')
    rendu, notes = m._keep_head_meta_tags(AVANT, avant)
    assert (rendu, notes) == (AVANT, [])


def test_le_retrait_de_robots_reste_permis():
    """C'est la seule balise dont la suppression peut etre la correction demandee."""
    avant = '<head><meta name="robots" content="noindex, follow"></head>'
    apres = "<head></head>"
    assert m._keep_head_meta_tags(apres, avant) == (apres, [])


def test_sans_endroit_ou_la_reposer_on_le_DIT_plutot_que_de_bricoler():
    rendu, notes = m._keep_head_meta_tags("export const metadata = {};", AVANT)
    assert rendu == "export const metadata = {};"
    assert notes and "non restituee" in notes[0]


def test_le_jsx_est_couvert_comme_le_balisage():
    """Les stacks a composants ecrivent les memes balises, sans `</head>`."""
    avant = '<>\n  <meta name="twitter:card" content="summary" />\n  <meta name="x" content="y" />\n</>'
    apres = '<>\n  <meta name="x" content="y" />\n</>'
    rendu, notes = m._keep_head_meta_tags(apres, avant)
    assert '<meta name="twitter:card" content="summary" />' in rendu
    assert len(notes) == 1
