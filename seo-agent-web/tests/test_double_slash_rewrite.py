# -*- coding: utf-8 -*-
"""Les deux barres de tete d'une URL protocole-relative ne sont PAS une double barre de chemin.

`_rewrite_double_slash` existait depuis longtemps sans avoir jamais tourne sur une seule entree
reelle : le crawler ne pouvait pas declencher `double_slash_in_url`, puisqu'il collapse `/{2,}`
avant d'enregistrer quoi que ce soit. Le jour ou la famille s'est enfin declenchee (12/09/2026),
ce correcteur a produit sa premiere correction — et elle cassait le lien : `//hote//page`
devenait `/hote/page`, l'hote transforme en segment de chemin.

Du code jamais execute n'est pas du code qui marche. C'est ce que ce fichier tient.
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


def test_une_url_protocole_relative_garde_son_hote():
    """Le cas mesure sur le parcours."""
    out, n = m._rewrite_double_slash('<a href="//exemple.test//a-propos">x</a>')
    assert n == 1
    assert out == '<a href="//exemple.test/a-propos">x</a>'


def test_une_url_absolue_garde_son_schema():
    out, n = m._rewrite_double_slash('<a href="https://exemple.test/blog//article">x</a>')
    assert n == 1
    assert out == '<a href="https://exemple.test/blog/article">x</a>'


def test_un_chemin_racine_est_collapse():
    out, n = m._rewrite_double_slash('<img src="/img//logo.png" />')
    assert n == 1
    assert out == '<img src="/img/logo.png" />'


def test_une_url_saine_n_est_pas_touchee():
    page = '<a href="//exemple.test/a-propos">x</a><a href="https://exemple.test/b">y</a>'
    assert m._rewrite_double_slash(page) == (page, 0)


def test_rien_hors_d_une_valeur_de_lien_n_est_touche():
    """Le texte d'une page peut contenir `//` — un extrait de code, une date, une adresse."""
    page = '<p>Voir //exemple//, ou le chemin a//b dans le texte.</p>'
    assert m._rewrite_double_slash(page) == (page, 0)
