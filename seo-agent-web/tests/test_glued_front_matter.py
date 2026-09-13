# -*- coding: utf-8 -*-
"""Une valeur injectee COLLEE au nom d'une balise doit garder son espace initiale.

Mesure du 13/09/2026, jekyll, cycle complet des neuf stacks puis lecture de la PAGE SERVIE.
Charge d'ajouter l'attribut lang manquant, le correcteur a ecrit dans le front matter :

    html_attrs: 'lang="fr"'

Le gabarit du depot fait `<html{{ page.html_attrs }}>`, sans espace. La page servie portait donc
`<htmllang="fr">` : la balise s'appelle desormais `htmllang`, le document n'a plus d'element
`<html>`, et l'anomalie visee n'avait pas bouge — 2 avant, 2 apres, sous un verdict « ok ».
Une correction qui ABIME le balisage servi est pire qu'une anomalie laissee en place.

La page voisine avait ete corrigee juste : son ancienne valeur portait deja l'espace
(`' lang="francais"'`) et le modele l'a conservee. Seul l'AJOUT partait de rien.

Rien n'est devine ici : on lit les gabarits du depot et on ne traite que les clefs qu'ils
injectent effectivement collees.
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

COLLE = "<!doctype html>\n<html{{ page.html_attrs }}>\n  <head></head>\n</html>\n"
ESPACE = "<!doctype html>\n<html {{ page.html_attrs }}>\n  <head></head>\n</html>\n"
GO = "<!doctype html>\n<html{{ .Params.html_attrs }}>\n</html>\n"

AVANT = "---\nlayout: gauntlet\nhtml_attrs: ''\n---\n<p>corps</p>\n"
APRES = "---\nlayout: gauntlet\nhtml_attrs: 'lang=\"fr\"'\n---\n<p>corps</p>\n"


def test_le_gabarit_colle_est_reconnu():
    assert "html_attrs" in m._glued_template_keys([COLLE])


def test_un_gabarit_qui_a_deja_l_espace_ne_declare_rien():
    assert m._glued_template_keys([ESPACE]) == set()


def test_la_forme_go_est_reconnue_aussi():
    assert "html_attrs" in m._glued_template_keys([GO])


def test_le_cas_mesure_sur_jekyll():
    rendu, notes = m._space_glued_front_matter(APRES, AVANT, {"html_attrs"})
    assert "html_attrs: ' lang=\"fr\"'" in rendu
    assert len(notes) == 1


def test_une_valeur_qui_a_deja_son_espace_n_est_pas_touchee():
    deja = APRES.replace("'lang=", "' lang=")
    assert m._space_glued_front_matter(deja, AVANT, {"html_attrs"}) == (deja, [])


def test_une_valeur_que_ce_patch_n_a_pas_ecrite_est_laissee_tranquille():
    """On ne repare que ce que cette correction vient d'ecrire, jamais l'existant."""
    assert m._space_glued_front_matter(APRES, APRES, {"html_attrs"}) == (APRES, [])


def test_une_valeur_qui_n_est_pas_un_bloc_d_attributs_reste_intacte():
    avant = "---\ntitle: ''\n---\n"
    apres = "---\ntitle: 'Bonjour tout le monde'\n---\n"
    assert m._space_glued_front_matter(apres, avant, {"title"}) == (apres, [])


def test_une_clef_que_le_gabarit_n_injecte_pas_collee_reste_intacte():
    assert m._space_glued_front_matter(APRES, AVANT, {"autre_chose"}) == (APRES, [])


def test_hors_du_front_matter_on_ne_touche_a_rien():
    corps = "---\nlayout: x\n---\n<p>html_attrs: 'lang=\"fr\"'</p>\n"
    assert m._space_glued_front_matter(corps, "---\nlayout: x\n---\n", {"html_attrs"}) == (corps, [])


def test_un_fichier_sans_front_matter_n_est_pas_analyse():
    page = "<html lang=\"fr\"></html>\n"
    assert m._space_glued_front_matter(page, page, {"html_attrs"}) == (page, [])
