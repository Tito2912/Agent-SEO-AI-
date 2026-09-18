# -*- coding: utf-8 -*-
"""Ce que le correcteur d'ancres ecrit survit-il aux garde-fous qui tournent APRES lui ?

Tout patch, quelle que soit la famille, traverse trois garde-fous juste avant d'etre commite :
le plafond de longueur, l'interdiction de retrograder une URL en http, puis l'echappement du
delimiteur dans les valeurs ecrites. Ils s'appliquent au chemin DETERMINISTE comme au patch du
modele — verifie dans `_deep_patch_issue_files`, ou le reecriveur produit `patched_content` que
la boucle suivante fait passer par les trois.

C'EST LEUR INTERACTION QUI A DEJA COUTE. L'historique du projet le dit : les garde-fous se
marchent dessus, trois ont ete revises le jour meme de leur ecriture, et le plafond mesurait des
sequences d'echappement comme des caracteres. Un correcteur neuf qui ecrit une valeur TEXTUELLE
dans un attribut — ce que fait celui des ancres — arrive exactement sur ce terrain.

Trois formes valaient d'etre verifiees plutot que raisonnees :

  - une apostrophe francaise dans le nom, avec des guillemets SIMPLES clones du href voisin :
    c'est la forme qui a livre huit fichiers non parsables sur les neuf stacks ;
  - un nom LONG, parce que le plafond coupe les titres a 70 et qu'un h1 de cible depasse
    volontiers cette taille sans etre un titre ;
  - un nom qui contient une adresse en http, que l'interdiction de retrogradation pourrait
    vouloir reecrire.

Aucune ne doit etre touchee : ce qu'ecrit ce correcteur n'est ni un titre, ni une description,
ni une URL.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-gardes-ancres-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

S = "https://exemple.fr"


def _corriger_puis_garder(source: str, items: list[dict[str, str]]) -> str:
    """Le trajet reel d'un patch : le reecriveur, puis les trois garde-fous, dans cet ordre."""
    patche, n = m._poser_aria_label_sur_liens_sans_ancre(source, items)
    assert n > 0, "le reecriveur n'a rien ecrit : le test ne mesure pas ce qu'il croit"
    patche, _ = m._enforce_length_ceilings(patche, source)
    patche, _ = m._forbid_https_downgrade(patche, source)
    patche, _ = m._escape_quotes_in_written_values(patche, source)
    return patche


def test_une_apostrophe_francaise_survit_aux_guillemets_SIMPLES() -> None:
    """La forme qui a livre huit fichiers non parsables sur les neuf stacks.

    Le reecriveur clone le guillemet du href — ici simple — et echappe le nom lui-meme, si bien
    qu'aucune apostrophe brute n'atteint le garde-fou. Ce test tient que la chaine ne se fait pas
    echapper DEUX fois en route.
    """
    sortie = _corriger_puis_garder(
        "<a href='/atelier'><svg /></a>",
        [{"page": S + "/", "field": "/atelier", "value": "L'atelier & l'equipe"}])
    assert "aria-label='L&#x27;atelier &amp; l&#x27;equipe'" in sortie, sortie
    assert "&amp;#x27;" not in sortie, "la valeur a ete echappee une seconde fois"


def test_un_nom_LONG_n_est_pas_coupe_par_le_plafond_des_titres() -> None:
    """Le plafond coupe a 70 caracteres ; un h1 de cible depasse volontiers cette taille.

    Ce n'est ni un titre ni une description : rien ne doit etre trime. Le contraire donnerait une
    ancre tronquee au milieu d'un mot, et personne en aval ne mesure ce que ce correcteur ecrit.
    """
    nom = "Comment choisir son atelier de reparation velo dans une grande ville francaise"
    assert len(nom) > 70
    sortie = _corriger_puis_garder(
        '<a href="/guide"><svg /></a>',
        [{"page": S + "/", "field": "/guide", "value": nom}])
    assert 'aria-label="%s"' % nom in sortie, sortie


def test_une_adresse_http_dans_le_nom_n_est_pas_reecrite() -> None:
    """L'interdiction de retrogradation ne doit pas confondre un NOM avec un lien."""
    nom = "Notre ancien site http://exemple.fr"
    sortie = _corriger_puis_garder(
        '<a href="/histoire"><svg /></a>',
        [{"page": S + "/", "field": "/histoire", "value": nom}])
    assert 'aria-label="Notre ancien site http://exemple.fr"' in sortie, sortie


def test_le_href_et_le_contenu_du_lien_ressortent_intacts() -> None:
    """La promesse du correcteur — ne rien changer de visible — doit tenir APRES les garde-fous,
    pas seulement a sa sortie."""
    source = '<a href="/contact"><svg class="i" /></a>'
    sortie = _corriger_puis_garder(
        source, [{"page": S + "/", "field": "/contact", "value": "Contactez-nous"}])
    assert '<svg class="i" />' in sortie
    assert 'href="/contact"' in sortie


def test_un_fichier_de_gabarit_JS_voisin_n_est_pas_abime() -> None:
    """Le cas ou les garde-fous ont deja fait des degats : une propriete `title:` d'un objet JS.

    Le correcteur d'ancres n'y touche pas, et les garde-fous ne doivent pas s'en prendre a une
    ligne que ce patch n'a pas ecrite — c'est leur regle commune, et c'est elle qu'on verifie ici
    sur le trajet reel plutot que sur leur seul test unitaire.
    """
    source = ("export const metadata = { title: 'Un titre avec une apostrophe d'artiste' }\n"
              "<a href=\"/contact\"><svg /></a>\n")
    sortie = _corriger_puis_garder(
        source, [{"page": S + "/", "field": "/contact", "value": "Contactez-nous"}])
    assert "title: 'Un titre avec une apostrophe d'artiste' }" in sortie, sortie
