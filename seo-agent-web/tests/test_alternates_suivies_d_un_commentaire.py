# -*- coding: utf-8 -*-
"""Une entree d'alternates suivie d'un commentaire doit etre reecrite comme les autres.

Defaut trouve le 18/09/2026 en instruisant une COLLISION DE NOM, pas en cherchant un bug de
reecriture. `_QUOTED_VALUE_RE` etait defini deux fois dans app.py. La premiere version, sans
lookahead, etait ecrite juste sous `_JS_LANGUAGES_RE` — le bloc d'alternates qu'elle sert. La
seconde, qui exige une ponctuation (`,` `}` `]` `;`) ou une fin de ligne juste apres la valeur, la
remplacait avant tout usage.

Le bloc JS tournait donc depuis toujours avec une regex ecrite pour un autre besoin. Mesure sur
neuf formes reelles d'alternates : les deux voient la meme chose partout, SAUF quand la valeur est
suivie d'un commentaire de fin de ligne. Cette entree-la n'etait jamais reecrite, et rien ne le
disait — la correction repartait en annoncant le nombre de remplacements qu'elle avait faits,
sans savoir ce qu'elle avait manque.

Elargir ne peut rien casser : le remplacement est conditionne par la preuve du crawl. On trouve
donc PLUS d'occurrences des URL deja ciblees, jamais d'autres valeurs. Et cette regex vit dans le
CORRECTEUR : la parite Ahrefs, qui porte sur la detection, n'est pas concernee.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-alternates-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

S = "https://exemple.fr"
PAIRES = [{"from": S + "/en-US", "to": S + "/en"}]

AVEC_COMMENTAIRE = """export const metadata = {
  alternates: {
    languages: {
      fr: '%s/fr',        // la version francaise
      en: '%s/en-US'      // the english one
    }
  }
}
""" % (S, S)

SANS_COMMENTAIRE = """export const metadata = {
  alternates: {
    languages: {
      fr: '%s/fr',
      en: '%s/en-US'
    }
  }
}
""" % (S, S)


def test_une_entree_suivie_d_un_COMMENTAIRE_est_reecrite() -> None:
    """C'est le cas qui etait perdu : la valeur n'est suivie ni d'une virgule ni d'une accolade."""
    sortie, n = m._rewrite_head_url_values(AVEC_COMMENTAIRE, PAIRES)
    assert n == 1, sortie
    assert "'%s/en'" % S in sortie, sortie
    assert "/en-US" not in sortie, sortie


def test_le_commentaire_lui_meme_n_est_pas_touche() -> None:
    """Elargir la regex ne doit pas deborder sur ce qui suit la valeur."""
    sortie, _ = m._rewrite_head_url_values(AVEC_COMMENTAIRE, PAIRES)
    assert "// the english one" in sortie
    assert "// la version francaise" in sortie


def test_la_forme_SANS_commentaire_marche_toujours() -> None:
    """Le cas qui fonctionnait deja ne doit pas changer en cours de route."""
    sortie, n = m._rewrite_head_url_values(SANS_COMMENTAIRE, PAIRES)
    assert n == 1, sortie
    assert "'%s/en'" % S in sortie


def test_une_entree_VOISINE_non_signalee_reste_intacte() -> None:
    """La garde qui compte vraiment : on ne remplace que ce que la preuve nomme."""
    sortie, _ = m._rewrite_head_url_values(AVEC_COMMENTAIRE, PAIRES)
    assert "fr: '%s/fr'" % S in sortie, sortie


def test_les_deux_regex_ne_portent_plus_le_meme_nom() -> None:
    """Le defaut d'origine etait la collision, pas la regex : c'est elle qu'on empeche de revenir.

    `test_aucune_collision_de_nom` le verifie pour tout le module ; ici on nomme les deux
    intentions, pour qu'une fusion distraite ne les reunisse pas sous un seul nom.
    """
    assert m._JS_PROP_VALUE_RE.pattern != m._QUOTED_VALUE_RE.pattern
    # La permissive voit une valeur suivie d'un commentaire ; l'autre, non. C'est toute la
    # difference, et c'est pour cela qu'elles ne sont pas interchangeables.
    ligne = "  en: 'https://exemple.fr/en'  // commentaire"
    assert m._JS_PROP_VALUE_RE.search(ligne)
    assert not m._QUOTED_VALUE_RE.search(ligne)
