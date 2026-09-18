"""Troisieme piece du lot hreflang : retirer les annotations en trop d'une page.

`page_referenced_for_more_than_one_language_in_hreflang` se leve quand une page designe la MEME
URL sous plusieurs langues primaires. Une des deux a raison, et la CIBLE le dit — par la langue
qu'elle declare. Le crawl mesure donc la reponse et la porte dans la preuve ; le correcteur
n'arbitre rien, il retire ce qui est nomme.

Ce que ces tests fixent, c'est la PRECISION du retrait. Trois choses distinctes peuvent partager
une ligne de ce fichier — un meme code vers plusieurs URL, une meme URL sous plusieurs codes, et
les annotations des pages voisines du groupe — et se tromper de critere en supprimerait une saine.
Une suppression ne se rattrape pas au recrawl : la balise perdue n'est plus signalee par personne.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-multilang-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

S = "https://exemple.fr"

# La page /fr designe /en sous `fr` ET sous `en`. La cible /en declare `lang="en"` : c'est donc
# `fr` qui est en trop, et c'est lui que la preuve nomme.
ITEMS = [{"page": S + "/fr", "field": "fr", "value": S + "/en"}]


def _page(canonical: str, alternates: list[tuple[str, str]]) -> str:
    lignes = ['<!doctype html><html lang="fr"><head>',
              '  <link rel="canonical" href="%s" />' % canonical]
    lignes += ['  <link rel="alternate" hreflang="%s" href="%s" />' % (c, h)
               for c, h in alternates]
    return "\n".join(lignes) + "\n</head><body></body></html>\n"


COUPABLE = _page(S + "/fr", [("fr", S + "/en"), ("en", S + "/en"), ("de", S + "/de")])


def _annotations(contenu: str) -> list[tuple[str, str]]:
    return [(m.group("code"), m.group("href"))
            for m in app_module._HREFLANG_LIEN_RE.finditer(contenu)]


def test_le_code_en_trop_part_et_le_bon_reste() -> None:
    sortie, n = app_module._drop_hreflang_annotations(COUPABLE, ITEMS)
    assert n == 1, sortie
    assert _annotations(sortie) == [("en", S + "/en"), ("de", S + "/de")], _annotations(sortie)


def test_aucune_ligne_vide_ne_reste_derriere() -> None:
    """Un diff doit se lire « cette annotation a disparu », pas « le fichier a ete reformate »."""
    sortie, _ = app_module._drop_hreflang_annotations(COUPABLE, ITEMS)
    assert "\n\n" not in sortie, repr(sortie)
    assert sortie.split("\n") == [x for x in COUPABLE.split("\n")
                                 if 'hreflang="fr"' not in x], sortie


def test_le_MEME_code_vers_une_AUTRE_url_survit() -> None:
    """La preuve porte le couple code+URL parce que le code seul ne suffit pas a designer."""
    source = _page(S + "/fr", [("fr", S + "/en"), ("fr", S + "/fr-ca"), ("en", S + "/en")])
    sortie, n = app_module._drop_hreflang_annotations(source, ITEMS)
    assert n == 1, sortie
    assert ("fr", S + "/fr-ca") in _annotations(sortie), _annotations(sortie)


def test_la_MEME_url_sous_x_default_survit() -> None:
    """`x-default` vers la meme URL est legitime : l'URL seule ne suffit pas non plus."""
    source = _page(S + "/fr", [("fr", S + "/en"), ("x-default", S + "/en"), ("en", S + "/en")])
    sortie, n = app_module._drop_hreflang_annotations(source, ITEMS)
    assert n == 1, sortie
    assert ("x-default", S + "/en") in _annotations(sortie), _annotations(sortie)


def test_une_AUTRE_page_du_groupe_ne_perd_rien() -> None:
    """Elle porte une annotation identique ligne pour ligne — seul le canonical les distingue."""
    voisine = _page(S + "/de", [("fr", S + "/en"), ("en", S + "/en"), ("de", S + "/de")])
    sortie, n = app_module._drop_hreflang_annotations(voisine, ITEMS)
    assert n == 0 and sortie == voisine


def test_sans_canonical_litteral_on_s_abstient() -> None:
    """Un gabarit partage ne dit pas quelle page il rend : supprimer serait deviner."""
    gabarit = ('<head>\n  <link rel="canonical" href={`${base}${slug}`} />\n'
               '  <link rel="alternate" hreflang="fr" href="%s/en" />\n</head>\n' % S)
    sortie, n = app_module._drop_hreflang_annotations(gabarit, ITEMS)
    assert n == 0 and sortie == gabarit


def test_sans_preuve_aucune_suppression() -> None:
    sortie, n = app_module._drop_hreflang_annotations(COUPABLE, [])
    assert n == 0 and sortie == COUPABLE


def test_une_preuve_qui_ne_nomme_aucune_ligne_ne_touche_rien() -> None:
    """Le rapport peut avoir vieilli : la page a deja ete corrigee a la main."""
    deja = _page(S + "/fr", [("en", S + "/en"), ("de", S + "/de")])
    sortie, n = app_module._drop_hreflang_annotations(deja, ITEMS)
    assert n == 0 and sortie == deja


def test_la_famille_est_declaree_corrigeable() -> None:
    assert ("page_referenced_for_more_than_one_language_in_hreflang"
            in set(app_module._handled_issue_keys()))
