"""Seconde moitie du lot hreflang : poser la balise de retour sur la page CIBLE.

La famille signale la page SOURCE ; le fichier a corriger est la cible, celle qui ne renvoie pas.
La preuve du crawl porte les trois choses que le nom de l'anomalie ne dit pas — quelle page,
quel code, vers quelle URL. Ce test couvre ce que le correcteur en fait.

LE FICHIER DIT LUI-MEME QUELLE PAGE IL EST, par son canonical, et c'est necessaire : le
reecriveur ne recoit que du contenu, jamais un chemin. L'autre discriminant possible ne tient pas
— « le fichier contient une alternative vers P » est vrai de TOUTES les pages du groupe, pas
seulement de P. Un test le fixe, parce que s'y tromper poserait la balise sur la mauvaise page,
exactement comme le canonical d'une page pose sur trois pages saines le 15/09/2026.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-recip-write-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

S = "https://exemple.fr"
ITEMS = [{"page": S + "/en", "field": "fr", "value": S + "/fr"}]


def _page(canonical: str, alternates: list[tuple[str, str]]) -> str:
    lignes = ['<!doctype html><html lang="en"><head>',
              '  <link rel="canonical" href="%s" />' % canonical]
    lignes += ['  <link rel="alternate" hreflang="%s" href="%s" />' % (c, h)
               for c, h in alternates]
    return "\n".join(lignes) + "\n</head><body></body></html>\n"


CIBLE = _page(S + "/en", [("en", S + "/en"), ("de", S + "/de")])


def _codes(contenu: str) -> list[str]:
    return [m.group("code") for m in app_module._HREFLANG_LIEN_RE.finditer(contenu)]


def test_la_balise_de_retour_est_posee_sur_la_cible() -> None:
    sortie, n = app_module._add_reciprocal_hreflang(CIBLE, ITEMS)
    assert n == 1, n
    assert _codes(sortie) == ["en", "fr", "de"], _codes(sortie)
    assert 'hreflang="fr" href="%s/fr"' % S in sortie, sortie


def test_une_AUTRE_page_du_groupe_ne_recoit_rien() -> None:
    """Elle mentionne la cible sans etre la cible — c'est ce que le canonical tranche."""
    voisine = _page(S + "/de", [("de", S + "/de"), ("en", S + "/en")])
    sortie, n = app_module._add_reciprocal_hreflang(voisine, ITEMS)
    assert n == 0 and sortie == voisine


def test_sans_canonical_litteral_on_s_abstient() -> None:
    """Un gabarit partage ne dit pas quelle page il rend : poser la balise serait deviner."""
    gabarit = ('<head>\n  <link rel="canonical" href={`${base}${slug}`} />\n'
               '  <link rel="alternate" hreflang="en" href="%s/en" />\n</head>\n' % S)
    sortie, n = app_module._add_reciprocal_hreflang(gabarit, ITEMS)
    assert n == 0 and sortie == gabarit


def test_un_code_DEJA_present_n_est_pas_redouble() -> None:
    deja = _page(S + "/en", [("en", S + "/en"), ("fr", S + "/fr")])
    sortie, n = app_module._add_reciprocal_hreflang(deja, ITEMS)
    assert n == 0 and sortie == deja


def test_un_fichier_SANS_annotation_n_offre_rien_a_cloner() -> None:
    """On ne devine pas l'idiome : sans balise a cloner, la famille reste signalee."""
    nu = '<head>\n  <link rel="canonical" href="%s/en" />\n</head>\n' % S
    sortie, n = app_module._add_reciprocal_hreflang(nu, ITEMS)
    assert n == 0 and sortie == nu


def test_l_idiome_du_fichier_est_CLONE() -> None:
    """Guillemets simples et fermeture sans barre : la ligne ajoutee s'y conforme."""
    source = ("<head>\n  <link rel='canonical' href='%s/en'>\n"
              "  <link rel='alternate' hreflang='en' href='%s/en'>\n</head>\n" % (S, S))
    sortie, n = app_module._add_reciprocal_hreflang(source, ITEMS)
    assert n == 1, sortie
    ajoutee = next(x for x in sortie.split("\n") if "hreflang='fr'" in x)
    assert ajoutee.strip() == "<link rel='alternate' hreflang='fr' href='%s/fr'>" % S, ajoutee


def test_sans_preuve_aucune_ecriture() -> None:
    sortie, n = app_module._add_reciprocal_hreflang(CIBLE, [])
    assert n == 0 and sortie == CIBLE


def test_la_famille_est_declaree_corrigeable() -> None:
    assert "missing_reciprocal_hreflang" in set(app_module._handled_issue_keys())
