# -*- coding: utf-8 -*-
"""Canonicaliser un sitemap, c'est faire CONVERGER — donc creer des doublons si on n'y prend garde.

Mesure du 11/09/2026, sur le parcours d'obstacles. En ajoutant au banc la famille
`sitemap_non_canonical_page`, le correcteur a produit un sitemap qui :

* listait encore `canonical-relay`, alors qu'une autre paire du MEME lot la renvoyait ailleurs —
  les paires etaient appliquees independamment, sans suivre la chaine ;
* listait DEUX FOIS `/gauntlet/missing-h1`, parce que deux entrees distinctes convergeaient vers
  elle et que rien ne dedupliquait ;
* portait `<loc>http://…</loc>`, une retrogradation que le garde-fou n'a pas vue parce que son
  motif exigeait un guillemet ou une parenthese devant l'URL — or dans un sitemap elle vit entre
  `>` et `<`. Un controle qui decrit une FORME plutot que la chose.

Les trois sont couverts ici.
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


def _sitemap(*urls: str) -> str:
    lignes = "\n".join("  <url><loc>%s</loc></url>" % u for u in urls)
    return '<?xml version="1.0" encoding="UTF-8"?>\n<urlset>\n%s\n</urlset>\n' % lignes


def _locs(xml: str) -> list[str]:
    return [ln.split("<loc>")[1].split("</loc>")[0] for ln in xml.splitlines() if "<loc>" in ln]


def _paire(frm: str, to: str) -> dict[str, str]:
    return {"page": frm, "from": frm, "to": to}


def test_la_chaine_est_suivie_jusqu_au_bout():
    """`a -> b` et `b -> c` dans le meme lot : `a` doit atterrir sur `c`, pas sur `b`."""
    xml = _sitemap(H + "/a", H + "/b", H + "/c")
    out, n = m._rewrite_sitemap_locs(xml, [_paire(H + "/a", H + "/b"), _paire(H + "/b", H + "/c")])
    assert n == 2
    assert _locs(out) == [H + "/c"]


def test_les_entrees_qui_convergent_ne_sont_pas_dupliquees():
    xml = _sitemap(H + "/vieille", H + "/canonique")
    out, _n = m._rewrite_sitemap_locs(xml, [_paire(H + "/vieille", H + "/canonique")])
    assert _locs(out) == [H + "/canonique"]


def test_un_doublon_preexistant_n_est_pas_touche():
    """On ne deduplique que ce que CETTE reecriture a ecrit ; le reste ne la regarde pas."""
    xml = _sitemap(H + "/deja", H + "/deja", H + "/vieille")
    out, _n = m._rewrite_sitemap_locs(xml, [_paire(H + "/vieille", H + "/neuve")])
    assert _locs(out) == [H + "/deja", H + "/deja", H + "/neuve"]


def test_une_boucle_declaree_ne_fait_pas_tourner_le_suivi():
    xml = _sitemap(H + "/a")
    out, _n = m._rewrite_sitemap_locs(xml, [_paire(H + "/a", H + "/b"), _paire(H + "/b", H + "/a")])
    assert _locs(out) == [H + "/b"]


def test_le_garde_fou_voit_une_url_entre_balises():
    """Le cas mesure : `<loc>http://…</loc>`, sans guillemet ni parenthese devant."""
    avant = _sitemap(H + "/page")
    apres = _sitemap(H.replace("https", "http") + "/page")
    rendu, notes = m._forbid_https_downgrade(apres, avant)
    assert _locs(rendu) == [H + "/page"]
    assert notes


def test_un_tiers_http_only_reste_intact():
    """La restauration reste conditionnee a ce que la forme https ait existe AVANT."""
    avant = _sitemap("http://tiers.invalid/x")
    apres = _sitemap("http://tiers.invalid/x")
    rendu, notes = m._forbid_https_downgrade(apres, avant)
    assert _locs(rendu) == ["http://tiers.invalid/x"]
    assert not notes
