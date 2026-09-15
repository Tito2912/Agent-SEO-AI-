# -*- coding: utf-8 -*-
"""Un canonical qui designe une page disparue — et la ligne a ne pas franchir.

Troisieme famille reprise dans la liste d'attente du 15/09/2026. Pointer un canonical vers une
page qui n'existe plus revient a n'en declarer aucun : le signal part dans le vide. La seule
valeur qu'on puisse ecrire sans rien deviner est l'URL de la page ELLE-MEME — un canonical
auto-referent, qui se trouve etre aussi la recommandation par defaut. On ne cherche pas a
retrouver la cible voulue : elle n'est pas mesurable.

Tout l'interet de cette famille est ailleurs, dans le TRI PAR STATUT.

  404 / 410 : la cible a disparu, c'est etabli. On corrige.
  401 / 403 : la cible est peut-etre simplement protegee contre les robots.
  429       : la cible a refuse ce crawl-la, pas les suivants.
  5xx       : la cible peut etre debout dans dix minutes.

Dans les trois derniers cas le canonical est vraisemblablement JUSTE, et le reecrire casserait
une intention volontaire au motif d'une indisponibilite passagere. Le correcteur refuse, en le
disant. Une correction qui ne sait pas distinguer une absence d'une panne ne corrige pas : elle
parie.
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

PAGE = "https://x.fr/gauntlet/canonical-404"
PAGES = [
    {"url": PAGE, "status_code": 200},
    {"url": "https://x.fr/disparue", "status_code": 404},
    {"url": "https://x.fr/partie", "status_code": 410},
    {"url": "https://x.fr/protegee", "status_code": 403},
    {"url": "https://x.fr/limitee", "status_code": 429},
    {"url": "https://x.fr/en-panne", "status_code": 503},
]


def _bloc(cible: str) -> dict:
    return {"count": 1, "examples": ["%s -> %s" % (PAGE, cible)]}


def _prep(cle: str, cible: str) -> dict:
    return m._prepare_issue_fix(
        issue_key=cle, issues={cle: _bloc(cible)}, impacted=[PAGE], all_paths=["index.html"],
        site_name="x.fr", owner="o", repo_name="r", branch="main", token="t",
        model_override="", pages=PAGES)


def test_une_cible_en_404_donne_un_canonical_auto_referent():
    paires, _ = m._canonical_self_pairs(_bloc("https://x.fr/disparue"), PAGES)
    assert paires == [{"page": PAGE, "from": "https://x.fr/disparue", "to": PAGE}]


def test_une_cible_en_410_aussi():
    """410 dit « partie volontairement » : c'est encore plus definitif qu'un 404."""
    paires, _ = m._canonical_self_pairs(_bloc("https://x.fr/partie"), PAGES)
    assert paires and paires[0]["to"] == PAGE


def test_une_cible_protegee_est_REFUSEE():
    prep = _prep("canonical_points_to_4xx", "https://x.fr/protegee")
    assert prep["refusal"] and "403" in prep["refusal"]
    assert prep["link_rewriter"] is None


def test_une_cible_limitee_est_REFUSEE():
    prep = _prep("canonical_points_to_4xx", "https://x.fr/limitee")
    assert prep["refusal"] and "429" in prep["refusal"]


def test_une_panne_serveur_est_REFUSEE():
    """Un 5xx peut etre debout dans dix minutes : son canonical est probablement juste."""
    prep = _prep("canonical_points_to_5xx", "https://x.fr/en-panne")
    assert prep["refusal"] and "503" in prep["refusal"]
    assert prep["link_rewriter"] is None


def test_une_cible_jamais_crawlee_est_REFUSEE():
    """Sans statut mesure, on ne sait rien — et on ne parie pas."""
    prep = _prep("canonical_points_to_4xx", "https://x.fr/jamais-vue")
    assert prep["refusal"] and "inconnu" in prep["refusal"]


def test_la_famille_est_declaree_corrigeable_et_proposee():
    for cle in ("canonical_points_to_4xx", "canonical_points_to_5xx"):
        assert cle in m._handled_issue_keys()
        assert m._github_issue_auto_fixable(cle)


def test_la_cle_vise_les_fichiers_du_canonical():
    groupes = [nom for nom, cles, _ in m._issue_file_families()
               if "canonical_points_to_4xx" in cles]
    assert groupes == ["canonical"]


def test_la_reecriture_ne_touche_que_la_balise_canonical():
    paires, _ = m._canonical_self_pairs(_bloc("https://x.fr/disparue"), PAGES)
    page = ('<link rel="canonical" href="https://x.fr/disparue" />\n'
            '<meta property="og:url" content="https://x.fr/disparue" />\n'
            '<p><a href="https://x.fr/disparue">un lien du corps</a></p>\n')
    rendu, n = m._rewrite_head_url_values(page, paires)
    assert n == 1
    assert 'rel="canonical" href="%s"' % PAGE in rendu
    assert 'og:url" content="https://x.fr/disparue"' in rendu, "og:url est une autre famille"
    assert 'href="https://x.fr/disparue">un lien' in rendu, "un lien du corps n'est pas un canonical"


def test_la_consigne_dit_de_ne_pas_inventer_de_destination():
    prep = _prep("canonical_points_to_4xx", "https://x.fr/disparue")
    assert "elle-meme" in prep["extra_hint"].lower().replace("ê", "e")
    assert PAGE in prep["extra_hint"]


# ── Le ciblage, paye au premier passage ──────────────────────────────────────────────────────
# Mesure du 15/09/2026, premier passage sur les neuf stacks. Pour UNE page signalee, le
# resolveur a rendu CINQ fichiers, et le repli IA a fait le reste : le modele a pris l'unique
# paire qu'on lui montrait et a ecrit l'URL de cette page sur le canonical de TROIS PAGES SAINES.
# Elles se seraient desindexees a son profit. Le sitemap y est passe aussi.
#
# C'est le mode d'echec que `_og_fallback_allowed` documente depuis le 10/09 : la bonne valeur
# est PAR PAGE, et rien dans un fichier ne dit au modele a quelle page ce fichier correspond.


def test_la_famille_cible_la_page_signalee_et_pas_le_site():
    assert "canonical_points_to_4xx" in m._PER_PAGE_CONTENT_KEYS
    assert "canonical_points_to_5xx" in m._PER_PAGE_CONTENT_KEYS


def test_AUCUN_repli_modele_meme_avec_une_seule_paire():
    """Montrer une valeur PAR PAGE a un modele qui edite un autre fichier, c'est lui demander
    d'y poser le canonical d'une page voisine."""
    prep = _prep("canonical_points_to_4xx", "https://x.fr/disparue")
    assert prep["rewriter_ai_fallback"] is False
    assert prep["link_rewriter"] is not None


def test_un_canonical_construit_par_du_code_reste_non_corrige():
    """Le prix assume du refus de repli : sans litteral, on ne touche a rien."""
    paires, _ = m._canonical_self_pairs(_bloc("https://x.fr/disparue"), PAGES)
    code = 'export const metadata = { alternates: { canonical: getSiteUrl(path) } };\n'
    assert m._rewrite_head_url_values(code, paires) == (code, 0)
