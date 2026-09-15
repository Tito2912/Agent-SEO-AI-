# -*- coding: utf-8 -*-
"""Un sitemap de site https qui liste des URL en clair : la destination ne se devine pas.

Premiere famille reprise dans la liste d'attente mesuree le 15/09/2026 — 104 familles montrees
au client sans correcteur. Celle-ci y etait alors qu'elle est parfaitement mecanique : le
crawler ne leve `sitemap_http_urls_for_https` QUE lorsque le site est servi en https, donc son
hote repond en https par definition. Passer `http://` a `https://` sur cet hote est une
reecriture de schema, pas un choix editorial.

La restriction a l'hote du site est ce qui empeche la regle de deborder : une URL en clair vers
un domaine TIERS reste intacte, parce que personne ne sait si ce tiers sert le https.

La reecriture elle-meme reutilise `_rewrite_sitemap_locs`, deja eprouve par deux familles
soeurs — seule la construction des paires est nouvelle, et c'est elle que ce fichier mesure.
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

SITE = "exemple.fr"


def _bloc(*urls: str) -> dict:
    return {"count": len(urls), "examples": list(urls)}


def test_les_url_de_l_hote_du_site_sont_appariees():
    paires = m._sitemap_https_pairs(_bloc("http://exemple.fr/a", "http://exemple.fr/b"), SITE)
    assert paires == [
        {"from": "http://exemple.fr/a", "to": "https://exemple.fr/a"},
        {"from": "http://exemple.fr/b", "to": "https://exemple.fr/b"},
    ]


def test_un_domaine_tiers_reste_intact():
    """On ne sait pas si ce tiers sert le https : le reecrire casserait le lien."""
    assert m._sitemap_https_pairs(_bloc("http://un-autre-site.fr/a"), SITE) == []


def test_un_sous_domaine_n_est_pas_l_hote_du_site():
    assert m._sitemap_https_pairs(_bloc("http://blog.exemple.fr/a"), SITE) == []


def test_une_url_deja_en_https_ne_produit_pas_de_paire():
    assert m._sitemap_https_pairs(_bloc("https://exemple.fr/a"), SITE) == []


def test_le_chemin_la_requete_et_l_ancre_sont_conserves():
    paires = m._sitemap_https_pairs(_bloc("http://exemple.fr/a/b?x=1&y=2"), SITE)
    assert paires[0]["to"] == "https://exemple.fr/a/b?x=1&y=2"


def test_les_doublons_ne_sont_apparies_qu_une_fois():
    paires = m._sitemap_https_pairs(_bloc("http://exemple.fr/a", "http://exemple.fr/a"), SITE)
    assert len(paires) == 1


def test_la_famille_est_declaree_corrigeable_et_proposee():
    assert "sitemap_http_urls_for_https" in m._handled_issue_keys()
    assert m._github_issue_auto_fixable("sitemap_http_urls_for_https")


def test_la_reecriture_ne_touche_que_les_loc_visees():
    paires = m._sitemap_https_pairs(_bloc("http://exemple.fr/a"), SITE)
    sitemap = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        "  <url><loc>http://exemple.fr/a</loc></url>\n"
        "  <url><loc>https://exemple.fr/b</loc></url>\n"
        "</urlset>\n"
    )
    rendu, n = m._rewrite_sitemap_locs(sitemap, paires)
    assert n == 1
    assert "<loc>https://exemple.fr/a</loc>" in rendu
    assert "<loc>https://exemple.fr/b</loc>" in rendu
    # L'espace de noms du document est une URL en http ET n'est pas un <loc> : y toucher
    # casserait le sitemap pour tous les moteurs.
    assert 'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"' in rendu


def test_le_correcteur_prepare_bien_un_reecriveur_sans_modele():
    prep = m._prepare_issue_fix(
        issue_key="sitemap_http_urls_for_https",
        issues={"sitemap_http_urls_for_https": _bloc("http://exemple.fr/a")},
        impacted=["http://exemple.fr/a"], all_paths=["public/sitemap.xml"], site_name=SITE,
        owner="o", repo_name="r", branch="main", token="t", model_override="", pages=[])
    assert prep["refusal"] is None
    assert prep["link_rewriter"] is not None
    assert prep["evidence"] == ["http://exemple.fr/a"]
