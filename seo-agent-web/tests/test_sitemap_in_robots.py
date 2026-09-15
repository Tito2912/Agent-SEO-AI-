# -*- coding: utf-8 -*-
"""`robots.txt` existe mais ne declare aucun sitemap : une ligne a ajouter, rien d'autre.

Deuxieme famille reprise dans la liste d'attente du 15/09/2026. Elle a deux pieges, et ils
comptent plus que la reparation elle-meme.

Le premier est de RANGEMENT : la cle s'appelle `sitemap_not_in_robots`, mais le fichier a editer
est `robots.txt`, pas le sitemap. Le tableau de ciblage classe donc la cle dans son propre
groupe — le module porte deja la trace de trois familles mal routees pour avoir ete classees sur
leur libelle.

Le second est le rayon d'action. Un `robots.txt` tient en quatre lignes dont trois peuvent
desindexer un site entier. On ajoute une ligne a la fin, on ne touche a aucun groupe
`User-agent` ni a aucune regle.

Quelle URL declarer ? Celle que le crawl a LUE. Sans directive dans robots.txt, le crawler n'a
pas d'autre source que son repli `/sitemap.xml` — c'est donc ce fichier-la qu'il a lu. Le seul
cas ambigu est ecarte par un refus : si `sitemap_xml_not_found` figure au rapport, il n'y a rien
a declarer et c'est l'autre famille qu'il faut traiter.
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
ATTENDU = "https://exemple.fr/sitemap.xml"
ROBOTS = "User-agent: *\nAllow: /\n"


def _prep(issues: dict, all_paths: list[str] | None = None) -> dict:
    return m._prepare_issue_fix(
        issue_key="sitemap_not_in_robots", issues=issues,
        impacted=["https://exemple.fr/robots.txt"],
        all_paths=all_paths or ["static/robots.txt", "public/sitemap.xml"], site_name=SITE,
        owner="o", repo_name="r", branch="main", token="t", model_override="", pages=[])


def test_la_ligne_est_ajoutee_a_la_fin():
    rendu, n = m._add_sitemap_to_robots(ROBOTS, ATTENDU)
    assert n == 1
    assert rendu.endswith("Sitemap: " + ATTENDU + "\n")


def test_les_regles_existantes_ne_bougent_pas():
    """Trois des quatre lignes d'un robots.txt peuvent desindexer un site."""
    robots = "User-agent: *\nDisallow: /admin/\nAllow: /\n"
    rendu, _ = m._add_sitemap_to_robots(robots, ATTENDU)
    assert rendu.startswith(robots.rstrip("\n"))
    assert "Disallow: /admin/" in rendu


def test_un_robots_qui_declare_deja_un_sitemap_n_est_pas_touche():
    deja = ROBOTS + "\nSitemap: https://exemple.fr/autre.xml\n"
    assert m._add_sitemap_to_robots(deja, ATTENDU) == (deja, 0)


def test_la_detection_du_doublon_ignore_la_casse_et_l_espace():
    deja = ROBOTS + "\n   sitemap :  https://exemple.fr/a.xml\n"
    assert m._add_sitemap_to_robots(deja, ATTENDU)[1] == 0


def test_un_fichier_vide_n_est_pas_complete():
    """Un robots.txt vide est un autre probleme : on n'invente pas de groupe `User-agent`."""
    assert m._add_sitemap_to_robots("", ATTENDU) == ("", 0)


def test_l_url_declaree_est_celle_que_le_crawl_a_lue():
    assert m._robots_sitemap_url({}, SITE) == ATTENDU


def test_un_sitemap_introuvable_fait_REFUSER_la_correction():
    prep = _prep({"sitemap_xml_not_found": {"count": 1, "examples": ["x"]}})
    assert prep["refusal"] and "sitemap_xml_not_found" in prep["refusal"]
    assert prep["link_rewriter"] is None


def test_la_famille_est_declaree_corrigeable_et_proposee():
    assert "sitemap_not_in_robots" in m._handled_issue_keys()
    assert m._github_issue_auto_fixable("sitemap_not_in_robots")


def test_la_cle_vise_robots_txt_et_NON_le_sitemap():
    """Le piege de rangement : la cle porte « sitemap » dans son nom."""
    groupes = {nom: cands for nom, cles, cands in m._issue_file_families()
               if "sitemap_not_in_robots" in cles}
    assert list(groupes) == ["robots"]
    assert groupes["robots"][0].endswith("robots.txt")


def test_le_repli_modele_est_autorise_pour_un_robots_ENGENDRE():
    """Next App Router produit `app/robots.ts` : aucun littéral a completer sur place."""
    prep = _prep({}, all_paths=["app/robots.ts"])
    assert prep["rewriter_ai_fallback"] is True
    assert ATTENDU in prep["extra_hint"]


# ── Le controle de FORME, paye au premier passage ────────────────────────────────────────────
# Mesure du 15/09/2026, premier passage de cette famille sur les neuf stacks. Le resolveur a
# rendu DEUX cibles sur cinq d'entre elles, et la ligne est partie dans le sitemap lui-meme :
# apres `</urlset>` pour les XML — du XML mal forme, verifie sur la preview de gatsby — et au
# milieu du TypeScript de `app/sitemap.ts` pour next-app, dont le DEPLOIEMENT A ECHOUE.
#
# Une reecriture qui accepte n'importe quel contenu n'est pas bornee, elle est seulement courte.

SITEMAP_XML = ('<?xml version="1.0" encoding="UTF-8"?>\n<urlset>\n'
               "  <url><loc>https://exemple.fr/</loc></url>\n</urlset>\n")
SITEMAP_TS = 'export default function sitemap() {\n  return [{ url: "https://exemple.fr/" }];\n}\n'


def test_un_sitemap_xml_n_est_JAMAIS_complete():
    assert m._add_sitemap_to_robots(SITEMAP_XML, ATTENDU) == (SITEMAP_XML, 0)


def test_un_generateur_typescript_n_est_JAMAIS_complete():
    """C'est ce cas precis qui a casse le build de next-app."""
    assert m._add_sitemap_to_robots(SITEMAP_TS, ATTENDU) == (SITEMAP_TS, 0)


def test_la_signature_reconnue_est_le_groupe_user_agent():
    """Un robots.txt en a un ; ni un XML ni un fichier de code n'en ont."""
    assert m._add_sitemap_to_robots("User-Agent: Googlebot\nDisallow:\n", ATTENDU)[1] == 1


def test_le_ciblage_ne_propose_que_des_fichiers_robots():
    prep = _prep({}, all_paths=["app/sitemap.ts", "public/robots.txt", "public/sitemap.xml"])
    assert prep["targets_override"] == ["public/robots.txt"]


def test_un_depot_sans_fichier_robots_fait_REFUSER():
    prep = _prep({}, all_paths=["public/sitemap.xml", "index.html"])
    assert prep["refusal"] and "robots" in prep["refusal"]
