# -*- coding: utf-8 -*-
"""Créer le sitemap d'un site qui n'en a pas — le premier correcteur sans modèle.

CE QUI L'A MOTIVÉ. Sur 187 familles d'anomalies détectées, 73 avaient un correcteur. Parmi les
19 familles sitemap/robots, 8 étaient traitées — toutes celles où le fichier **existe mais dit
quelque chose de faux**. Aucune ne savait en créer un, parce que toute la boucle de correction
MODIFIE des fichiers existants.

Et un correcteur déjà écrit attendait celui-ci. `sitemap_not_in_robots` refuse en le nommant :
*« Aucun sitemap lisible à déclarer : c'est `sitemap_xml_not_found` qu'il faut traiter
d'abord. »*

DEUX VÉRIFICATIONS ONT CHANGÉ LE PLAN avant la première ligne de code. `robots_txt_not_found`
est **supprimé volontairement** par le crawler — « Ahrefs does not flag missing robots.txt as a
distinct issue » — donc un correcteur pour cette famille n'aurait jamais rien corrigé. Et les
neuf dépôts du banc rangent leur sitemap **exactement là où vit leur `robots.txt`**, ce qui
permet de transposer l'emplacement au lieu de le deviner par une table par stack.

CE CORRECTEUR N'APPELLE AUCUN MODÈLE, et c'est le premier. La liste des pages vient du crawl,
l'emplacement se lit dans le dépôt, le format est figé par une norme. Zéro quota consommé, zéro
hallucination possible.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-sitemap-"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-sitemap-runs-"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

# Les arbres réels des dépôts du banc, réduits à ce qui décide de l'emplacement.
ARBRES = [
    ("static-html", ["index.html", "robots.txt", "gauntlet/a.html"], "sitemap.xml"),
    ("jekyll", ["_config.yml", "robots.txt", "index.html"], "sitemap.xml"),
    ("next-pages", ["package.json", "public/robots.txt", "pages/a.js"], "public/sitemap.xml"),
    ("astro", ["package.json", "public/robots.txt", "src/pages/a.astro"], "public/sitemap.xml"),
    ("nuxt", ["package.json", "public/robots.txt", "pages/a.vue"], "public/sitemap.xml"),
    ("gatsby", ["package.json", "static/robots.txt", "src/pages/a.js"], "static/sitemap.xml"),
    ("sveltekit", ["package.json", "static/robots.txt", "src/routes/a/+page.svelte"],
     "static/sitemap.xml"),
    ("hugo", ["hugo.toml", "static/robots.txt", "content/a.md"], "static/sitemap.xml"),
]

PAGES = [
    {"url": "https://site.fr/", "status_code": 200},
    {"url": "https://site.fr/blog", "status_code": 200, "canonical": "https://site.fr/blog"},
]


# ── où poser le fichier ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("nom, arbre, attendu", ARBRES, ids=[n for n, _a, _x in ARBRES])
def test_l_emplacement_se_TRANSPOSE_depuis_le_robots(nom, arbre, attendu) -> None:
    """Mesuré sur les neuf dépôts : le sitemap vit toujours dans le dossier du `robots.txt`.
    Ce témoin-là survit aux conventions qu'aucune table écrite d'avance n'anticipe."""
    assert m._emplacement_du_sitemap(arbre) == attendu


def test_sans_robots_on_retombe_sur_un_dossier_statique_EXISTANT() -> None:
    assert m._emplacement_du_sitemap(
        ["package.json", "public/og.png", "src/pages/a.astro"]) == "public/sitemap.xml"


def test_un_dossier_statique_ABSENT_n_est_jamais_invente() -> None:
    """En créer un que le générateur ne sert pas produirait un fichier invisible — donc une
    correction qui se croit faite."""
    assert m._emplacement_du_sitemap(["package.json", "src/pages/a.astro"]) == ""


def test_le_repli_prefere_public_a_static() -> None:
    """L'ordre doit être déterministe : deux dossiers servis, un seul fichier à poser."""
    assert m._emplacement_du_sitemap(
        ["package.json", "public/a.png", "static/b.png"]) == "public/sitemap.xml"


# ── ce que le sitemap contient ───────────────────────────────────────────────────────────────

def test_une_page_NOINDEX_n_entre_pas_dans_le_sitemap() -> None:
    """Sinon on répare une anomalie en en créant une autre — que ce produit détecte lui-même
    sous le nom `sitemap_noindex_page`."""
    urls = m._urls_indexables_du_rapport(
        PAGES + [{"url": "https://site.fr/x", "status_code": 200, "meta_robots": "noindex"}])
    assert "https://site.fr/x" not in urls, urls


def test_une_page_NON_CANONIQUE_n_entre_pas_dans_le_sitemap() -> None:
    """Même raison : `sitemap_non_canonical_page` est une famille que nous signalons."""
    urls = m._urls_indexables_du_rapport(
        PAGES + [{"url": "https://site.fr/copie", "status_code": 200,
                  "canonical": "https://site.fr/blog"}])
    assert "https://site.fr/copie" not in urls, urls


def test_une_page_en_ERREUR_n_entre_pas_dans_le_sitemap() -> None:
    urls = m._urls_indexables_du_rapport(
        PAGES + [{"url": "https://site.fr/404", "status_code": 404},
                 {"url": "https://site.fr/boom", "status_code": 200, "error": "timeout"}])
    assert urls == ["https://site.fr/", "https://site.fr/blog"], urls


def test_le_x_robots_tag_compte_AUTANT_que_la_balise_meta() -> None:
    """Un `noindex` servi par un en-tête HTTP est aussi contraignant qu'une balise, et c'est la
    forme qu'on oublie."""
    urls = m._urls_indexables_du_rapport(
        [{"url": "https://site.fr/x", "status_code": 200, "x_robots_tag": "noindex"}])
    assert urls == [], urls


def test_le_sitemap_est_un_XML_VALIDE_et_minimal() -> None:
    """Pas de `lastmod` : le crawl ne connaît pas la date de modification des pages, et y
    écrire la date du jour serait faux pour toutes."""
    import xml.etree.ElementTree as ET

    xml = m._sitemap_xml(["https://site.fr/", "https://site.fr/blog"])
    racine = ET.fromstring(xml)
    ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    assert racine.tag == ns + "urlset"
    locs = [e.text for e in racine.iter(ns + "loc")]
    assert locs == ["https://site.fr/", "https://site.fr/blog"], locs
    assert "lastmod" not in xml and "priority" not in xml and "changefreq" not in xml


def test_une_URL_a_ESPERLUETTE_ne_casse_pas_le_XML() -> None:
    """Une URL à paramètres est légitime, et un `&` non échappé rend le fichier illisible —
    donc pire que pas de sitemap du tout."""
    import xml.etree.ElementTree as ET

    xml = m._sitemap_xml(["https://site.fr/r?a=1&b=2"])
    locs = [e.text for e in ET.fromstring(xml).iter(
        "{http://www.sitemaps.org/schemas/sitemap/0.9}loc")]
    assert locs == ["https://site.fr/r?a=1&b=2"], locs


# ── ce que le correcteur REFUSE ──────────────────────────────────────────────────────────────

@pytest.fixture()
def github(monkeypatch):
    ecrits: list[tuple[str, str]] = []

    def _put(path, **kw):
        import base64
        ecrits.append((path.split("/contents/", 1)[1],
                       base64.b64decode((kw.get("json_body") or {}).get("content", "")).decode()))
        return {"content": {"sha": "neuf"}}

    monkeypatch.setattr(m, "_github_api_put", _put)
    return ecrits


def _creer(arbre, pages, github):
    return m._deep_creer_le_sitemap(owner="o", repo_name="r", token="t", fix_branch="b",
                                    all_paths=arbre, pages=pages)


def test_le_fichier_est_ECRIT_au_bon_endroit(github) -> None:
    changes, notes = _creer(["index.html", "robots.txt"], PAGES, github)
    assert changes == ["sitemap.xml"], changes
    assert dict(github)["sitemap.xml"].startswith("<?xml")
    assert notes and "2 URL" in notes[0], notes


def test_un_sitemap_EXISTANT_n_est_jamais_ecrase(github) -> None:
    """Cette famille signale une absence. Si le fichier est là, l'arbre est périmé ou le
    fichier n'est pas servi — et écraser détruirait le travail du client."""
    changes, notes = _creer(["index.html", "robots.txt", "sitemap.xml"], PAGES, github)
    assert changes == [] and github == []
    assert "existe deja" in notes[0], notes


def test_sans_EMPLACEMENT_connu_on_n_ecrit_rien(github) -> None:
    changes, notes = _creer(["package.json", "src/pages/a.astro"], PAGES, github)
    assert changes == [] and github == []
    assert "impossible de savoir" in notes[0], notes


def test_un_crawl_SANS_page_indexable_ne_produit_pas_un_sitemap_vide(github) -> None:
    changes, notes = _creer(["index.html", "robots.txt"],
                            [{"url": "https://site.fr/x", "status_code": 404}], github)
    assert changes == [] and github == []
    assert "vide" in notes[0], notes


def test_au_dela_du_plafond_on_REFUSE_plutot_que_d_ecrire_a_moitie(github) -> None:
    """Au-delà, un sitemap doit être découpé en index. Un fichier qu'on ne sait pas découper
    vaut mieux refusé qu'écrit tronqué."""
    trop = [{"url": "https://site.fr/p%d" % i, "status_code": 200}
            for i in range(m._SITEMAP_URLS_MAX + 1)]
    changes, notes = _creer(["index.html", "robots.txt"], trop, github)
    assert changes == [] and github == []
    assert "decoupe en index" in notes[0], notes


# ── le piège que le verdict précédent portait ────────────────────────────────────────────────

GENERATEURS = [
    ("next-app", ["app/sitemap.ts", "public/robots.txt"], "", "app/sitemap.ts"),
    ("next-pages", ["pages/sitemap.xml.ts", "public/robots.txt"], "", "pages/sitemap.xml.ts"),
    ("sveltekit", ["src/routes/sitemap.xml/+server.ts", "static/robots.txt"], "",
     "src/routes/sitemap.xml/+server.ts"),
    ("astro-paquet", ["package.json", "public/robots.txt"],
     '{"dependencies": {"@astrojs/sitemap": "^3.0.0"}}', "@astrojs/sitemap"),
    ("gatsby-paquet", ["package.json", "static/robots.txt"],
     '{"dependencies": {"gatsby-plugin-sitemap": "^6.0.0"}}', "gatsby-plugin-sitemap"),
    ("hugo", ["hugo.toml", "static/robots.txt", "content/a.md"], "", "Hugo"),
]


@pytest.mark.parametrize("nom, arbre, pkg, attendu", GENERATEURS,
                         ids=[n for n, _a, _p, _x in GENERATEURS])
def test_un_depot_qui_ENGENDRE_son_sitemap_est_reconnu(nom, arbre, pkg, attendu) -> None:
    """Avant d'avoir un correcteur, cette famille portait le verdict `HORS_DEPOT` avec ce
    motif : « la plupart des stacks le GÉNÈRENT ». C'était juste, et c'est devenu la condition
    de sécurité du correcteur plutôt que la raison de ne pas l'écrire."""
    assert attendu in m._sitemap_deja_engendre(arbre, pkg)


@pytest.mark.parametrize("nom, arbre, pkg, attendu", GENERATEURS,
                         ids=[n for n, _a, _p, _x in GENERATEURS])
def test_aucun_fichier_n_est_ecrit_la_ou_un_generateur_existe(nom, arbre, pkg, attendu,
                                                              github) -> None:
    """Le cœur du refus : écrire un statique à côté d'un générateur ne répare rien. Selon la
    stack, le générateur gagne la route et notre fichier est mort-né, ou les deux se
    contredisent — et dans les deux cas la cause réelle reste entière."""
    changes, notes = m._deep_creer_le_sitemap(
        owner="o", repo_name="r", token="t", fix_branch="b",
        all_paths=arbre, pages=PAGES, package_json=pkg)
    assert changes == [] and github == [], github
    assert attendu in notes[0], notes


def test_le_refus_NOMME_ce_qu_il_a_vu() -> None:
    """Un refus qui ne dit pas ce qu'il a trouvé n'instruit personne : le client doit savoir
    qu'il a un générateur et que c'est LUI qu'il faut regarder."""
    _c, notes = m._deep_creer_le_sitemap(
        owner="o", repo_name="r", token="t", fix_branch="b",
        all_paths=["app/sitemap.ts", "public/robots.txt"], pages=PAGES)
    assert "app/sitemap.ts" in notes[0] and "generation" in notes[0].lower(), notes


def test_HUGO_est_le_piege_SILENCIEUX() -> None:
    """Ni fichier de route, ni dépendance déclarée : Hugo engendre son sitemap par défaut.
    C'est celui qu'une détection par fichiers seuls laisserait passer."""
    assert m._sitemap_deja_engendre(["hugo.toml", "content/a.md"], "")
    assert m._sitemap_deja_engendre(["config.toml", "content/a.md"], "")


def test_un_config_toml_IMBRIQUE_n_est_pas_du_Hugo() -> None:
    """Le témoin du précédent : `themes/x/config.toml` est le fichier d'un thème, pas la
    racine d'un site Hugo. Confondre les deux refuserait des dépôts corrigeables."""
    assert m._sitemap_deja_engendre(["themes/joli/config.toml", "index.html"], "") == ""


# ── l'intégration ────────────────────────────────────────────────────────────────────────────

def test_la_famille_est_DECLAREE_corrigeable() -> None:
    """Sans ça, l'écran des anomalies n'offre aucun bouton et le correcteur est inerte."""
    assert "sitemap_xml_not_found" in set(m._handled_issue_keys())


def test_le_correcteur_n_appelle_AUCUN_modele(monkeypatch, github) -> None:
    """Le premier de ce produit. Un appel de modèle ici consommerait un quota et rendrait
    possible une hallucination, pour un fichier dont chaque octet est connu d'avance."""
    def _interdit(**kw):
        raise AssertionError("un modèle a été appelé pour un fichier entièrement déterministe")

    monkeypatch.setattr(m, "_correction_ai_json", _interdit)
    changes, _notes = _creer(["index.html", "robots.txt"], PAGES, github)
    assert changes == ["sitemap.xml"]
