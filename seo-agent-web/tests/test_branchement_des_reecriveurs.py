# -*- coding: utf-8 -*-
"""Chaque reecriveur deterministe sort-il VRAIMENT du branchement ?

Trou laisse ouvert par d1da141, en toutes lettres : « Le meme trou existe probablement sur les
familles precedentes du lot. A verifier separement. » Mesure du 18/09/2026 : sur les trente
familles a reecriveur deterministe, NEUF seulement etaient citees dans un test qui appelle
`_prepare_issue_fix`. Pour les vingt et une autres, debrancher le reecriveur ne cassait rien.

Une famille peut etre declaree dans `_handled_issue_keys`, rangee dans son groupe de fichiers,
couverte par des tests qui appellent son reecriveur EN DIRECT — et ne rien recevoir au moment ou
le client clique. Les deux moities sont prouvees separement ; `_prepare_issue_fix` est le seul
endroit ou elles se rejoignent, et c'est celui que personne ne regardait.

POURQUOI PAR GROUPE ET NON PAR FAMILLE. Le branchement route sur les GROUPES (`_URL_PAIR_KEYS`,
`_MIXED_CONTENT_KEYS`…), pas sur les cles une a une. Un test par groupe couvre donc toutes ses
cles, presentes et futures : une famille ajoutee a un groupe deja teste herite de sa preuve, et
une famille ajoutee a un groupe NOUVEAU fait tomber la garde du bas de ce fichier.

CHAQUE GROUPE RECOIT LA PREUVE QU'IL ATTEND, et c'est la lecon de la mesure qui a precede ce
test. Une premiere version donnait la MEME preuve synthetique a toutes les familles et concluait
que dix-neuf etaient muettes ; `sitemap_http_urls_for_https` en faisait partie alors que son
branchement est sain — elle parle d'URL en `http://` et la preuve les donnait en `https://`.
Juger toutes les familles sur une forme unique, c'est le tri par nom deplace d'un cran.
"""

from __future__ import annotations

import ast
import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-branchement-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

import pytest  # noqa: E402

from backend import app as m  # noqa: E402

SITE = "exemple.fr"
S = "https://exemple.fr"
P = S + "/"

# Un depot plausible. Volontairement SANS sitemap en .ts : cette forme fait lire les types
# importes sur GitHub, donc un appel reseau que ce test n'a aucune raison de payer.
#
# `lib/seo.ts` est indispensable et non decoratif : `_X_DEFAULT_KEYS` ne repond QUE si le depot
# offre un fichier d'alternates partage, parce que x-default appartient au GROUPE et non a une
# page. Sans lui, la famille se tait — et l'avoir retire d'abord m'a fait croire a un trou.
CHEMINS = [
    "public/robots.txt", "public/sitemap.xml", "netlify.toml", "index.html",
    "src/pages/index.astro", "src/components/Header.astro", "app/layout.tsx", "app/page.tsx",
    "lib/seo.ts", "_layouts/default.html", "src/app.html",
]

# Ce que le crawl a MESURE sur la page. `_OG_URL_KEYS` ne lit pas la preuve du bloc : il relit
# `pages` et compare og:url au canonical, les deux valeurs etant deja connues. Une page sans ces
# deux champs ne produit aucune paire, donc aucun reecriveur — ce n'est pas un debranchement.
PAGES = [{"url": P, "status_code": 200, "canonical": P, "og_url": S + "/ancienne"}]


def _bloc(kind: str = "", items: "list[dict[str, str]] | None" = None,
          examples: "list[str] | None" = None, **extra) -> dict:
    b: dict = {"count": max(1, len(examples or [P])), "examples": list(examples or [P])}
    if kind:
        b["evidence"] = {"kind": kind, "items": list(items or [])}
    b.update(extra)
    return b


# --- la preuve que chaque groupe attend, lue dans sa branche de `_prepare_issue_fix` ----------

_PAIRES = [{"page": P, "from": S + "/a", "to": S + "/b"}]
_VALEURS = [{"page": P, "field": "fr", "value": S + "/en"}]

# (cles du groupe, preuve qu'il attend, nom du reecriveur qui DOIT en sortir)
#
# Le troisieme terme n'est pas un luxe. Une premiere version se contentait de « un reecriveur
# sort », et trois mutations y ont survecu : la cascade est une suite de `elif`, si bien que
# debrancher `_ASSET_REWRITE_KEYS` fait retomber la famille sur le cas generique, qui fournit
# `_rewrite_head_url_values`. Un reecriveur sortait donc bien — le MAUVAIS. C'est exactement le
# defaut que ce module porte en commentaire : l'ancien resolveur « mis-routed three families at
# once », et deux d'entre elles etaient cassees depuis leur mise en service.
FORMES: "dict[str, tuple[set[str], dict, str]]" = {
    # `_issue_url_pairs` alimente la cascade : sans paire, aucun reecriveur ne sort.
    "_URL_PAIR_KEYS": (m._URL_PAIR_KEYS, _bloc("url_pairs", _PAIRES),
                       "_rewrite_head_url_values"),
    "_ASSET_REWRITE_KEYS": (m._ASSET_REWRITE_KEYS, _bloc("url_pairs", _PAIRES),
                            "_rewrite_asset_srcs"),
    "_SITEMAP_REWRITE_KEYS": (m._SITEMAP_REWRITE_KEYS, _bloc("url_pairs", _PAIRES),
                              "_rewrite_sitemap_locs"),
    # Le conflit hreflang porte le code ET le cote ou il vit : `where=page` retire l'annotation
    # en trop DANS la page, `where=sitemap` reecrit le sitemap. Deux reecriveurs, un seul nom.
    "_SITEMAP_ALTERNATE_KEYS": (m._SITEMAP_ALTERNATE_KEYS, _bloc(
        "hreflang_pairs", [{"page": P, "code": "fr", "from": S + "/a", "to": S + "/b",
                            "where": "page"}]), "_drop_duplicate_hreflang"),
    # `_sitemap_https_pairs` construit la paire a partir des exemples : ils doivent etre en http.
    "_SITEMAP_HTTPS_KEYS": (m._SITEMAP_HTTPS_KEYS, _bloc(
        examples=["http://exemple.fr/a", "http://exemple.fr/b"]), "_rewrite_sitemap_locs"),
    # Ces trois-la n'ont besoin d'aucune preuve : le geste est le meme partout.
    "_MIXED_CONTENT_KEYS": (m._MIXED_CONTENT_KEYS, _bloc(), "_rewrite_http_to_https"),
    "_DOUBLE_SLASH_KEYS": (m._DOUBLE_SLASH_KEYS, _bloc(), "_rewrite_double_slash"),
    "_STRUCTURED_DATA_KEYS": (m._STRUCTURED_DATA_KEYS, _bloc(),
                              "_rewrite_jsonld_numeric_strings"),
    # La preuve nomme la page, le champ et la valeur.
    "_HREFLANG_DROP_KEYS": (m._HREFLANG_DROP_KEYS, _bloc("page_values", _VALEURS),
                            "_drop_hreflang_annotations"),
    "_HREFLANG_RETURN_KEYS": (m._HREFLANG_RETURN_KEYS, _bloc("page_values", _VALEURS),
                              "_add_reciprocal_hreflang"),
    "_ANCHOR_TEXT_KEYS": (m._ANCHOR_TEXT_KEYS, _bloc(
        "page_values", [{"page": P, "field": "/contact", "value": "Contactez-nous"}]),
        "_poser_aria_label_sur_liens_sans_ancre"),
    # Le geste porte sur les URL signalees, pas sur une preuve structuree.
    "_SITEMAP_DEDUPE_KEYS": (m._SITEMAP_DEDUPE_KEYS, _bloc(), "_dedupe_sitemap_locs"),
    "_SITEMAP_REMOVE_KEYS": (m._SITEMAP_REMOVE_KEYS, _bloc(), ""),
    "_ROBOTS_KEYS": (m._ROBOTS_KEYS, _bloc(
        "page_values", [{"page": P, "field": "sitemap", "value": S + "/sitemap.xml"}]), ""),
    "_CANONICAL_BROKEN_KEYS": (m._CANONICAL_BROKEN_KEYS, _bloc(
        "url_pairs", [{"page": P, "from": P, "to": S + "/absente"}]), ""),
    "_AI_POLICY_KEYS": (m._AI_POLICY_KEYS, _bloc(
        "page_values", [{"page": P, "field": "GPTBot", "value": "autorise"}]), ""),
    "_OG_URL_KEYS": (m._OG_URL_KEYS, _bloc("url_pairs", _PAIRES), "_rewrite_og_url"),
    "_X_DEFAULT_KEYS": (m._X_DEFAULT_KEYS, _bloc("page_values", _VALEURS), ""),
    "_SERVED_LANG_FIX_KEYS": (m._SERVED_LANG_FIX_KEYS, _bloc(
        "page_values", [{"page": P, "field": "lang", "value": "fr"}]), ""),
}


def _nom_du_reecriveur(rw) -> str:
    """Le reecriveur reellement branche, nomme.

    Une fonction directe se nomme elle-meme ; un `lambda raw, _p=…: _rewrite_x(raw, _p)` porte
    `_rewrite_x` dans les noms globaux de son code. C'est la seule facon de distinguer le bon
    reecriveur du reecriveur de repli, puisque tous deux sont des lambdas sans nom propre.
    """
    if getattr(rw, "__name__", "") not in ("", "<lambda>"):
        return rw.__name__
    appels = [n for n in getattr(rw, "__code__", None).co_names] if hasattr(rw, "__code__") else []
    return appels[0] if appels else ""

# Groupes dont la branche NE promet pas de reecriveur deterministe pour toute cle : ils sortent
# un refus ou une simple consigne selon ce que la preuve contient. On verifie alors qu'ils
# parlent — refus motive ou consigne — jamais qu'ils reecrivent.
SANS_REECRIVEUR_GARANTI = {"_CANONICAL_BROKEN_KEYS", "_AI_POLICY_KEYS", "_SERVED_LANG_FIX_KEYS",
                           "_X_DEFAULT_KEYS", "_ROBOTS_KEYS", "_SITEMAP_REMOVE_KEYS"}


def _prepare(cle: str, bloc: dict) -> dict:
    return m._prepare_issue_fix(
        issue_key=cle, issues={cle: dict(bloc)}, impacted=[P], all_paths=list(CHEMINS),
        site_name=SITE, owner="o", repo_name="r", branch="main", token="",
        pages=list(PAGES))


def _emises() -> "set[str]":
    """Les cles que le crawler leve vraiment, autrement que vides.

    Les groupes portent aussi les variantes d'indexabilite fabriquees en bloc par
    `_with_indexability_variants`, et le crawler n'en emet qu'une partie :
    `x_default_hreflang_missing_indexable` n'apparait NULLE PART dans le crawler. Une cle que
    personne ne leve ne peut pas etre muette en production — elle n'y arrive jamais.

    Meme piege que la cle nue `orphan_page`, rencontre le meme jour a l'autre bout de la mesure
    (voir `ops/couverture.py`). D'ou la reutilisation de sa lecture plutot qu'une deuxieme.
    """
    from ops import couverture
    crawler = (WEB_ROOT.parent / "skills" / "public" / "seo-autopilot" / "scripts"
               / "seo_audit.py")
    emissions = couverture._emissions(ast.parse(crawler.read_text(encoding="utf-8")))
    return {k for k, formes in emissions.items()
            if not all(couverture._muette(f) for f in formes)}


EMISES = _emises()


def _cas() -> "list[tuple[str, str, dict, str]]":
    out = []
    for groupe, (cles, bloc, attendu) in sorted(FORMES.items()):
        if groupe in SANS_REECRIVEUR_GARANTI:
            continue
        for cle in sorted(cles & EMISES):
            out.append((groupe, cle, bloc, attendu))
    return out


@pytest.mark.parametrize("groupe,cle,bloc,attendu", _cas(),
                         ids=lambda v: v if isinstance(v, str) else "")
def test_le_branchement_fournit_LE_BON_reecriveur(groupe: str, cle: str, bloc: dict,
                                                  attendu: str) -> None:
    """Debrancher la famille, ou la router ailleurs, doit se voir ici et nulle part ailleurs."""
    out = _prepare(cle, bloc)
    assert not str(out.get("refusal") or "").strip(), (cle, out.get("refusal"))
    rw = out.get("link_rewriter")
    assert callable(rw), (
        "%s (%s) ne recoit AUCUN reecriveur : la famille est declaree mais muette" % (cle, groupe))
    assert _nom_du_reecriveur(rw) == attendu, (
        "%s (%s) est branchee sur %s au lieu de %s : un mauvais routage repare autre chose, "
        "silencieusement" % (cle, groupe, _nom_du_reecriveur(rw), attendu))


@pytest.mark.parametrize("groupe", sorted(SANS_REECRIVEUR_GARANTI))
def test_un_groupe_sans_reecriveur_garanti_parle_quand_meme(groupe: str) -> None:
    """Il peut refuser ou guider, jamais rester sans voix : c'est le silence qu'on traque."""
    cles, bloc, _ = FORMES[groupe]
    for cle in sorted(cles & EMISES):
        out = _prepare(cle, bloc)
        parle = (callable(out.get("link_rewriter"))
                 or str(out.get("refusal") or "").strip()
                 or str(out.get("extra_hint") or "").strip()
                 or out.get("targets_override"))
        assert parle, "%s (%s) ne rend ni reecriveur, ni refus, ni consigne" % (cle, groupe)


# --- la garde : aucun groupe a reecriveur ne peut echapper a ce fichier -----------------------

def _groupes_qui_promettent_un_reecriveur() -> "set[str]":
    """Les groupes dont une branche de `_prepare_issue_fix` affecte `out["link_rewriter"]`.

    Lu dans le CODE, pas dans une liste tenue a la main : c'est la seule facon qu'un groupe
    ajoute demain ne passe pas au travers. La liste ecrite a la main aurait vieilli exactement
    comme les verdicts de `ops/reparabilite.py`, et au meme moment — juste apres une reussite.
    """
    source = (WEB_ROOT / "backend" / "app.py").read_text(encoding="utf-8")
    arbre = ast.parse(source)
    fonction = next(n for n in ast.walk(arbre)
                    if isinstance(n, ast.FunctionDef) and n.name == "_prepare_issue_fix")
    trouves: set[str] = set()
    for noeud in ast.walk(fonction):
        if not isinstance(noeud, ast.If):
            continue
        pose_un_reecriveur = any(
            isinstance(a, ast.Subscript) and isinstance(a.value, ast.Name) and a.value.id == "out"
            and isinstance(a.slice, ast.Constant) and a.slice.value == "link_rewriter"
            for n2 in noeud.body for a in (n2.targets if isinstance(n2, ast.Assign) else []))
        if not pose_un_reecriveur:
            # Le reecriveur peut etre pose dans un `if` imbrique (voir `_SITEMAP_ALTERNATE_KEYS`).
            pose_un_reecriveur = any(
                isinstance(a, ast.Subscript) and isinstance(a.value, ast.Name)
                and a.value.id == "out" and isinstance(a.slice, ast.Constant)
                and a.slice.value == "link_rewriter"
                for inner in ast.walk(ast.Module(body=noeud.body, type_ignores=[]))
                if isinstance(inner, ast.Assign) for a in inner.targets)
        if not pose_un_reecriveur:
            continue
        for nom in ast.walk(noeud.test):
            if isinstance(nom, ast.Name) and nom.id.isupper() and nom.id.startswith("_"):
                trouves.add(nom.id)
    return trouves


def test_aucun_groupe_a_reecriveur_n_echappe_a_ce_fichier() -> None:
    manquants = sorted(_groupes_qui_promettent_un_reecriveur() - set(FORMES))
    assert manquants == [], (
        "groupes qui posent un reecriveur sans test de branchement : %s" % manquants)


def test_chaque_forme_declaree_porte_sur_un_groupe_reel() -> None:
    """Une forme qui ne designe plus rien laisserait croire a une couverture qu'on n'a plus."""
    vides = sorted(g for g, (cles, _b, _r) in FORMES.items() if not cles)
    assert vides == [], "groupes devenus vides, encore listes ici : %s" % vides


def test_chaque_groupe_teste_a_au_moins_une_cle_que_le_crawler_LEVE() -> None:
    """Sinon le filtre des variantes fantomes viderait un groupe et le test passerait a vide.

    C'est le risque de toute restriction : elle protege d'un faux echec et peut, sans bruit,
    supprimer le test qu'elle filtre. Un groupe dont plus aucune cle n'est emise n'a plus rien a
    corriger et doit se voir, pas s'effacer.
    """
    muets = sorted(g for g, (cles, _b, _r) in FORMES.items() if not (cles & EMISES))
    assert muets == [], (
        "groupes dont le crawler ne leve plus aucune cle : %s" % muets)
