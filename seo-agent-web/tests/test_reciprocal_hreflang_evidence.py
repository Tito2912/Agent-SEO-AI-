"""La moitie manquante de `missing_reciprocal_hreflang` : dire QUELLE page editer.

Mesure du 18/09/2026 : les 19 familles qui portent une preuve attachee sont EXACTEMENT les 19 qui
ont un correcteur. Ce n'est pas une coincidence — la preuve existe parce qu'un correcteur en avait
besoin. Ajouter une famille est donc toujours DEUX chantiers : le crawler doit d'abord dire quoi
reparer. Celui-ci est le premier des deux.

CE QUE LA PREUVE DOIT PORTER, et pourquoi rien d'autre ne suffit : la famille signale la page
SOURCE — celle qui designe une alternative sans retour — mais le fichier a corriger est la CIBLE,
celle qui ne renvoie pas. Rien dans le nom de l'anomalie ne le dit. Un correcteur sans cette
preuve editerait la page signalee, c'est-a-dire celle qui n'a rien a se reprocher.

UNE SEULE DES DEUX CAUSES EST INSTRUMENTEE. La detection en a deux :
  (A) une page du sitemap designe une alternative HORS sitemap — c'est une question
      d'appartenance au sitemap, qu'aucune balise de retour ne resout ;
  (B) P designe T et T ne designe pas P en retour — la balise de retour sur T est la correction.
On ne recueille que pour (B). La detection elle-meme n'est pas touchee : elle est reglee finement
pour la parite Ahrefs (elevenlabs 8, tradingview 2, avis-invest 0) et un correcteur n'a aucune
raison de la deplacer.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SRC = (Path(__file__).resolve().parents[2]
        / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")
_spec = importlib.util.spec_from_file_location("seo_audit_recip", _SRC)
assert _spec and _spec.loader
audit = importlib.util.module_from_spec(_spec)
sys.modules["seo_audit_recip"] = audit
_spec.loader.exec_module(audit)

S = "https://exemple.fr"


def _page(chemin: str, hreflang: dict[str, str], lang: str = "fr") -> object:
    p = audit.PageData(url=S + chemin)
    p.final_url = S + chemin
    p.status_code = 200
    p.content_type = "text/html; charset=utf-8"
    p.lang = lang
    p.hreflang = dict(hreflang)
    p.canonical = S + chemin
    return p


def _issues(pages: list[object]) -> dict:
    # La detection est gardee par la presence d'un sitemap : sans lui, tout le bloc est saute.
    return audit._score_issues(
        pages, sitemap_urls={p.final_url for p in pages}, base_url=S)


def _preuve(issues: dict) -> list[dict]:
    bloc = issues.get("missing_reciprocal_hreflang") or {}
    ev = bloc.get("evidence") or {}
    return list(ev.get("items") or []) if ev.get("kind") == "page_values" else []


def test_la_preuve_nomme_la_page_a_EDITER_pas_celle_signalee() -> None:
    """`/fr` designe `/en`, `/en` ne renvoie pas. Le correctif va dans `/en`."""
    pages = [_page("/fr", {"fr": S + "/fr", "en": S + "/en"}),
             _page("/en", {"en": S + "/en", "de": S + "/de"}, "en"),
             _page("/de", {"de": S + "/de", "en": S + "/en"}, "de")]
    issues = _issues(pages)
    assert (issues["missing_reciprocal_hreflang"]["count"]) == 1
    assert issues["missing_reciprocal_hreflang"]["examples"] == [S + "/fr"]
    items = _preuve(issues)
    assert items == [{"page": S + "/en", "field": "fr", "value": S + "/fr"}], items


def test_le_code_est_celui_sous_lequel_la_source_se_designe() -> None:
    """La cible doit nommer la source avec le code que la source emploie pour elle-meme.

    Prendre la langue du document serait faux des qu'un site emploie une variante regionale
    (`fr-CA`) alors que son attribut lang dit `fr`.
    """
    pages = [_page("/ca", {"fr-ca": S + "/ca", "en": S + "/en"}),
             _page("/en", {"en": S + "/en", "de": S + "/de"}, "en"),
             _page("/de", {"de": S + "/de", "en": S + "/en"}, "de")]
    items = _preuve(_issues(pages))
    assert items and items[0]["field"] == "fr-ca", items


def test_un_groupe_RECIPROQUE_ne_produit_aucune_preuve() -> None:
    """Le garde-fou de toute preuve : elle ne doit apparaitre que sur une vraie anomalie."""
    pages = [_page("/fr", {"fr": S + "/fr", "en": S + "/en"}),
             _page("/en", {"en": S + "/en", "fr": S + "/fr"}, "en")]
    issues = _issues(pages)
    assert issues["missing_reciprocal_hreflang"]["count"] == 0
    assert _preuve(issues) == []


def test_sans_sitemap_la_famille_entiere_se_tait() -> None:
    """Comportement d'origine, fixe ici : tout le bloc de detection est garde par le sitemap.

    Sans ce test, une future preuve pourrait etre attachee hors de cette garde et faire parler la
    famille sur des sites ou elle se taisait — une divergence de parite invisible au banc.
    """
    pages = [_page("/fr", {"fr": S + "/fr", "en": S + "/en"}),
             _page("/en", {"en": S + "/en", "de": S + "/de"}, "en")]
    issues = audit._score_issues(pages, base_url=S)
    assert (issues.get("missing_reciprocal_hreflang") or {}).get("count") == 0
