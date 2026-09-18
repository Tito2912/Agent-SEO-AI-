"""La moitie crawler de `page_referenced_for_more_than_one_language_in_hreflang`.

La famille se leve quand une page designe la MEME URL sous plusieurs langues primaires — `fr` et
`en` pointant tous deux vers /en. Elle nomme bien la page a editer, contrairement a la famille
voisine ; ce qu'elle ne dit pas, c'est LAQUELLE des deux annotations retirer. Les deux sont
syntaxiquement irreprochables et le nom de l'anomalie ne departage pas.

LA CIBLE TRANCHE, et c'est une MESURE, pas un arbitrage : la page pointee declare une langue, et
le code qui la contredit est celui qui part. Quand la cible n'a pas ete crawlee, ne declare aucune
langue, ou en declare une qui n'est meme pas en lice, rien ne designe la bonne — et la famille
reste alors signalee sans preuve, donc expliquee sans etre corrigee. C'est le comportement voulu :
REFUSER bat REPARER des qu'il faudrait deviner.

La detection elle-meme n'est pas touchee. Elle est reglee pour la parite Ahrefs — variantes
regionales de meme langue primaire ignorees, `x-default` exclu — et un correcteur n'a aucune
raison de la deplacer. Deux tests l'attachent ici pour qu'un futur ajout de preuve ne la fasse pas
glisser.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SRC = (Path(__file__).resolve().parents[2]
        / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")
_spec = importlib.util.spec_from_file_location("seo_audit_multilang", _SRC)
assert _spec and _spec.loader
audit = importlib.util.module_from_spec(_spec)
sys.modules["seo_audit_multilang"] = audit
_spec.loader.exec_module(audit)

S = "https://exemple.fr"
CLE = "page_referenced_for_more_than_one_language_in_hreflang"


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
    return audit._score_issues(pages, base_url=S)


def _preuve(issues: dict) -> list[dict]:
    bloc = issues.get(CLE) or {}
    ev = bloc.get("evidence") or {}
    return list(ev.get("items") or []) if ev.get("kind") == "page_values" else []


# `/fr` designe `/en` sous `fr` ET sous `en`. `/en` declare lang="en" : `fr` est en trop.
FAUTIVE = _page("/fr", {"fr": S + "/en", "en": S + "/en"})
CIBLE = _page("/en", {"en": S + "/en"}, "en")


def test_la_preuve_nomme_le_code_a_RETIRER_pas_celui_a_garder() -> None:
    issues = _issues([FAUTIVE, CIBLE])
    assert issues[CLE]["count"] == 1
    assert issues[CLE]["examples"] == [S + "/fr"]
    assert _preuve(issues) == [{"page": S + "/fr", "field": "fr", "value": S + "/en"}], _preuve(issues)


def test_sans_cible_crawlee_la_famille_parle_mais_ne_propose_rien() -> None:
    """Personne ne dit quelle langue la cible est : deviner poserait un retrait irrattrapable."""
    issues = _issues([_page("/fr", {"fr": S + "/ailleurs", "en": S + "/ailleurs"})])
    assert issues[CLE]["count"] == 1
    assert _preuve(issues) == []


def test_une_cible_SANS_langue_declaree_ne_tranche_rien() -> None:
    issues = _issues([FAUTIVE, _page("/en", {"en": S + "/en"}, "")])
    assert issues[CLE]["count"] == 1
    assert _preuve(issues) == []


def test_une_cible_dont_la_langue_n_est_PAS_en_lice_ne_tranche_rien() -> None:
    """`/en` se declare `de` : ni `fr` ni `en` n'a raison, et retirer au hasard en casserait un."""
    issues = _issues([FAUTIVE, _page("/en", {"en": S + "/en"}, "de")])
    assert issues[CLE]["count"] == 1
    assert _preuve(issues) == []


def test_x_default_n_est_jamais_propose_au_retrait() -> None:
    """Il designe legitimement la meme URL qu'une langue : le retirer serait une regression."""
    fautive = _page("/fr", {"fr": S + "/en", "en": S + "/en", "x-default": S + "/en"})
    items = _preuve(_issues([fautive, CIBLE]))
    assert [i["field"] for i in items] == ["fr"], items


def test_une_variante_REGIONALE_n_est_pas_un_conflit() -> None:
    """Parite Ahrefs : `fr` et `fr-CA` vers la meme URL partagent la langue primaire."""
    pages = [_page("/fr", {"fr": S + "/fr", "fr-ca": S + "/fr"}), CIBLE]
    issues = _issues(pages)
    assert issues[CLE]["count"] == 0
    assert _preuve(issues) == []


def test_un_groupe_SAIN_ne_produit_aucune_preuve() -> None:
    pages = [_page("/fr", {"fr": S + "/fr", "en": S + "/en"}),
             _page("/en", {"fr": S + "/fr", "en": S + "/en"}, "en")]
    issues = _issues(pages)
    assert issues[CLE]["count"] == 0
    assert _preuve(issues) == []
