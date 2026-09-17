"""Une preproduction qui declare les canonical de la PRODUCTION reste jugeable.

Situation client reelle, et situation du banc : le site crawle est servi sur un hote
(`deploy-preview-30--exemple.netlify.app`), mais ses `canonical` et `hreflang` nomment l'hote de
production (`exemple.fr`) — c'est ce que fera tout site correctement configure avant sa mise en
ligne. La cible du canonical n'est alors JAMAIS parmi les pages crawlees, et toute famille qui
doit la resoudre se tait.

Mesure du 17/09/2026 sur les neuf stacks du banc, production contre preview :

    canonical_points_to_4xx                         9  ->  0
    canonical_points_to_redirect                   18  ->  0
    non_canonical_page_specified_as_canonical_one   9  ->  0
    hreflang_to_non_canonical                      16  ->  0
    hreflang_to_redirect_or_broken_page             1  ->  0
                                                   53  ->  0

Pas une survivante — et un zero pareil se lit comme « tout va bien ».

`--canonical-host-alias` indexe chaque page crawlee SOUS CE NOM AUSSI. Il n'ouvre aucune visite :
l'alias n'entre que dans l'index des pages deja vues. C'est la difference entre « crawler la
production » — qu'on ne veut pas — et « comprendre que la page dont le site parle est celle qu'on
vient de crawler ».
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SRC = (Path(__file__).resolve().parents[2]
        / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")
_spec = importlib.util.spec_from_file_location("seo_audit_alias", _SRC)
assert _spec and _spec.loader
audit = importlib.util.module_from_spec(_spec)
sys.modules["seo_audit_alias"] = audit
_spec.loader.exec_module(audit)

PREVIEW = "https://deploy-preview-30--exemple.netlify.app"
PROD = "exemple.fr"


def _page(chemin: str, *, statut: int = 200, canonical: str | None = None) -> object:
    p = audit.PageData(url=f"{PREVIEW}{chemin}")
    p.final_url = f"{PREVIEW}{chemin}"
    p.status_code = statut
    p.content_type = "text/html; charset=utf-8"
    p.canonical = canonical
    return p


def _issues(pages: list[object], alias: str = "") -> dict[str, dict]:
    return audit._score_issues(pages, base_url=PREVIEW, canonical_host_alias=alias)


def test_sans_alias_la_famille_se_TAIT_et_c_est_le_defaut_mesure() -> None:
    """Le comportement d'avant, fixe ici pour qu'on voie ce que l'alias change."""
    pages = [
        _page("/a", canonical=f"https://{PROD}/absente"),
        _page("/absente", statut=404),
    ]
    bloc = _issues(pages).get("canonical_points_to_4xx") or {}
    assert bloc.get("count", 0) == 0, bloc


def test_avec_l_alias_le_canonical_vers_une_404_est_vu() -> None:
    pages = [
        _page("/a", canonical=f"https://{PROD}/absente"),
        _page("/absente", statut=404),
    ]
    bloc = _issues(pages, alias=PROD).get("canonical_points_to_4xx") or {}
    assert bloc.get("count", 0) == 1, bloc


def test_l_alias_accepte_une_URL_complete_pas_seulement_un_hote() -> None:
    """On tape volontiers `https://exemple.fr/` : c'est l'hote qui compte."""
    pages = [
        _page("/a", canonical=f"https://{PROD}/absente"),
        _page("/absente", statut=404),
    ]
    bloc = _issues(pages, alias=f"https://{PROD}/").get("canonical_points_to_4xx") or {}
    assert bloc.get("count", 0) == 1, bloc


def test_un_canonical_SAIN_ne_declenche_rien_avec_l_alias() -> None:
    """L'alias doit rendre la famille MESURABLE, pas bavarde."""
    pages = [
        _page("/a", canonical=f"https://{PROD}/a"),
        _page("/b", canonical=f"https://{PROD}/b"),
    ]
    issues = _issues(pages, alias=PROD)
    for cle in ("canonical_points_to_4xx", "canonical_points_to_5xx"):
        assert (issues.get(cle) or {}).get("count", 0) == 0, (cle, issues.get(cle))


def test_un_hote_ETRANGER_n_est_pas_resolu_pour_autant() -> None:
    """Seul l'hote declare est un alias. Un canonical vers un tiers reste hors perimetre."""
    pages = [
        _page("/a", canonical="https://un-autre-site.fr/absente"),
        _page("/absente", statut=404),
    ]
    bloc = _issues(pages, alias=PROD).get("canonical_points_to_4xx") or {}
    assert bloc.get("count", 0) == 0, bloc


def test_un_demi_alias_serait_PIRE_que_pas_d_alias() -> None:
    """Chaque page d'une preprod declare un canonical different de l'URL servie. C'est NORMAL.

    Mesure du 17/09/2026, et le banc l'a attrape avant livraison : apres avoir rendu la CIBLE
    resoluble, j'avais laisse la comparaison d'une page avec elle-meme sur son `final_url` brut.
    `non_canonical_page_specified_as_canonical_one` est passee a 36-46 pages par stack — presque
    toutes. Un client aurait vu quarante fausses alertes sur sa preproduction.

    Un demi-alias remplace un silence par un tapage, et le tapage coute plus cher : un zero se
    verifie, quarante alertes fausses se croient.
    """
    pages = [_page("/a", canonical=f"https://{PROD}/a"),
             _page("/b", canonical=f"https://{PROD}/b"),
             _page("/c", canonical=f"https://{PROD}/c")]
    bloc = _issues(pages, alias=PROD).get("non_canonical_page_specified_as_canonical_one") or {}
    assert bloc.get("count", 0) == 0, bloc


def test_une_VRAIE_chaine_non_canonique_reste_signalee_sous_l_alias() -> None:
    """Le garde-fou ne doit pas rendre la famille muette : a -> b, et b se declare canonique de c."""
    pages = [_page("/a", canonical=f"https://{PROD}/b"),
             _page("/b", canonical=f"https://{PROD}/c"),
             _page("/c", canonical=f"https://{PROD}/c")]
    bloc = _issues(pages, alias=PROD).get("non_canonical_page_specified_as_canonical_one") or {}
    assert bloc.get("count", 0) == 1, bloc
