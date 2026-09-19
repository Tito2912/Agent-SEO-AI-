# -*- coding: utf-8 -*-
"""Une balise sociale qui pointe sur un hote prive vaut une balise absente.

MESURE DU 19/09/2026, reproduction Next.js reelle. Sans `metadataBase`, Next resout les URL
relatives des metadonnees contre `http://localhost:3000` et se contente d'un avertissement de
build :

    ⚠ metadataBase property in metadata export is not set for resolving social open graph
      or twitter images, using "http://localhost:3000"

    og:image = http://localhost:3000/og.png

Le site part donc en production avec une image de partage qui pointe sur la machine du
visiteur. `metadataBase` est optionnel, souvent oublie, et l'avertissement disparait dans un
journal de deploiement. Le crawler, lui, voyait une chaine non vide et concluait « og:image
presente ».

POURQUOI ON ELARGIT UNE ANOMALIE EXISTANTE AU LIEU D'EN CREER UNE. Le catalogue Ahrefs ne
compte que cinq anomalies de balises sociales et aucune ne parle d'atteignabilite ; inventer
une cle sortirait de la parite qui gouverne ce crawler. Or une `og:image` sur localhost EST,
pour tout consommateur externe — robot, Facebook, LinkedIn, X —, une `og:image` absente. La
compter comme presente est le choix le moins exact des deux, et c'est celui qu'on abandonne.

CE QUE CE CHANGEMENT N'EST PAS. Ce n'est pas une verification d'atteignabilite : on ne va rien
chercher sur le reseau. On refuse une classe d'hotes dont on sait par construction qu'aucun
tiers ne peut les joindre. Une URL publique cassee (404) reste hors de portee de cette regle,
et c'est volontaire — la mesurer demanderait une requete par balise et par page.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_social_tests",
    REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py",
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_social_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)

BASE = "https://site.test"

OG_COMPLET: dict[str, Any] = {
    "og_title": "Un titre de partage",
    "og_description": "Une description de partage assez longue pour être crédible.",
    "og_image": "https://site.test/og.png",
    "og_url": BASE + "/",
    "og_type": "website",
}


def _page(url: str, **kw: Any) -> Any:
    defaults: dict[str, Any] = {
        "url": url,
        "final_url": url,
        "status_code": 200,
        "content_type": "text/html; charset=utf-8",
        "title": f"Titre propre de la page {url}",
        "meta_description": f"Description unique et de longueur raisonnable pour {url}.",
        "canonical": url,
        "lang": "fr",
        "h1": ["Titre principal"],
        "h1_tag_count": 1,
        "title_tag_count": 1,
    }
    defaults.update(OG_COMPLET)
    defaults["og_url"] = url
    defaults.update(kw)
    return seo_audit.PageData(**defaults)


def _cle(issues: dict[str, Any], key: str) -> int:
    bloc = issues.get(key)
    return int(bloc.get("count") or 0) if isinstance(bloc, dict) else 0


# --- le cas mesure ------------------------------------------------------------------------------

def test_une_og_image_sur_localhost_rend_la_page_INCOMPLETE() -> None:
    """Le défaut exact : quatre balises correctes, une cinquième inutilisable."""
    pages = [_page(f"{BASE}/", og_image="http://localhost:3000/og.png")]
    issues = seo_audit._score_issues(pages, base_url=BASE)
    assert _cle(issues, "open_graph_tags_incomplete") == 1


def test_la_balise_fautive_est_NOMMEE_dans_la_preuve() -> None:
    """Dire « incomplet » sans dire laquelle laisse le client chercher parmi cinq."""
    pages = [_page(f"{BASE}/", og_image="http://localhost:3000/og.png")]
    issues = seo_audit._score_issues(pages, base_url=BASE)
    ev = issues["open_graph_tags_incomplete"].get("evidence") or {}
    valeurs = " ".join(str(i.get("value") or "") for i in (ev.get("items") or []))
    assert "og:image" in valeurs, ev


def test_un_og_image_PUBLIC_ne_declenche_rien() -> None:
    """Le contre-test sans lequel le précédent ne prouve rien : la règle doit distinguer."""
    pages = [_page(f"{BASE}/")]
    issues = seo_audit._score_issues(pages, base_url=BASE)
    assert _cle(issues, "open_graph_tags_incomplete") == 0
    assert _cle(issues, "open_graph_tags_missing") == 0


@pytest.mark.parametrize("hote", [
    "http://localhost:3000/og.png",
    "http://127.0.0.1:8080/og.png",
    "http://0.0.0.0/og.png",
    "http://[::1]/og.png",
    "http://127.0.0.53/og.png",
])
def test_toute_la_famille_des_hotes_PRIVES(hote) -> None:
    """Next écrit `localhost:3000`, mais toute la plage de bouclage produit le même résultat
    pour un tiers : rien à récupérer. `127.0.0.53` est celle du résolveur systemd, qu'on
    croise dans des configurations conteneurisées."""
    pages = [_page(f"{BASE}/", og_image=hote)]
    issues = seo_audit._score_issues(pages, base_url=BASE)
    assert _cle(issues, "open_graph_tags_incomplete") == 1, hote


@pytest.mark.parametrize("public", [
    "https://site.test/og.png",
    "https://cdn.exemple.fr/i/og.png",
    "/og.png",
    "https://localhost-hosting.fr/og.png",
    "https://ma-boutique.local-shop.fr/og.png",
    # `.local` et `.test` sont DELIBEREMENT absents de la regle : voir le commentaire du
    # predicat. Une politique large sur « ce qui est public » invalide des balises correctes.
    "https://mon-mac.local/og.png",
    "https://exemple.test/og.png",
])
def test_ce_qui_RESSEMBLE_a_un_hote_prive_sans_en_etre_un(public) -> None:
    """Le piège d'une règle écrite à la sous-chaîne : `localhost-hosting.fr` et
    `local-shop.fr` contiennent le motif sans être privés. Refuser une image valide serait
    pire que le défaut qu'on corrige — on ferait chercher un problème qui n'existe pas."""
    pages = [_page(f"{BASE}/", og_image=public)]
    issues = seo_audit._score_issues(pages, base_url=BASE)
    assert _cle(issues, "open_graph_tags_incomplete") == 0, public


# --- la meme regle partout ------------------------------------------------------------------------

def test_une_page_dont_TOUTES_les_balises_sont_privees_est_MANQUANTE_pas_incomplete() -> None:
    """La distinction que fait Ahrefs : rien du tout, ou presque tout. Des balises toutes
    inutilisables valent zéro balise, donc `missing` — sinon on classerait la page dans la
    mauvaise famille et la correction proposée serait la mauvaise."""
    prives = {k: "http://localhost:3000/x" for k in ("og_image", "og_url")}
    prives.update({"og_title": None, "og_description": None, "og_type": None})
    pages = [_page(f"{BASE}/", **prives)]
    issues = seo_audit._score_issues(pages, base_url=BASE)
    assert _cle(issues, "open_graph_tags_missing") == 1
    assert _cle(issues, "open_graph_tags_incomplete") == 0


def test_la_carte_twitter_suit_la_MEME_regle() -> None:
    """Même cause, même fallback Next, même conséquence : traiter l'une sans l'autre
    laisserait la moitié du défaut en place."""
    pages = [_page(f"{BASE}/", twitter_card="summary_large_image",
                   twitter_title="Un titre", twitter_description="Une description",
                   twitter_image="http://localhost:3000/tw.png")]
    issues = seo_audit._score_issues(pages, base_url=BASE)
    assert _cle(issues, "twitter_card_incomplete") == 1


# --- le predicat lui-meme --------------------------------------------------------------------------

def test_une_balise_ABSENTE_compte_toujours_comme_absente() -> None:
    """Le garde-fou du `and` : la nouvelle condition ne doit rien retirer à l'ancienne.

    CE TEST NE MESURE PAS CE QUE SA PREMIÈRE VERSION ANNONÇAIT. Je le présentais comme la
    preuve que `_url_non_publique` distingue « inatteignable » de « absent » — une mutation
    sur cette distinction lui a survécu, et à raison : `_balise_utilisable` s'écrit
    `_non_empty(v) and not _url_non_publique(v)`, donc sur une valeur vide le `and`
    court-circuite avant d'interroger le prédicat. La branche est inatteignable depuis son
    seul appelant. Ce qui reste vrai et vérifiable est plus modeste : ajouter une condition
    n'a pas fait disparaître la détection d'origine.
    """
    pages = [_page(f"{BASE}/", og_image=None)]
    issues = seo_audit._score_issues(pages, base_url=BASE)
    assert _cle(issues, "open_graph_tags_incomplete") == 1


def test_un_titre_qui_CONTIENT_le_mot_localhost_reste_valide() -> None:
    """Le prédicat s'applique à toutes les balises, y compris celles qui ne sont pas des URL.
    Un article qui parle de `localhost` ne doit pas voir ses balises sociales invalidées."""
    pages = [_page(f"{BASE}/", og_title="Déboguer sur localhost en 5 minutes")]
    issues = seo_audit._score_issues(pages, base_url=BASE)
    assert _cle(issues, "open_graph_tags_incomplete") == 0
