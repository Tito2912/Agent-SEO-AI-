# -*- coding: utf-8 -*-
"""La moitie crawler de `links_with_no_anchor_text`.

La famille se leve quand un lien INTERNE n'offre aucune ancre : ni texte visible, ni `title`, ni
`aria-label`, ni `alt` sur l'image qu'il entoure — l'alt compte deja comme ancre, et c'est ce qui
evite d'accuser tous les liens-images d'un site. Elle nomme donc la page a editer et le lien
fautif ; ce qu'elle ne dit pas, c'est QUOI ecrire dedans.

LA CIBLE TRANCHE, et c'est une MESURE, pas une redaction : la page pointee a ete crawlee, et elle
se nomme elle-meme. Son h1, quand elle en a exactement un, dit son nom sans le suffixe de marque
que traine un <title> ; sinon son <title> fait foi. Deux h1 ne departagent rien et on ne choisit
pas entre eux. Une cible jamais crawlee, en erreur ou muette ne rend rien : la famille reste alors
signalee sans preuve, donc expliquee sans etre corrigee.

CE QUE LA PREUVE PORTE, ET POURQUOI. Le `field` est le href BRUT, tel que le document l'ecrit.
C'est la seule forme qui se retrouve dans un depot : un correcteur qui chercherait
`https://exemple.fr/contact` ne trouverait rien la ou le source dit `href="/contact"`. Ce champ
traverse `_lignes_sans_ancre`, sortie de `_extract_page` pour qu'un test puisse l'atteindre —
`_extract_page` pilote Playwright, et le maillon parseur -> ligne serait autrement reste le seul
non prouve de la chaine.

La detection elle-meme n'est pas touchee. Elle est reglee pour la parite Ahrefs (liens internes
seulement, 7d88ff7) et un correcteur n'a aucune raison de la deplacer. Un defaut y est CONNU et
laisse tel quel le 18/09/2026, le proprietaire tranchant : le motif `www\\.` de la ligne url-ish
matche un antislash litteral, donc un texte d'ancre `www.exemple.fr` n'est jamais reconnu comme
une URL nue et le lien passe pour ancre. Le corriger ferait MONTER le compte et demande d'etre
mesure sur les six sites de reference avant tout.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SRC = (Path(__file__).resolve().parents[2]
        / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")
_spec = importlib.util.spec_from_file_location("seo_audit_ancres", _SRC)
assert _spec and _spec.loader
audit = importlib.util.module_from_spec(_spec)
sys.modules["seo_audit_ancres"] = audit
_spec.loader.exec_module(audit)

S = "https://exemple.fr"
CLE = "links_with_no_anchor_text"


def _page(chemin: str, *, h1: list[str] | None = None, titre: str | None = None,
          statut: int = 200, erreur: str | None = None) -> object:
    p = audit.PageData(url=S + chemin)
    p.final_url = S + chemin
    p.status_code = statut
    p.content_type = "text/html; charset=utf-8"
    p.title = titre
    p.h1 = list(h1 or [])
    p.error = erreur
    return p


def _source(chemin: str, liens: list[dict]) -> object:
    """Une page qui porte deja ses lignes sans ancre, comme le parseur les aurait posees."""
    p = _page(chemin, titre="Source")
    p.links_without_anchor_text = [
        {"source_url": S + chemin, "target_url": S + d["vers"], "rel": "",
         "internal": d.get("interne", True), "anchor_text": d.get("texte", ""),
         "title": "", "aria_label": "", "href": d.get("href", d["vers"])}
        for d in liens
    ]
    return p


def _issues(pages: list[object]) -> dict:
    return audit._score_issues(pages, base_url=S)


def _preuve(issues: dict) -> list[dict]:
    bloc = issues.get(CLE) or {}
    ev = bloc.get("evidence") or {}
    return list(ev.get("items") or []) if ev.get("kind") == "page_values" else []


# --- ce que la cible dit d'elle-meme -------------------------------------------------------

def test_la_preuve_porte_le_href_BRUT_et_le_nom_de_la_cible() -> None:
    issues = _issues([_source("/", [{"vers": "/contact", "href": "/contact"}]),
                      _page("/contact", h1=["Contactez-nous"], titre="Contact | Exemple")])
    assert issues[CLE]["count"] == 1
    assert _preuve(issues) == [{"page": S + "/", "field": "/contact", "value": "Contactez-nous"}]


def test_le_h1_unique_bat_le_title_qui_traine_la_marque() -> None:
    """Un <title> dit « Tarifs | Exemple » ; une ancre ne se termine pas par le nom du site."""
    issues = _issues([_source("/", [{"vers": "/tarifs"}]),
                      _page("/tarifs", h1=["Nos tarifs"], titre="Tarifs | Exemple")])
    assert _preuve(issues)[0]["value"] == "Nos tarifs"


def test_sans_h1_le_title_fait_foi() -> None:
    issues = _issues([_source("/", [{"vers": "/blog"}]),
                      _page("/blog", h1=[], titre="Le journal")])
    assert _preuve(issues)[0]["value"] == "Le journal"


def test_DEUX_h1_ne_departagent_rien_et_on_retombe_sur_le_title() -> None:
    """Choisir entre deux h1 serait deviner ; le <title>, lui, est unique par construction."""
    issues = _issues([_source("/", [{"vers": "/offres"}]),
                      _page("/offres", h1=["Offres", "Promotions"], titre="Offres | Exemple")])
    assert _preuve(issues)[0]["value"] == "Offres | Exemple"


# --- ce qui fait TAIRE la preuve ------------------------------------------------------------

def test_une_cible_jamais_crawlee_ne_nomme_rien() -> None:
    issues = _issues([_source("/", [{"vers": "/inconnue"}])])
    assert issues[CLE]["count"] == 1
    assert _preuve(issues) == []


def test_une_cible_en_erreur_ne_nomme_rien() -> None:
    """Nommer un lien d'apres une page cassee ferait ecrire une ancre qui ment."""
    issues = _issues([_source("/", [{"vers": "/panne"}]),
                      _page("/panne", h1=["Erreur"], titre="500", statut=500)])
    assert _preuve(issues) == []


def test_une_cible_qui_a_echoue_au_crawl_ne_nomme_rien() -> None:
    issues = _issues([_source("/", [{"vers": "/muette"}]),
                      _page("/muette", h1=["Titre"], titre="Titre", erreur="Timeout")])
    assert _preuve(issues) == []


def test_une_cible_SANS_h1_NI_title_ne_nomme_rien() -> None:
    issues = _issues([_source("/", [{"vers": "/vide"}]), _page("/vide", h1=[], titre=None)])
    assert _preuve(issues) == []


def test_un_lien_sans_href_brut_ne_peut_pas_etre_retrouve() -> None:
    """Sans la forme ecrite dans le source, le correcteur ne saurait pas quelle ligne toucher.

    Le gardien est le CONTRAT de preuve, pas un filtre local : `_attach_issue_evidence` exige
    `page`, `field` et `value` non vides pour un `page_values`. Une premiere version refiltrait
    ici, et la mutation l'a montre inutile — le test restait vert avec le filtre retire, parce
    qu'il verifiait le contrat sans le savoir. Un seul gardien, nomme.
    """
    issues = _issues([_source("/", [{"vers": "/contact", "href": ""}]),
                      _page("/contact", h1=["Contactez-nous"])])
    assert issues[CLE]["count"] == 1
    assert _preuve(issues) == []


def test_un_lien_EXTERNE_reste_hors_sujet() -> None:
    """Parite Ahrefs : la famille est interne. La preuve ne doit pas la rouvrir par la bande."""
    src = _source("/", [{"vers": "/contact"}])
    src.links_without_anchor_text[0]["internal"] = False
    src.links_without_anchor_text[0]["target_url"] = "https://autre.fr/contact"
    assert _issues([src, _page("/contact", h1=["Contactez-nous"])])[CLE]["count"] == 0


# --- le maillon parseur -> ligne, que Playwright rendait inatteignable ----------------------

def _lignes(html_items: list[dict]) -> list[dict]:
    from urllib.parse import urlsplit
    return audit._lignes_sans_ancre(
        html_items, page_url=S + "/", base_for_urls=S + "/",
        base_parts=urlsplit(S), allow_subdomains=False,
    )


def test_le_href_ecrit_dans_le_document_traverse_jusqu_a_la_ligne() -> None:
    """C'est CE maillon qui n'etait prouve par rien : le reste de la chaine l'attend."""
    lignes = _lignes([{"href": "/contact", "text": "", "title": "", "aria_label": "", "rel": ""}])
    assert len(lignes) == 1
    assert lignes[0]["href"] == "/contact"
    assert lignes[0]["target_url"] == S + "/contact"


def test_un_lien_relatif_garde_sa_forme_relative() -> None:
    """La normalisation sert a comparer, jamais a reecrire : `../contact` doit rester tel quel."""
    lignes = _lignes([{"href": "../contact", "text": "", "title": "", "aria_label": "", "rel": ""}])
    assert lignes[0]["href"] == "../contact"


def test_une_ancre_reelle_ailleurs_sur_la_page_disculpe_le_lien_vide() -> None:
    """Un overlay vide au-dessus d'un lien texte n'est pas un lien sans ancre."""
    lignes = _lignes([
        {"href": "/contact", "text": "", "title": "", "aria_label": "", "rel": ""},
        {"href": "/contact", "text": "Contactez-nous", "title": "", "aria_label": "", "rel": ""},
    ])
    assert lignes == []


def test_un_title_ou_un_aria_label_suffit_deja_comme_ancre() -> None:
    assert _lignes([{"href": "/a", "text": "", "title": "Vers A", "aria_label": "", "rel": ""}]) == []
    assert _lignes([{"href": "/b", "text": "", "title": "", "aria_label": "Vers B", "rel": ""}]) == []
