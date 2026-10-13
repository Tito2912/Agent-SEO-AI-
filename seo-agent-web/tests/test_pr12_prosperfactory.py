# -*- coding: utf-8 -*-
"""Les trois defauts de la PR #12 de prosperfactory.com (13/10/2026).

1. « Page -> lien vers redirection » : les accueils lient /payment, /fr/paiement… qui repondent
   302 vers buy.stripe.com. Le correcteur a reecrit `/es/pago` en `/5kQd…` (le CHEMIN de l'URL
   Stripe, accole au site : un lien casse), et la canonical d'une page vers Stripe. Un lien vers
   une redirection qui quitte le site ne se reecrit pas ; un <link> n'est pas un lien.
2. Cinq titres RACCOURCIS revenus avec « 2024 » ou « 2025 », absents des pages (datees 2026).
3. « Non traites : 28 » suivi de douze lignes, sans dire qu'il en restait seize.
"""
from __future__ import annotations

import json

import pytest

from backend import app as m

S = "https://prosperfactory.com"
STRIPE = "https://buy.stripe.com/5kQdR87022i71XF54B63K02"


# --- 1. liens vers une redirection hors du site -------------------------------------------

def test_le_chemin_d_une_destination_sur_un_autre_site_n_est_jamais_accole_au_site() -> None:
    assert m._url_value_variants({"from": S + "/es/pago", "to": STRIPE}) == [
        (S + "/es/pago", STRIPE), ("/es/pago", STRIPE)]
    # Meme site (au www pres) : l'ecriture relative reste relative.
    assert m._url_value_variants({"from": S + "/x/", "to": "https://www.prosperfactory.com/x"}) == [
        (S + "/x/", "https://www.prosperfactory.com/x"), ("/x/", "/x")]


def _prep(paires: list[dict[str, str]]) -> dict:
    issues = {"page_has_links_to_redirect_indexable": {"count": 4, "redirect_link_samples": paires}}
    return m._prepare_issue_fix(
        issue_key="page_has_links_to_redirect_indexable", issues=issues, impacted=[S + "/"],
        all_paths=["app/(sales)/page.tsx"], site_name="prosperfactory.com",
        owner="o", repo_name="r", branch="main", token="t")


def test_un_lien_vers_un_paiement_externe_est_refuse_sans_rien_toucher() -> None:
    prep = _prep([{"from": S + "/payment", "to": "https://buy.stripe.com/14Ab"},
                  {"from": S + "/es/pago", "to": STRIPE}])

    assert "buy.stripe.com" in prep["refusal"]
    assert not prep.get("link_rewriter")


def test_les_paires_du_site_restent_corrigees_et_seules_elles() -> None:
    prep = _prep([{"from": S + "/payment", "to": "https://buy.stripe.com/14Ab"},
                  {"from": S + "/a/", "to": S + "/a"}])

    assert not prep["refusal"]
    assert prep["evidence"] == [S + "/a/"]
    nouveau, n = prep["link_rewriter"]('<a href="/a/">A</a> <a href="/payment">Payer</a>')
    assert (nouveau, n) == ('<a href="/a">A</a> <a href="/payment">Payer</a>', 1)


def test_un_link_canonical_ou_hreflang_n_est_pas_un_lien() -> None:
    html = ('<link rel="canonical" href="https://prosperfactory.com/vieille"/>'
            '<link rel="alternate" hreflang="fr" href="https://prosperfactory.com/vieille"/>'
            '<a href="https://prosperfactory.com/vieille">x</a>')

    nouveau, n = m._rewrite_redirect_links(html, [{"from": S + "/vieille", "to": S + "/neuve"}])

    assert n == 1
    assert nouveau.count(S + "/vieille") == 2 and '<a href="https://prosperfactory.com/neuve">' in nouveau


# --- 2. nombres inventes ------------------------------------------------------------------

@pytest.mark.parametrize("valeur, fond, attendu", [
    ("TradingView Preise: gratis vs. bezahlt 2024", "TradingView: Preise (gratis vs bezahlt) — so entscheiden Sie",
     ["2024"]),
    ("Les 7 lois financières des ultra-riches", "7 lois financières que les ultra-riches ne partagent jamais", []),
    ("eToro : frais et commissions à vérifier en 2025", "eToro : frais et commissions (ce qu’il faut vérifier)",
     ["2025"]),
])
def test_un_nombre_absent_de_la_source_est_invente(valeur, fond, attendu) -> None:
    assert m._chiffres_inventes(valeur, fond) == attendu


LONG = "TradingView : tarifs (gratuit vs payant) — comment choisir le bon plan selon vos besoins"
AVEC_ANNEE = "TradingView : tarifs gratuit vs payant 2024"
PROPRE = "TradingView : tarifs (gratuit vs payant) — comment choisir le bon plan"


def _raccourcir(monkeypatch, reponses):
    appels = []

    def faux(**kw):
        appels.append(json.loads(kw["user_msg"]))
        return {"value": reponses[len(appels) - 1]}
    monkeypatch.setattr(m, "_correction_ai_json", faux)
    valeur = m._length_value_for_page(current=LONG, kind="title", url=S + "/fr/tools/tradingview-tarifs",
                                      site_name="prosperfactory.com", contexte="Mis a jour en 2026. TradingView")
    return valeur, appels


def test_une_annee_ajoutee_en_raccourcissant_fait_relancer(monkeypatch) -> None:
    valeur, appels = _raccourcir(monkeypatch, [AVEC_ANNEE, PROPRE])

    assert valeur == PROPRE
    assert "2024" in appels[1]["probleme"]


def test_elle_n_est_jamais_gardee_meme_faute_de_mieux(monkeypatch) -> None:
    valeur, _ = _raccourcir(monkeypatch, [AVEC_ANNEE, "TradingView : tarifs 2025 gratuit vs payant"])

    assert valeur == ""


def test_la_page_ne_suffit_pas_a_justifier_un_nombre_en_raccourcissant(monkeypatch) -> None:
    # 2026 est dans la page (date de mise a jour) mais pas dans le titre : raccourcir n'ajoute rien.
    # Dans la fenetre (63 car.) : sans la garde, elle serait rendue des le premier essai.
    valeur, _ = _raccourcir(monkeypatch, ["TradingView : tarifs gratuit vs payant, comment choisir en 2026", PROPRE])

    assert valeur == PROPRE


def test_la_consigne_ne_demande_plus_de_garder_une_annee_absente(monkeypatch) -> None:
    systemes = []

    def faux(**kw):
        systemes.append(kw["system"])
        return {"value": PROPRE}
    monkeypatch.setattr(m, "_correction_ai_json", faux)
    m._length_value_for_page(current=LONG, kind="title", url=S + "/x", site_name="prosperfactory.com")

    assert "l'année si elle y figure déjà" in systemes[0] and "aucune année" in systemes[0]


# --- 3. la liste des fichiers non traites dit qu'elle s'arrete ----------------------------

def test_la_liste_tronquee_dit_combien_il_en_reste() -> None:
    note = m._note_de_troncature([f"f{i}.mdx" for i in range(28)], None)

    assert "Non traités : 28 fichier(s)" in note
    assert note.count("> - `") == 12 and "… et 16 autre(s)" in note


def test_une_liste_courte_ne_dit_rien_de_plus() -> None:
    assert "autre(s)" not in m._note_de_troncature(["a.mdx", "b.mdx"], None)


# --- 4. PR #13 : la description courte absorbee par la longue -----------------------------
# « Tout corriger » regroupait les variantes de longueur d'une meme balise ; depuis le 12/10 la
# passe « trop longue » ecarte les descriptions courtes, qui ont leur route. Les six courtes de
# prosperfactory n'etaient ni corrigees ni citees dans la PR.

def _cles(*cles: str) -> list[str]:
    return [i["key"] for i in m._regrouper_les_passes([{"key": c} for c in cles])]


def test_la_description_courte_garde_sa_passe_a_cote_de_la_longue() -> None:
    assert _cles("title_too_long_indexable", "title_too_short_indexable", "meta_description_too_long_indexable",
                 "meta_description_too_short_indexable") == [
        "title_too_long_indexable", "meta_description_too_long_indexable", "meta_description_too_short_indexable"]
    assert _cles("meta_description_too_short_indexable", "meta_description_too_long_indexable") == [
        "meta_description_too_short_indexable", "meta_description_too_long_indexable"]


def test_chaque_passe_se_verifie_sur_ce_qu_elle_traite() -> None:
    longue = {"meta_description_too_long", "meta_description_too_long_indexable",
              "meta_description_too_long_not_indexable"}
    assert m._verification_family_keys("meta_description_too_long_indexable") == longue
    assert m._verification_family_keys("meta_description_too_short_indexable") == {
        "meta_description_too_short", "meta_description_too_short_indexable",
        "meta_description_too_short_not_indexable"}
    # Le titre garde une seule passe ; une famille hors longueur garde ses variantes.
    assert "title_too_short_indexable" in m._verification_family_keys("title_too_long_indexable")
    assert m._verification_family_keys("missing_h1_indexable") == {
        "missing_h1", "missing_h1_indexable", "missing_h1_not_indexable"}
    # Apres fusion de la PR #13 : 0 longue, 6 courtes — la passe longue est resolue.
    apres = {"meta_description_too_long_indexable": {"count": 0},
             "meta_description_too_short_indexable": {"count": 6}}
    assert m._verification_scope_count("meta_description_too_long_indexable", apres) == 0


def test_les_variantes_d_une_meme_passe_restent_regroupees() -> None:
    assert _cles("meta_description_too_short_indexable", "meta_description_too_short_not_indexable",
                 "redirect_3xx") == ["meta_description_too_short_indexable", "redirect_3xx"]
    assert _cles("title_too_short_indexable", "title_too_long_not_indexable") == ["title_too_short_indexable"]
