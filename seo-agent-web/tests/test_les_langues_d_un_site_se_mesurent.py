# -*- coding: utf-8 -*-
"""Quelles langues ce site parle, et quelle cle lie ses traductions — sans les nommer.

PREMIERE COUCHE DU CHANTIER MULTILINGUE, decide le 27/09/2026 : « ca serait bien que le client
puisse choisir une ou plusieurs langues avant de lancer la generation ». Avant d'ecrire N pages,
il faut savoir QUELLES langues proposer, et OU trouver la soeur dans chacune.

Deux questions, deux grammaires, aucune liste codee en dur :

    quelles langues ?   -> les prefixes de route qui portent plusieurs pages
    quelle cle lie ?    -> celle dont la valeur est EGALE entre langues et UNIQUE par page

Mesure sur un vrai site client (`prosperfactory.com`, 4 langues, 104 routes) : `fr` 28, `de` 27,
`es` 26, `en` 23, plus 6 routes sans prefixe — la langue par defaut, servie a la racine.
"""
from __future__ import annotations

import pytest

from backend import app as m


# ── quelles langues ce site parle ────────────────────────────────────────────────────────────

def test_un_site_multilingue_rend_chaque_langue_et_son_defaut() -> None:
    routes = ["/fr/guides/a", "/fr/guides/b", "/fr/a-propos",
              "/de/guides/a", "/de/guides/b",
              "/a-propos", "/contact"]
    assert m._langues_des_routes(routes) == {"fr": 3, "de": 2, "": 2}


def test_un_site_monolingue_ne_rend_que_le_defaut() -> None:
    """Pas de cas particulier a ecrire chez l'appelant : la racine est une langue comme une
    autre, sous la cle vide."""
    assert m._langues_des_routes(["/blog/a", "/blog/b", "/contact"]) == {"": 3}


def test_une_rubrique_qui_RESSEMBLE_a_une_langue_n_en_est_pas_une() -> None:
    """`/en/` portant UNE page est une rubrique — une presentation en anglais, par exemple.
    Sans ce plancher, on proposerait au client de generer dans une langue qui n'existe pas."""
    routes = ["/blog/a", "/blog/b", "/blog/c", "/en/about-us"]
    assert m._langues_des_routes(routes) == {"": 3}


def test_un_code_regional_est_reconnu() -> None:
    routes = ["/pt-br/a", "/pt-br/b", "/zh-cn/a", "/zh-cn/b", "/a"]
    obtenu = m._langues_des_routes(routes)
    assert obtenu["pt-br"] == 2 and obtenu["zh-cn"] == 2, obtenu


@pytest.mark.parametrize("segment", ["guides", "a-propos", "blog", "2026", "f"])
def test_ce_qui_n_a_pas_la_forme_d_un_code_compte_pour_le_defaut(segment: str) -> None:
    routes = ["/%s/a" % segment, "/%s/b" % segment, "/%s/c" % segment]
    assert m._langues_des_routes(routes) == {"": 3}


# ── quelle cle lie les traductions ───────────────────────────────────────────────────────────

FR = ('---\ntitle: "DCA crypto"\ntype: "guide"\ntranslationKey: "guides/crypto-dca"\n'
      'updatedAt: "2026-03-11"\n---\n\nCorps.\n')
DE = ('---\ntitle: "Krypto DCA"\ntype: "guide"\ntranslationKey: "guides/crypto-dca"\n'
      'updatedAt: "2026-03-11"\n---\n\nInhalt.\n')
ES = ('---\ntitle: "DCA cripto"\ntype: "guide"\ntranslationKey: "guides/crypto-dca"\n'
      '---\n\nCuerpo.\n')
# Une AUTRE page, dans la MEME langue que la source. C'est le temoin.
FR_AUTRE = ('---\ntitle: "Compte Nickel"\ntype: "guide"\ntranslationKey: "guides/nickel"\n'
            'updatedAt: "2026-03-11"\n---\n\nCorps.\n')


def test_la_cle_qui_lie_est_trouvee_sans_etre_nommee() -> None:
    cle, famille = m._famille_de_traduction(FR, {"de": DE, "es": ES}, [FR_AUTRE])
    assert cle == "translationKey", cle
    assert sorted(famille) == ["de", "es"], sorted(famille)


def test_une_cle_de_CATEGORIE_est_ecartee_par_les_temoins() -> None:
    """LE PIEGE MESURE. `type: "guide"` est egal entre toutes les pages de la section : il lie
    donc PLUS de candidats que la vraie cle et gagnerait tout classement par nombre.

    Ma premiere version l'énonçait dans sa docstring sans le vérifier — elle demandait à
    l'appelant de ne passer que des candidats de la même section. Première mesure sur le site
    client : elle a rendu `type` et « lié » une page sans rapport. Une précondition écrite dans
    un commentaire n'est pas une garde.
    """
    leurre = ('---\ntitle: "Sans rapport"\ntype: "guide"\n'
              'translationKey: "guides/autre-chose"\n---\n\nCorps.\n')
    cle, famille = m._famille_de_traduction(FR, {"de": DE, "leurre": leurre}, [FR_AUTRE])
    assert cle == "translationKey", cle
    assert "leurre" not in famille, sorted(famille)


def test_sans_temoin_la_categorie_gagnerait() -> None:
    """Le témoin de l'autre bord : il montre que ce sont bien les témoins qui tranchent, et
    pas un hasard de tri. Sans eux, `type` lie trois candidats contre deux."""
    leurre = '---\ntitle: "X"\ntype: "guide"\ntranslationKey: "guides/x"\n---\n\nCorps.\n'
    cle, _ = m._famille_de_traduction(FR, {"de": DE, "es": ES, "leurre": leurre})
    assert cle == "type", cle


def test_une_page_sans_bloc_de_tete_ne_lie_rien() -> None:
    assert m._famille_de_traduction("export const metadata = {};\n", {"de": DE}) == ("", {})


def test_sans_aucune_valeur_commune_on_ne_lie_rien() -> None:
    etranger = '---\ntitle: "Rien"\nautre: "valeur"\n---\n\nCorps.\n'
    assert m._famille_de_traduction(FR, {"de": etranger}, [FR_AUTRE]) == ("", {})


# ── lire les valeurs de tete ─────────────────────────────────────────────────────────────────

def test_les_valeurs_de_tete_ignorent_listes_et_objets() -> None:
    """On compare des valeurs entre fichiers : seules celles qui tiennent sur une ligne s'y
    pretent. Une liste comparee par sa premiere ligne dirait n'importe quoi."""
    page = ('---\ntitle: "T"\njumpLinks:\n  - href: "#a"\n    label: "A"\n'
            'objet: { a: 1 }\nsimple: "oui"\n---\n\nCorps.\n')
    valeurs = m._valeurs_de_tete(page)
    assert valeurs.get("title") == "T" and valeurs.get("simple") == "oui", valeurs
    assert "jumpLinks" not in valeurs and "objet" not in valeurs, valeurs


def test_le_front_matter_TOML_est_lu_comme_le_YAML() -> None:
    assert m._valeurs_de_tete('+++\ntitle = "T"\nkey = "v"\n+++\n\nCorps.\n') == {
        "title": "T", "key": "v"}


def test_le_CORPS_n_est_jamais_lu() -> None:
    """Sans borne, une ligne `title: autre chose` du corps ecraserait celle de la tete."""
    page = '---\ntitle: "Vrai"\n---\n\ntitle: "Faux, dans le corps"\n'
    assert m._valeurs_de_tete(page).get("title") == "Vrai"
