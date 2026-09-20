# -*- coding: utf-8 -*-
"""Un prix encode en chaine n'est PAS une erreur, et ne doit plus etre signale.

CE QUI A ETE RETIRE LE 20/09/2026, et pourquoi c'est une suppression et non un elargissement.

Le controle `offer_price_is_string` etait enferme dans `if "SoftwareApplication" in types:` —
donc un `Product` avec `"price": "29.90"`, le cas le plus courant en e-commerce, n'etait jamais
examine. L'incoherence sautait aux yeux et j'ai d'abord propose d'ELARGIR la regle au `Product`.

C'etait l'erreur. La regle elle-meme est fausse :

* schema.org donne `Text` OU `Number` comme types attendus pour `Offer.price` ;
* la documentation Google des donnees structurees `Product` ecrit `"price": "119.99"` — en
  chaine — dans ses propres exemples.

Et la sous-erreur alimentait `structured_data_google_rich_results_validation_error`,
c'est-a-dire qu'on attribuait a GOOGLE une plainte que Google ne formule pas, sur la facon dont
il recommande lui-meme d'ecrire un prix. L'elargir au `Product` aurait signale chaque fiche
produit de chaque client e-commerce, dans une famille qui doit rester credible.

Le commentaire d'origine le disait a demi-mot — « SOME VALIDATORS treat Offer.price as invalid
when encoded as a string » — et le verrou sur `SoftwareApplication` etait un confinement, pas un
oubli : garder une regle discutable sur un type rare. On retire plutot que de choisir entre
incoherente et bruyante ; la reference du projet est Ahrefs, qui ne l'applique pas.

LA LECON, et c'est la deuxieme fois de la journee. Verifier que le code fait ce qu'une note dit
ne suffit pas : il faut verifier que la note a RAISON. J'avais confirme le verrou de type, pas
la validite de la regle qu'il confinait.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_price_tests",
    REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py",
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_price_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)


def _erreurs(ld: str) -> list[str]:
    return seo_audit._schema_org_validation_errors([ld], page_url="https://site.test/p")


# --- ce qu'on ne signale plus ------------------------------------------------------------------

@pytest.mark.parametrize("nom, ld", [
    ("Product, la forme des exemples de Google",
     '{"@context":"https://schema.org","@type":"Product","name":"Chaise",'
     '"offers":{"@type":"Offer","price":"119.99","priceCurrency":"EUR"}}'),
    ("SoftwareApplication, le seul type qui etait examine",
     '{"@context":"https://schema.org","@type":"SoftwareApplication","name":"App",'
     '"offers":{"@type":"Offer","price":"0","priceCurrency":"EUR"}}'),
    ("AggregateOffer aux bornes en chaine",
     '{"@context":"https://schema.org","@type":"SoftwareApplication","name":"App",'
     '"offers":{"@type":"AggregateOffer","lowPrice":"10","highPrice":"20","offerCount":"3"}}'),
    ("liste d'offres",
     '{"@context":"https://schema.org","@type":"Product","name":"Chaise","offers":'
     '[{"@type":"Offer","price":"9.90","priceCurrency":"EUR"}]}'),
])
def test_un_prix_en_chaine_ne_leve_AUCUNE_erreur(nom, ld) -> None:
    assert _erreurs(ld) == [], "%s : %r" % (nom, _erreurs(ld))


def test_la_sous_erreur_a_disparu_du_CODE() -> None:
    """Une clé retirée du jeu d'erreurs mais laissée dans le code y reviendrait au premier
    copier-coller. On vérifie qu'elle n'existe plus nulle part dans le crawler."""
    src = (REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" /
           "seo_audit.py").read_text(encoding="utf-8")
    # Seule la note expliquant le retrait a le droit de la nommer.
    lignes = [ligne for ligne in src.splitlines()
              if "offer_price_is_string" in ligne and not ligne.strip().startswith("#")]
    assert not lignes, lignes


# --- ce qu'on signale toujours -------------------------------------------------------------------

def test_les_VRAIES_erreurs_de_donnees_structurees_restent() -> None:
    """Le contre-test sans lequel le retrait ne prouve rien : on a enlevé une règle, pas la
    famille. Un JSON sans `@type` reste une erreur schema.org dure."""
    assert "missing_type" in _erreurs(
        '{"@context":"https://schema.org","name":"Objet sans type"}')


def test_une_FAQ_sans_reponse_reste_une_erreur_rich_results() -> None:
    """C'est le nouveau déclencheur de la fixture du banc : Google exige `acceptedAnswer`,
    son test des résultats enrichis la refuse. Contrairement au prix en chaîne, c'en est
    vraiment une."""
    errs = _erreurs('{"@context":"https://schema.org","@type":"FAQPage","mainEntity":'
                    '[{"@type":"Question","name":"Une question sans reponse ?"}]}')
    assert "faq_answer_missing" in errs, errs


def test_la_famille_rich_results_a_ENCORE_de_quoi_se_declencher() -> None:
    """Retirer la cinquième entrée d'un jeu de cinq pourrait le vider sans qu'on le voie."""
    src = (REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" /
           "seo_audit.py").read_text(encoding="utf-8")
    bloc = src.split("RICH_RESULTS_ERRORS = {", 1)[1].split("}", 1)[0]
    restantes = [ligne.strip().strip('",') for ligne in bloc.splitlines() if '"' in ligne]
    assert len(restantes) >= 4, restantes
    assert "offer_price_is_string" not in restantes


# --- le banc ---------------------------------------------------------------------------------------

def test_la_fixture_du_banc_declenche_ce_qu_elle_annonce() -> None:
    """Une fixture qui vise une famille que plus rien ne déclenche mesure un silence.

    Le cas `schema-invalid` du parcours portait le prix en chaîne ; il porte maintenant la FAQ
    incomplète. Ce test relie les deux bouts : ce que la fixture émet doit produire une erreur
    du jeu rich results.
    """
    cat = (REPO_ROOT / "seo-agent-web" / "ops" / "gauntlet" / "catalogue.py").read_text(
        encoding="utf-8")
    assert "jsonld_bad_price" not in cat, "l'ancien drapeau subsiste dans le catalogue"
    assert "jsonld_faq_incomplete=True" in cat

    emit = (REPO_ROOT / "seo-agent-web" / "ops" / "gauntlet" / "emit_stacks.py").read_text(
        encoding="utf-8")
    assert "jsonld_bad_price" not in emit, "l'ancien drapeau subsiste dans l'émetteur"
    assert emit.count("jsonld_faq_incomplete") == 2, "les deux émetteurs doivent suivre"
