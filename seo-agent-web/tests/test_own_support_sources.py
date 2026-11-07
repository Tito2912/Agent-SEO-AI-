"""Public support evidence keeps prices attached to plans without exceeding its budget."""

import pytest

from backend import own_support_sources as sources


@pytest.mark.parametrize(
    "question",
    [
        "Quel est le forfait le moins cher ?",
        "Quels sont vos tarifs ?",
        "Combien coûte Noyaru ?",
        "Existe-t-il une offre gratuite ?",
        "Quelle est l'offre la moins chère ?",
        "What is the price of the paid plan?",
        "How much is Noyaru?",
        "What are my subscription limits?",
    ],
)
def test_price_vocabulary_selects_public_plan_evidence(question):
    assert sources.select_docs(question)[0] == "plans-et-quotas"


@pytest.mark.parametrize(
    "question", ["Explain how to analyse my site", "Planifier une analyse Google"]
)
def test_similar_words_do_not_select_prices(question):
    assert "plans-et-quotas" not in sources.select_docs(question)


def test_price_question_keeps_evidence_when_it_also_mentions_other_topics():
    selected = sources.select_docs("Quel forfait pour Google, GitHub, analyse et score ?")
    assert len(selected) == len(set(selected)) == 3
    assert selected[0] == "plans-et-quotas"


def test_tables_keep_each_price_with_its_plan_and_each_limit_with_its_metric():
    text, limited = sources.excerpt(
        "<h2>Plans</h2><table><thead><tr><th></th><th>Free</th><th>Solo</th>"
        "</tr></thead><tbody><tr><td><strong>Prix</strong></td><td>Gratuit</td>"
        "<td>29 € / mois</td></tr><tr><td>Projets</td><td>1</td><td>3</td>"
        "</tr></tbody></table><p>Choisissez depuis Abonnement.</p>"
    )
    assert "Prix — Free: Gratuit; Solo: 29 € / mois" in text
    assert "Projets — Free: 1; Solo: 3" in text
    assert "Choisissez depuis Abonnement." in text and not limited


def test_paragraphs_inside_tables_and_lists_are_not_duplicated():
    text, limited = sources.excerpt(
        "<h3>Details</h3><ul><li><p>Single item.</p></li></ul>"
        "<table><tr><th></th><th>Solo</th></tr>"
        "<tr><td>Price</td><td><p>29 €</p></td></tr></table>"
    )
    assert text.count("Single item.") == 1 and text.count("29 €") == 1
    assert "Details" in text and not limited


def test_table_without_headers_is_not_given_invented_column_names():
    text, limited = sources.excerpt("<table><tr><td>Solo</td><td>29 €</td></tr></table>")
    assert text == "Solo | 29 €" and not limited


def test_excerpt_budget_counts_separators_and_drops_whole_rows():
    text, limited = sources.excerpt(
        "<p>" + "a" * 1798 + "</p><table><tr><th></th><th>Solo</th></tr>"
        "<tr><td>Price</td><td>29 €</td></tr></table><p>After table</p>"
    )
    assert len(text) <= sources.MAX_EXCERPT and limited
    assert "Price" not in text and "29" not in text and "After table" not in text
    full, limited = sources.excerpt("<p>" + "a" * 1798 + "</p><p>b</p><p>c</p>")
    assert len(full) == sources.MAX_EXCERPT and full.endswith("\nb") and limited


def test_empty_or_first_oversized_block_never_claims_to_supply_evidence():
    assert sources.excerpt("<div></div>") == ("", False)
    assert sources.excerpt("<p>" + "x" * 1801 + "</p>") == ("", True)
