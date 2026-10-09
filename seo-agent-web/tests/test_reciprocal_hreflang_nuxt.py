from __future__ import annotations

import pytest

from backend import app as m

SITE = "https://fixture.test"
ITEMS = [{"page": SITE + "/en/", "field": "fr", "value": SITE + "/fr/"}]
SOURCE = """<script setup>
useHead({
  link: [
    { rel: 'canonical', href: 'https://fixture.test/en/' },
    { rel: 'alternate', hreflang: 'en', href: 'https://fixture.test/en/' },
    { rel: 'alternate', hreflang: 'de', href: 'https://fixture.test/de/' }
  ]
});
</script>
<template><main>English page</main></template>
"""


def test_nuxt_gets_its_return_annotation_without_ai():
    output, count = m._add_reciprocal_hreflang(SOURCE, ITEMS)
    assert count == 1
    assert "{ rel: 'alternate', hreflang: 'fr', href: 'https://fixture.test/fr/' }," in output
    assert "<template><main>English page</main></template>" in output
    assert m._add_reciprocal_hreflang(output, ITEMS) == (output, 0)


@pytest.mark.parametrize("source", [SOURCE.replace("/en/", "/other/"),
                                     SOURCE.replace("hreflang: 'en'", "hreflang: locale")
                                           .replace("hreflang: 'de'", "hreflang: locale"),
                                     SOURCE.replace("rel: 'alternate'", "rel: 'stylesheet'")])
def test_unrelated_or_computed_objects_are_not_guessed(source):
    assert m._add_reciprocal_hreflang(source, ITEMS) == (source, 0)


def test_existing_code_with_a_different_destination_is_not_overwritten():
    source = SOURCE.replace("hreflang: 'de'", "hreflang: 'fr'")
    assert m._add_reciprocal_hreflang(source, ITEMS) == (source, 0)


def test_object_property_order_and_quotes_are_preserved():
    source = SOURCE.replace("{ rel: 'alternate', hreflang: 'en', href: 'https://fixture.test/en/' }",
                            '{ href: "https://fixture.test/en/", hreflang: "en", rel: "alternate" }')
    output, count = m._add_reciprocal_hreflang(source, ITEMS)
    assert count == 1
    assert '{ href: "https://fixture.test/fr/", hreflang: "fr", rel: "alternate" },' in output


def test_ambiguous_report_is_refused_before_any_file_read(monkeypatch):
    monkeypatch.setattr(m, "_github_api_get", lambda *a, **kw: pytest.fail("GitHub read"))
    prep = m._prepare_issue_fix(
        issue_key="missing_reciprocal_hreflang",
        issues={"missing_reciprocal_hreflang": {"evidence": {"kind": "page_values", "items": ITEMS}}},
        impacted=[SITE + "/fr/"], all_paths=["pages/en.vue"], site_name="fixture.test",
        owner="fixture", repo_name="fixture", branch="main", token="unused",
        pages=[{"url": SITE + "/en/", "hreflang": {"fr": SITE + "/another/"}}])
    assert prep["refusal"] and "fr" in prep["refusal"]
    assert prep["link_rewriter"] is None


def test_mixed_batch_keeps_repairable_items_and_explains_the_conflict():
    conflict = {"page": SITE + "/other/", "field": "fr", "value": SITE + "/fr/"}
    prep = m._prepare_issue_fix(
        issue_key="missing_reciprocal_hreflang",
        issues={"missing_reciprocal_hreflang": {"evidence": {"kind": "page_values", "items": [*ITEMS, conflict]}}},
        impacted=[SITE + "/fr/"], all_paths=[], site_name="fixture.test",
        owner="fixture", repo_name="fixture", branch="main", token="unused",
        pages=[{"url": SITE + "/other/", "hreflang": {"fr": SITE + "/another/"}}])
    assert not prep["refusal"]
    assert prep["evidence"] == [SITE + "/en/"]
    assert SITE + "/other/" in prep["side_effects"]


def test_evidence_target_wins_over_the_flagged_source_with_one_file_limit():
    prep = m._prepare_issue_fix(
        issue_key="missing_reciprocal_hreflang",
        issues={"missing_reciprocal_hreflang": {"evidence": {"kind": "page_values", "items": ITEMS}}},
        impacted=[SITE + "/fr/"], all_paths=["nuxt.config.ts", "pages/fr.vue", "pages/en.vue"],
        site_name="fixture.test", owner="fixture", repo_name="fixture", branch="main", token="unused")
    assert prep["targets_override"] == ["pages/en.vue"]


@pytest.mark.parametrize("item", [{**ITEMS[0], "field": "fr'"},
                                  {**ITEMS[0], "value": "javascript:alert(1)"},
                                  {**ITEMS[0], "value": SITE + "/bad\npath/"},
                                  {**ITEMS[0], "value": SITE + "/</script><script>alert(1)</script>"}])
def test_untrusted_evidence_cannot_break_source_syntax(item):
    assert m._add_reciprocal_hreflang(SOURCE, [item]) == (SOURCE, 0)


def test_contradictory_evidence_for_the_same_code_abstains():
    assert m._add_reciprocal_hreflang(SOURCE, [*ITEMS, {**ITEMS[0], "value": SITE + "/other/"}]) == (SOURCE, 0)


def test_url_apostrophes_are_escaped_in_nuxt_strings():
    output, count = m._add_reciprocal_hreflang(SOURCE, [{**ITEMS[0], "value": SITE + "/it's-french/"}])
    assert count == 1 and "href: 'https://fixture.test/it\\'s-french/'" in output


def test_a_last_object_without_comma_stays_valid_after_insertion():
    source = SOURCE.replace("    { rel: 'alternate', hreflang: 'en', href: 'https://fixture.test/en/' },\n", "")
    output, count = m._add_reciprocal_hreflang(source, ITEMS)
    assert count == 1
    assert "href: 'https://fixture.test/de/' },\n" in output
    assert "href: 'https://fixture.test/fr/' }\n" in output


@pytest.mark.parametrize("source", [SOURCE.replace("useHead({", "const unused = {"),
                                  SOURCE.replace("  link: [", "  meta: ["),
                                  SOURCE.replace("    { rel: 'canonical'", "    ...sharedLinks,\n    { rel: 'canonical'"),
                                  SOURCE + "\nuseHead({ title: 'Another head' });\n"])
def test_only_one_literal_usehead_link_array_is_supported(source):
    assert m._add_reciprocal_hreflang(source, ITEMS) == (source, 0)


def test_unicode_escaped_codes_are_not_misread_as_a_missing_language():
    source = SOURCE.replace("hreflang: 'de'", r"hreflang: '\u0066r'")
    assert m._add_reciprocal_hreflang(source, ITEMS) == (source, 0)


def test_html_with_mixed_attribute_order_cannot_duplicate_an_existing_language():
    source = '''<link rel="canonical" href="https://fixture.test/en/" />
<link rel="alternate" hreflang="en" href="https://fixture.test/en/" />
<link href="https://fixture.test/fr/" hreflang="fr" rel="alternate" />
'''
    assert m._add_reciprocal_hreflang(source, ITEMS) == (source, 0)


def test_html_return_url_is_attribute_escaped():
    source = '''<link rel="canonical" href="https://fixture.test/en/" />
<link rel="alternate" hreflang="en" href="https://fixture.test/en/" />
'''
    output, count = m._add_reciprocal_hreflang(source, [{**ITEMS[0], "value": SITE + '/fr/?a="quoted"&b=1'}])
    assert count == 1 and '&quot;quoted&quot;&amp;b=1' in output


def test_html_entity_in_an_existing_code_cannot_create_a_duplicate_language():
    source = '''<link rel="canonical" href="https://fixture.test/en/" />
<link rel="alternate" hreflang="f&#114;" href="https://fixture.test/fr/" />
'''
    assert m._add_reciprocal_hreflang(source, ITEMS) == (source, 0)
