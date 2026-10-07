"""Literal anchor edits must not confuse data, markup and an existing name."""
import html

import pytest

from backend import app as m

ITEM = {'page': 'https://site.test/', 'field': '/contact', 'value': 'Contactez-nous'}
LINK = '<a href="/contact"><svg class="icon" /></a>'
LABEL = ' aria-label="Contactez-nous"'


def rewrite(raw, items=None):
    return m._poser_aria_label_sur_liens_sans_ancre(raw, [ITEM] if items is None else items)


@pytest.mark.parametrize('raw', [
    '<a data-href="/contact" href="/other"><svg /></a>',
    '<a data-href="/contact"><svg /></a>',
    '<a x:href="/contact" href="/other"><svg /></a>',
    "<a data-example='href=\"/contact\"' href='/other'><svg /></a>",
    '<!-- ' + LINK + ' -->',
    '<script type="application/json">{"link":\'' + LINK + '\'}</script>',
    '<script>const link = \'' + LINK + '\';</script>',
    '<style>p::after { content: \'' + LINK + '\'; }</style>',
    '<template>' + LINK + '</template>',
    '<noscript>' + LINK + '</noscript>',
    '<textarea>' + LINK + '</textarea>',
    '<title>' + LINK + '</title>',
    '<xmp>' + LINK + '</xmp>',
    '<iframe>' + LINK + '</iframe>',
    '<noembed>' + LINK + '</noembed>',
    '<plaintext>' + LINK + '</plaintext>',
    '<div hidden>' + LINK + '</div>',
    '<div inert>' + LINK + '</div>',
    '<div aria-hidden="true">' + LINK + '</div>',
    '<a href="/contact" hidden><svg /></a>',
    '<a href="/contact" aria-hidden="true"><svg /></a>',
    '<a href="/contact" style="display:none"><svg /></a>',
    '<a href="/contact" onclick="go()"><svg /></a>',
    '<a href="/contact" aria-labelledby="contact-name"><svg /></a>',
    '<a href="/contact" aria-labelledby=""><svg /></a>',
    '<a href="/contact" aria-label=""><svg /></a>',
    '<a href="/contact" title=""><svg /></a>',
    '<a href="/contact"><img src="contact.png" alt="Contact" /></a>',
    '<a href="/contact"><svg aria-label="Contact" /></a>',
    '<a href="/contact"><svg aria-labelledby="icon-title" /></a>',
    '<a href="/contact"><svg><title>Contact</title></svg></a>',
    '<a href="/contact"><svg><use href="#contact-icon" /></svg></a>',
    '<a href="/contact"><input type="image" alt="Contact" /></a>',
    '<a href="/contact"><button></button></a>',
    '<a href="/contact"><object data="icon.svg"></object></a>',
    '<a href="/contact"><span title="Contact"></span></a>',
    '<a href="/contact"><span aria-label="Contact"></span></a>',
    '<a href="/contact"><span style="display:none">Contact</span></a>',
    '<a href="/contact"><!-- decoy <a href="/other"> -->Contact</a>',
    '<a href="/contact"><span>{label}</span></a>',
    '<a href="/contact"><span v-text="name"></span></a>',
    '<a href="/contact"><span x-text="name"></span></a>',
    '<a href="/contact"><span data-bind="text: name"></span></a>',
    '<a href="/contact"><custom-icon /></a>',
    '<a href="/contact"><Icon /></a>',
])
def test_decoys_existing_names_and_uncertain_content_are_unchanged(raw):
    assert rewrite(raw) == (raw, 0)


@pytest.mark.parametrize('raw', [
    '<a href="/contact" href="/other"><svg /></a>',
    '<a href="/contact" HREF="/contact"><svg /></a>',
    '<a href="/contact" class="x" class="y"><svg /></a>',
    '<a href=/contact><svg /></a>',
    '<a href="/contact" onclick={() => go()}><svg /></a>',
    '<a href="/contact"><svg></a>',
    '<a href="/contact"><svg />',
    '<a href="/contact"><a href="/other"></a></a>',
    '<a href="/contact" />',
    '<a href="/contact"><img src="x" alt="Contact"></img></a>',
    '<div>' + LINK,
    LINK + '</div>',
    '<a href="/contact"class="icon"><svg /></a>',
    '<a href="/contact" data-name="{name}"><svg /></a>',
    '<a href="/contact"><% include icon %></a>',
    '<a href="/contact">' + (' ' * 80_000) + '</a>',
    LINK + '<!-- unfinished',
], ids=lambda raw: raw[:100])
def test_ambiguous_or_incomplete_source_refuses_the_entire_edit(raw):
    assert rewrite(raw) == (raw, 0)


@pytest.mark.parametrize('name', ['Contact', ' ', None, False, 42, ['Contact'], {'label': 'Contact'},
                                 '\x00Contact', 'Contact\x7f', 'Contact\ud800'])
def test_conflicting_or_untyped_names_cannot_write(name):
    items = [dict(ITEM), dict(ITEM, value=name)]
    assert rewrite(LINK, items) == (LINK, 0)
    assert items == [ITEM, dict(ITEM, value=name)]


@pytest.mark.parametrize('items', [[], {}, 'unknown', [None], [False], [dict(ITEM, field=42)],
                                  [dict(ITEM, field=None)], [dict(ITEM, page=False)],
                                  [dict(ITEM, page='')]])
def test_unknown_evidence_is_not_coerced_to_a_label(items):
    assert rewrite(LINK, items) == (LINK, 0)


@pytest.mark.parametrize('named', [
    '<a href="/contact">Contact</a>',
    '<a href="/contact" aria-label="Contact"><svg /></a>',
    '<a href="/contact" title="Contact"><svg /></a>',
    '<a href="/contact"><img src="x.png" alt="Contact" /></a>',
    '<a href="/contact" aria-labelledby="name"><svg /></a>',
])
@pytest.mark.parametrize('before', [False, True])
def test_a_current_named_link_exonerates_the_same_literal_href(named, before):
    raw = named + '\n' + LINK if before else LINK + '\n' + named
    assert rewrite(raw) == (raw, 0)


@pytest.mark.parametrize('opening,ending', [
    ('<a href="/contact">', '</a>'),
    ("<a href='/contact'>", '</a>'),
    ('<A HREF="/contact">', '</A>'),
    ('<a class="icon" href = "/contact" data-id="1">', '</a >'),
    ('<a data-href="/other" href="/contact">', '</a>'),
    ('<a data-title="incidental" href="/contact">', '</a>'),
    ('<a title-data="incidental" href="/contact">', '</a>'),
    ('<a data-example="x > y" href="/contact">', '</a>'),
    ('<a data-example=\'href="/other"\' href="/contact">', '</a>'),
    ('<a\n href="/contact"\n class="icon">', '</a>'),
])
@pytest.mark.parametrize('newline', ['\n', '\r\n'])
def test_only_the_aria_attribute_is_inserted_preserving_every_other_byte(opening, ending, newline):
    prefix = '<!-- ' + LINK + ' -->' + newline + '<header>' + newline
    raw = prefix + opening + '<svg class="icon" />' + ending + newline + '</header>'
    quote = "'" if "href='" in opening else '"'
    label = ' aria-label=' + quote + ITEM['value'] + quote
    href_end = opening.index('/contact') + len('/contact') + 1
    golden = prefix + opening[:href_end] + label + opening[href_end:] + '<svg class="icon" />' + ending + newline + '</header>'
    output, count = rewrite(raw)
    assert (output, count) == (golden, 1)
    assert rewrite(output) == (output, 0)
    assert rewrite(raw, [ITEM, dict(ITEM)]) == (golden, 1)


@pytest.mark.parametrize('inner', ['', ' \n\t', '&nbsp;', '<svg />', '<i />', '<img src="x.png" alt="" />',
                                   '<!-- ' + LINK + ' -->', '<span><svg><path d="M0 0" /></svg></span>'])
def test_a_literal_empty_link_can_still_receive_a_name(inner):
    raw = '<a href="/contact">' + inner + '</a>'
    assert rewrite(raw) == ('<a href="/contact"' + LABEL + '>' + inner + '</a>', 1)


@pytest.mark.parametrize('quote', ['"', "'"])
@pytest.mark.parametrize('name', ["L'atelier & l'equipe", 'Contact <atelier> "principal"',
                                 'Nous ecrire ' + ('a' * 100), 'Notre ancien site http://site.test'])
def test_escaping_and_existing_post_rewrite_guards_preserve_the_measured_name(quote, name):
    raw = '<a href=' + quote + '/contact' + quote + '><svg /></a>'
    expected = '<a href=' + quote + '/contact' + quote + ' aria-label=' + quote + html.escape(name, quote=True) + quote + '><svg /></a>'
    output, count = rewrite(raw, [dict(ITEM, value=name)])
    assert (output, count) == (expected, 1)
    for guard in (m._enforce_length_ceilings, m._forbid_https_downgrade, m._escape_quotes_in_written_values):
        output, _ = guard(output, raw)
    assert output == expected


def test_decoded_href_matches_the_evidence_without_changing_its_original_entities():
    raw = '<a href="/contact?from=nav&amp;mode=icon"><svg /></a>'
    item = dict(ITEM, field='/contact?from=nav&mode=icon')
    assert rewrite(raw, [item]) == (raw.replace('"><svg', '"' + LABEL + '><svg'), 1)


def test_other_attributes_and_an_attribute_containing_markup_are_not_rewritten():
    raw = "<div data-example='" + LINK + "'>" + LINK + '</div>'
    expected = "<div data-example='" + LINK + "'>" + LINK.replace('"><svg', '"' + LABEL + '><svg') + '</div>'
    assert rewrite(raw) == (expected, 1)


def test_every_empty_occurrence_is_named_without_touching_an_unselected_target():
    other = '<a href="/other"><svg /></a>'
    raw = LINK + '\n' + other + '\n' + LINK
    expected = LINK.replace('"><svg', '"' + LABEL + '><svg')
    assert rewrite(raw) == (expected + '\n' + other + '\n' + expected, 2)


def test_insertions_never_exceed_the_source_byte_budget():
    raw = '<!--' + ('x' * (79_999 - len(LINK) - 7)) + '-->' + LINK
    assert len(raw.encode('utf-8')) == 79_999
    assert rewrite(raw) == (raw, 0)


def test_the_existing_prepared_callback_uses_the_same_literal_guard():
    key = 'links_with_no_anchor_text'
    plan = m._prepare_issue_fix(issue_key=key, issues={key: {'count': 1, 'examples': [ITEM['page']],
        'evidence': {'kind': 'page_values', 'items': [ITEM]}}}, impacted=[ITEM['page']],
        all_paths=['index.html'], site_name='site.test', owner='fixture', repo_name='fixture',
        branch='baseline', token='unused')
    # The unverified legacy callback is now blocked; the underlying literal guard is retained.
    assert plan['refusal'] and plan['anchor_text_items'] == [] and plan['link_rewriter'] is None
    assert not plan['rewriter_ai_fallback']
    raw = '<!-- ' + LINK + ' -->' + LINK
    expected = '<!-- ' + LINK + ' -->' + LINK.replace('"><svg', '"' + LABEL + '><svg')
    assert rewrite(raw) == (expected, 1)


def test_both_source_and_evidence_are_not_mutated():
    items = [dict(ITEM), dict(ITEM, field='/other', value='Autre page')]
    raw = LINK + '<a href="/other"><i /></a>'
    expected = LINK.replace('"><svg', '"' + LABEL + '><svg') + '<a href="/other" aria-label="Autre page"><i /></a>'
    assert rewrite(raw, items) == (expected, 2)
    assert items == [ITEM, dict(ITEM, field='/other', value='Autre page')]
