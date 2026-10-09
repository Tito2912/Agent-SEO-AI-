"""A passing local browser receipt must not accept collateral edits."""
import copy

import pytest

from ops.gauntlet import anchor_text_rewriter_check as b


def test_the_golden_fixture_has_one_exact_edit_and_replays_without_writes():
    before, after = b.check_rewrite()
    assert after == before.replace(b.OPENING, b.OPENING[:-1] + b.LABEL + '>', 1)


@pytest.mark.parametrize('case', ['zero', 'wrong_count', 'wrong_name', 'other_href', 'comment', 'delete_template', 'replay'])
def test_the_benchmark_rejects_a_false_rewriter_success(case):
    def bad(raw, items):
        if case == 'replay':
            return raw.replace(b.OPENING, b.OPENING[:-1] + b.LABEL + '>', 1), 1
        new, count = b.anchor_text.rewrite(raw, items)
        if case == 'zero':
            return raw, 0
        if case == 'wrong_count':
            return new, 2
        if case == 'wrong_name':
            return new.replace(b.LABEL, ' aria-label="Other"'), count
        if case == 'other_href':
            return new.replace('href="/other"', 'href="/contact"'), count
        if case == 'comment':
            return new.replace('<!-- <a href="/contact">', '<!-- <a href="/contact"' + b.LABEL + '>'), count
        return new.replace('<template>', '<div>').replace('</template>', '</div>'), count

    with pytest.raises(AssertionError):
        b.check_rewrite(bad)


def facts():
    return {key: {'href': '/contact' if key == 'empty' else '/' + key, 'aria_label': None,
                  'labelledby': 'name' if key == 'already-named' else None, 'inner': '<svg />',
                  'text': '', 'rectangle': {'width': 28}} for key in
            ['empty', 'already-named', 'duplicate-empty', 'image', 'decoy', 'unproved']}


def test_exact_dom_facts_are_required_except_for_the_one_added_name():
    before, after = facts(), facts()
    after['empty']['aria_label'] = b.NAME
    b.check_dom(before, after)
    assert before == facts()


@pytest.mark.parametrize('case', ['missing', 'extra', 'no_label', 'wrong_label', 'old_label', 'href', 'geometry',
                                 'image_content', 'unproved_label', 'labelledby', 'text'])
def test_browser_facts_cannot_hide_collateral_changes(case):
    before, after = facts(), facts()
    after['empty']['aria_label'] = b.NAME
    if case == 'missing':
        del after['unproved']
    elif case == 'extra':
        after['extra'] = copy.deepcopy(after['empty'])
    elif case == 'old_label':
        before['empty']['aria_label'] = b.NAME
    elif case in {'no_label', 'wrong_label'}:
        after['empty']['aria_label'] = None if case == 'no_label' else 'Other'
    else:
        key, field, value = {'href': ('decoy', 'href', '/contact'), 'geometry': ('empty', 'rectangle', {'width': 29}),
                             'image_content': ('image', 'inner', '<img />'),
                             'unproved_label': ('unproved', 'aria_label', b.NAME),
                             'labelledby': ('already-named', 'labelledby', None), 'text': ('empty', 'text', b.NAME)}[case]
        after[key][field] = value
    with pytest.raises(AssertionError):
        b.check_dom(before, after)
