"""Native editorial starter packs. Read-only, no model, inbox, client or network access."""
from urllib.parse import urlencode

try:
    from . import content_library, own_seo, seo_watch
    from .own_social_copy import TOPICS, strings
except ImportError:
    import content_library
    import own_seo
    import seo_watch
    from own_social_copy import TOPICS, strings

PATH = '/settings/system/social'
VERSION = 'native-editorial-v1'
# The fixed starter copy was reviewed against these product-document editions.
# A future doc or approved summary change requires editorial review, not blind reuse.
PRODUCT_REVIEWS = {
    'fr': {'lire-le-rapport': 'a51c703ad931483d4b7cf6a9cbd104d626f9572bdfb3458d89c5eb9f45bcdaca',
           'verifier-une-correction': '1ff56caa9c72692efdf46cf10467215f95b7af3ea715dae129a1071bf6190d61',
           'demarrer': '515745c2a45c830125466caca00834dc14e7f4ca39300636aadd9e6272101cf6'},
    'en': {'lire-le-rapport': '08f849c8eb81365ee08a41b5f29549e15e55edfc9af5ab850d33726d1fa20938',
           'verifier-une-correction': '7d0f5aca9b325522b6566cbdac8b478f3a0fe2944b177942292daaf5f6816926',
           'demarrer': 'b2d52d80abe6ed3733b26e1545467d82874e367b123f63a94ae285232b0c3eb3'},
}


def options(network, goal, audience):
    if network not in {'instagram', 'tiktok'} or goal not in {'discovery', 'help', 'product'} or audience not in {'freelance', 'business'}:
        raise ValueError('invalid_choice')


def link(network, goal, audience, lang, *, topic=''):
    return PATH + '?' + urlencode({'network': network, 'goal': goal, 'audience': audience, 'lang': lang, **({'topic': topic} if topic else {})}) + ('#production-pack' if topic else '#social-ideas')


def plan(db, *, network='instagram', goal='discovery', audience='freelance', lang='fr'):
    options(network, goal, audience)
    lang = 'en' if lang == 'en' else 'fr'
    copy = strings(lang)
    at = own_seo.clock()
    knowledge = {s['key']: s for s in seo_watch.catalog(db, now=at, lang=lang)['sources']}
    ideas, deferred = [], []
    for topic in TOPICS:
        value = topic[lang]
        refs, missing = [], ''
        if topic['doc']:
            page = content_library.get_doc(topic['doc'], lang)
            if page and page['source_sha256'] == PRODUCT_REVIEWS[lang][topic['doc']]:
                refs.append({'key': topic['doc'], 'title': page['title'], 'kind': 'product',
                    'url': 'https://noyaru.com' + ('/en' if lang == 'en' else '') + '/docs/' + topic['doc'],
                    'fingerprint': page['source_sha256'], 'checked_at': '', 'text': ''})
            else:
                missing = 'product'
        for key in topic['sources']:
            source = knowledge.get(key)
            expected = getattr(seo_watch.REGISTRY[key], 'en' if lang == 'en' else 'fr')
            if not source or source['status'] != 'current' or source['guidance'] != expected:
                missing = 'guidance' if source and source['status'] == 'current' else 'official'
                continue
            refs.append({k: source[k] for k in ('key', 'title', 'url', 'fingerprint', 'checked_at')}
                        | {'kind': 'official', 'text': source['guidance']})
        if goal not in topic['goals']:
            continue
        item = {'key': topic['key'], 'title': value['title'], 'why': value['why'],
            'audience_reason': copy['audience_reasons'][audience], 'format': copy['formats'][network],
            'hook': value['hooks'][network], 'sources': refs, 'ready': not missing, 'missing': missing,
            'sequence': value['cards'] if network == 'instagram' else value['scenes'],
            'caption': value['caption'], 'cta': copy['ctas'][goal],
            'production': [copy['visuals'][network], copy['demo'] if topic['demo'] else copy['simple']],
            'measure': copy['metrics'][goal], 'blog': None,
            'href': link(network, goal, audience, lang, topic=topic['key'])}
        # Exact topic match, actual translation only. An unrelated or newest article is never substituted.
        if topic.get('blog'):
            page = content_library.get_article(topic['blog'], lang)
            if page:
                item['blog'] = {'title': page['title'], 'url': 'https://noyaru.com' + ('/en' if lang == 'en' else '') + '/ressources-seo/' + topic['blog']}
        stable = {k: v for k, v in item.items() if k not in {'href', 'sources'}}
        stable['sources'] = [{k: v for k, v in r.items() if k != 'checked_at'} for r in refs]
        item['version'] = own_seo.digest([VERSION, network, goal, audience, lang, stable])
        item['download'] = PATH + '/' + item['key'] + '/download?' + urlencode({'network': network,
            'goal': goal, 'audience': audience, 'lang': lang, 'version': item['version']})
        (ideas if item['ready'] else deferred).append(item)
    # Goal-specific order, with an intentionally small and inspectable set of starter ideas.
    order = {'discovery': ['score', 'geo', 'essentials'], 'help': ['verify', 'essentials', 'first', 'geo'], 'product': ['first', 'score', 'verify']}[goal]
    ideas.sort(key=lambda i: order.index(i['key']))
    return {'ideas': ideas, 'deferred': deferred, 'as_of': seo_watch.iso(at), 'network': network,
        'goal': goal, 'audience': audience, 'lang': lang, 'copy': copy,
        'back': link(network, goal, audience, lang)}


def export(state, item):
    """A preparation record, never a transport payload or approval to publish."""
    c = state['copy']
    def text(value):
        return str(value).replace('\\', '\\\\').replace('[', '\\[').replace(']', '\\]').replace('<', '&lt;').replace('>', '&gt;')
    lines = ['# ' + text(item['title']), '', c['footer'], '',
        f"{c['as_of']} : {state['as_of']}", f"{c['version']} : {item['version']}",
        f"{c['network']} : {state['network']}", f"{c['goal']} : {c['goals'][state['goal']]}",
        f"{c['audience']} : {c['audiences'][state['audience']]}", '', c['basis'], '',
        '## ' + c['why'], text(item['why']), text(item['audience_reason']), '',
        '## ' + c['hook'], text(item['hook']), '', '## ' + c['sequence'], item['format'], '']
    for part in item['sequence']:
        lines.extend(['### ' + text(part[0]), *[text(p) for p in part[1:]], ''])
    lines.extend(['## ' + c['caption'], text(item['caption']), '', '## ' + c['cta'], text(item['cta']), '',
        '## ' + c['make'], *[text(p) for p in item['production']], '', '## ' + c['measure'],
        text(item['measure']), c['measure_info'], '', '## ' + c['sources']])
    for ref in item['sources']:
        lines.extend(['- ' + text(ref['title']) + ' : ' + ref['url'], '  ' + (c['source_product'] if ref['kind'] == 'product' else c['source_official']),
            '  ' + c['checked'] + ' : ' + ref['checked_at'] if ref['checked_at'] else '', '  SHA-256 : ' + ref['fingerprint']])
    if item['blog']:
        lines.extend(['', '## ' + c['blog_optional'], c['blog_info'], item['blog']['url']])
    lines.extend(['', c['changed'], ''])
    return '\n'.join(lines)
