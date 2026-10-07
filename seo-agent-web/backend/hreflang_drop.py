"""Remove one false language declaration in a proved two-language static group."""
from html.parser import HTMLParser
from urllib.parse import urlsplit

try:
    from . import hreflang_canonical as literal, sitemap_https
except ImportError:
    import hreflang_canonical as literal
    import sitemap_https

def document(raw, canonical, inspect, identify):
    parsed = literal.document(raw, canonical, inspect, identify)
    return parsed if parsed and inspect(raw, allow_noindex=True)['canonicals'] == [canonical] else None


def matches(raw, canonical, observed, inspect, identify):
    parsed, known = document(raw, canonical, inspect, identify), literal.snapshot(observed)
    return bool(parsed and known and known[0] == known[1] == parsed['language'] and known[2] == parsed['alternates'])


def witness(item, pages, identify, host):
    if not isinstance(item, dict) or not all(literal.direct(item.get(k), identify, host) for k in ('page', 'value')):
        return None
    source, target = item['page'], item['value']
    if source == target or not isinstance(item.get('field'), str):
        return None
    verified, refused = sitemap_https.verified_urls([source, target], pages, identify, host=host)
    if refused or verified != [source, target]:
        return None
    observed = {}
    for url in (source, target):
        rows = [row for row in pages or [] if isinstance(row, dict) and identify(row.get('url')) == url]
        snapshots = [literal.snapshot(row) for row in rows]
        if (not snapshots or not snapshots[0] or any(value != snapshots[0] for value in snapshots)
                or any(row.get('url') != url or row.get('final_url') != url or row.get('canonical') != url for row in rows)):
            return None
        observed[url] = rows[0]
    source_lang, source_served, source_pairs = literal.snapshot(observed[source])
    target_lang, target_served, target_pairs = literal.snapshot(observed[target])
    if (source_served != source_lang or target_served != target_lang or item['field'] != source_lang
            or source_lang.split('-', 1)[0] == target_lang.split('-', 1)[0]
            or dict(source_pairs) != {source_lang: target, target_lang: target}
            or dict(target_pairs) != {source_lang: source, target_lang: target}):
        return None
    return observed


def verified_items(items, pages, identify, *, host):
    if not isinstance(items, list):
        return [], ['annotations inconnues']
    grouped, accepted, refused = {}, [], []
    for item in items:
        page = item.get('page') if isinstance(item, dict) else None
        grouped.setdefault(page if isinstance(page, str) else '', []).append(item)
    for page, proposals in grouped.items():
        unique = []
        for item in proposals:
            if item not in unique:
                unique.append(item)
        if len(unique) != 1 or not witness(unique[0], pages, identify, host):
            refused.append(page or 'page inconnue')
        else:
            accepted.append({k: unique[0][k] for k in ('page', 'field', 'value')})
    return accepted, sorted(set(refused))


def rewrite(raw, item, observed, inspect, identify):
    try:
        source, code, target = (item[k] for k in ('page', 'field', 'value'))
        host = urlsplit(source).netloc.lower()
        if source == target or not all(literal.direct(url, identify, host) for url in (source, target)):
            return raw, 0
    except (KeyError, TypeError, ValueError):
        return raw, 0
    if not matches(raw, source, observed, inspect, identify):
        return raw, 0
    parsed = document(raw, source, inspect, identify)
    other = [c for c, href in parsed['alternates'] if c != code and href == target and c != 'x-default']
    if (code != parsed['language'] or len(parsed['alternates']) != 2 or len(other) != 1
            or other[0].split('-', 1)[0] == code.split('-', 1)[0]
            or dict(parsed['alternates']) != {code: target, other[0]: target}):
        return raw, 0
    lines, spans = raw.splitlines(keepends=True), []

    class Removal(HTMLParser):
        def handle_starttag(self, tag, attrs):
            values = dict(attrs)
            if (tag == 'link' and str(values.get('hreflang') or '').strip().lower() == code
                    and values.get('href') == target):
                text, (line, column) = self.get_starttag_text(), self.getpos()
                start = sum(map(len, lines[:line - 1])) + column
                if raw[start:start + len(text)] == text:
                    spans.append((start, start + len(text)))

    parser = Removal()
    parser.feed(raw)
    parser.close()
    if len(spans) != 1:
        return raw, 0
    start, end = spans[0]
    return raw[:start] + raw[end:], 1
