"""Rewrite one alias only within an already-declared coherent bilingual group."""
import html
from html.parser import HTMLParser
import re
from urllib.parse import urlsplit

try:
    from . import hreflang_lang, sitemap_https
except ImportError:
    import hreflang_lang
    import sitemap_https

ATTR = re.compile(r'''([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+)))?''')


def direct(value, identify, host):
    if not isinstance(value, str) or identify(value) != value:
        return False
    try:
        parsed = urlsplit(value)
        return (parsed.scheme == 'https' and parsed.netloc.lower() == host and not parsed.username
                and not parsed.password and not parsed.fragment and parsed.port is None)
    except ValueError:
        return False


def snapshot(row):
    if not isinstance(row, dict) or any(k not in row for k in ('canonical', 'lang', 'served_lang', 'hreflang', 'hreflang_raw')):
        return None
    language, served = row['lang'], row['served_lang']
    if not isinstance(language, str) or not hreflang_lang.CODE.fullmatch(language.strip()) or language.lower() == 'x-default':
        return None
    if served is not None and (not isinstance(served, str) or served.strip().lower() != language.strip().lower()):
        return None
    pairs = hreflang_lang.alternates(row['hreflang_raw'], row['hreflang'])
    return (language.strip().lower(), served.strip().lower() if isinstance(served, str) else None, pairs) if pairs else None


def witness(pair, pages, identify, host):
    if not isinstance(pair, dict) or any(not direct(pair.get(k), identify, host) for k in ('page', 'from', 'to')):
        return None
    source, old, new = (pair[k] for k in ('page', 'from', 'to'))
    if len({source, old, new}) != 3:
        return None
    observed = {}
    for url, canonical in ((source, source), (old, new), (new, new)):
        rows = [row for row in pages or [] if isinstance(row, dict) and identify(row.get('url')) == url]
        if not rows or any(type(row.get('status_code')) is not int or row['status_code'] != 200
                or row.get('error') or row.get('blocked_by_host') or row.get('url') != url or row.get('final_url') != url
                or row.get('canonical') != canonical or row.get('redirect_chain') or row.get('redirect_statuses')
                or str(row.get('content_type') or '').split(';', 1)[0].strip().lower() not in {'text/html', 'application/xhtml+xml'}
                or any(sitemap_https.excluded(row.get(k)) for k in ('meta_robots', 'x_robots_tag')) for row in rows):
            return None
        snapshots = [snapshot(row) for row in rows]
        if not snapshots[0] or any(value != snapshots[0] for value in snapshots):
            return None
        observed[url] = rows[0]
    source_lang, served, annotations = snapshot(observed[source])
    codes = [code for code, href in annotations if href == old and code != 'x-default']
    if (len(annotations) != 2 or len(codes) != 1 or served != source_lang
            or hreflang_lang.self_language(annotations, source) != source_lang):
        return None
    target_lang = codes[0]
    group = {source_lang: source, target_lang: new}
    for url in (old, new):
        language, served, annotations = snapshot(observed[url])
        if language != target_lang or dict(annotations) != group or (url == new and served != target_lang):
            return None
    return observed


def verified_pairs(pairs, pages, identify, *, host):
    if not isinstance(pairs, list):
        return [], ['paire inconnue']
    grouped, accepted, refused = {}, [], []
    for pair in pairs:
        page = pair.get('page') if isinstance(pair, dict) else None
        grouped.setdefault(page if isinstance(page, str) else '', []).append(pair)
    for page, proposals in grouped.items():
        unique = []
        for pair in proposals:
            if pair not in unique:
                unique.append(pair)
        if len(unique) != 1 or not witness(unique[0], pages, identify, host):
            refused.append(page or 'page inconnue')
        else:
            accepted.append({key: unique[0][key] for key in ('page', 'from', 'to')})
    return accepted, sorted(set(refused))


def href_span(text, attrs):
    prefix = re.match(r'<link\b', text, re.I)
    close = re.search(r'\s*/?>$', text)
    if not prefix or not close:
        return None
    tokens, span, position = [], None, prefix.end()
    for match in ATTR.finditer(text, position, close.start()):
        if text[position:match.start()].strip():
            return None
        name = match[1].lower()
        group = next((i for i in (2, 3, 4) if match[i] is not None), None)
        value = html.unescape(match[group]) if group else None
        tokens.append((name, value))
        if name == 'href':
            if group not in (2, 3) or span is not None:
                return None
            span = (match.start(group), match.end(group))
        position = match.end()
    return span if not text[position:close.start()].strip() and tokens == attrs else None


def document(raw, canonical, inspect, identify):
    if not isinstance(raw, str) or len(raw.encode('utf-8')) > 80_000 or not sitemap_https.literal_destination(raw, canonical, inspect, identify):
        return None
    parsed, lines = inspect(raw, allow_noindex=True), raw.splitlines(keepends=True)

    class Alternates(HTMLParser):
        def __init__(self):
            super().__init__()
            self.stack, self.rows, self.mapped, self.spans, self.ambiguous = [], [], {}, {}, False

        def handle_starttag(self, tag, attrs):
            values = dict(attrs)
            if tag == 'link' and 'hreflang' in values:
                text, (line, column) = self.get_starttag_text(), self.getpos()
                start = sum(map(len, lines[:line - 1])) + column
                span = href_span(text, attrs)
                self.ambiguous |= (self.stack != ['html', 'head'] or len(values) != len(attrs)
                    or str(values.get('rel') or '').strip().lower() != 'alternate' or not span
                    or raw[start:start + len(text)] != text or any(k in values for k in ('media', 'type', 'as')))
                code = str(values.get('hreflang') or '').strip().lower()
                self.rows.append({'hreflang': values.get('hreflang'), 'href': values.get('href')})
                self.mapped[code] = values.get('href')
                if span:
                    self.spans[code] = (start + span[0], start + span[1])
            if tag not in hreflang_lang.VOID:
                self.stack.append(tag)

        def handle_endtag(self, tag):
            if tag not in hreflang_lang.VOID:
                if not self.stack or self.stack[-1] != tag:
                    self.ambiguous = True
                else:
                    self.stack.pop()

    parser = Alternates()
    parser.feed(raw)
    parser.close()
    pairs = hreflang_lang.alternates(parser.rows, parser.mapped)
    if parser.ambiguous or parser.stack or not pairs:
        return None
    return {'language': parsed['lang'].strip().lower(), 'alternates': pairs, 'spans': parser.spans}


def matches(raw, canonical, observed, inspect, identify):
    parsed, known = document(raw, canonical, inspect, identify), snapshot(observed)
    return bool(parsed and known and parsed['language'] == known[0] and parsed['alternates'] == known[2])


def rewrite(raw, pair, observed, inspect, identify):
    try:
        source, old, new = (pair[k] for k in ('page', 'from', 'to'))
        host = urlsplit(source).netloc.lower()
        if len({source, old, new}) != 3 or not all(direct(url, identify, host) for url in (source, old, new)):
            return raw, 0
    except (KeyError, TypeError, ValueError):
        return raw, 0
    if not matches(raw, source, observed, inspect, identify):
        return raw, 0
    parsed = document(raw, source, inspect, identify)
    codes = [code for code, href in parsed['alternates'] if href == old and code != 'x-default']
    if len(parsed['alternates']) != 2 or len(codes) != 1 or hreflang_lang.self_language(parsed['alternates'], source) != parsed['language']:
        return raw, 0
    start, end = parsed['spans'][codes[0]]
    output = raw[:start] + html.escape(new, quote=True) + raw[end:]
    return (output, 1) if len(output.encode('utf-8')) <= 80_000 else (raw, 0)
