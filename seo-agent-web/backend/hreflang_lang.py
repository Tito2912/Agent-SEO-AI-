"""Add only a missing root language declared by one literal self hreflang."""

import re
from html.parser import HTMLParser

try:
    from . import sitemap_https
except ImportError:
    import sitemap_https

CODE = re.compile(r'(?:x-default|[a-z]{2}(?:-[a-z0-9]{2,8})*)', re.I)
VOID = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'}


def alternates(raw, mapped):
    if not isinstance(raw, list) or not raw or not isinstance(mapped, dict):
        return None
    pairs, seen = [], set()
    for item in raw:
        if not isinstance(item, dict) or any(not isinstance(item.get(key), str) for key in ('hreflang', 'href')):
            return None
        code, href = item['hreflang'].strip().lower(), item['href'].strip()
        if not CODE.fullmatch(code) or not href or code in seen:
            return None
        seen.add(code)
        pairs.append((code, href))
    values = {}
    for code, href in mapped.items():
        if not isinstance(code, str) or not isinstance(href, str) or code.strip().lower() in values:
            return None
        values[code.strip().lower()] = href.strip()
    return tuple(pairs) if values == dict(pairs) else None


def snapshot(row):
    if not isinstance(row, dict) or any(key not in row for key in ('lang', 'served_lang', 'hreflang', 'hreflang_raw')):
        return None
    if any(value is not None and (not isinstance(value, str) or value.strip()) for value in (row['lang'], row['served_lang'])):
        return None
    return alternates(row['hreflang_raw'], row['hreflang'])


def self_language(pairs, url):
    codes = [code for code, href in pairs or [] if href == url and code != 'x-default']
    return codes[0] if len(codes) == 1 else None


def verified_urls(urls, pages, identify, *, host):
    verified, refused = sitemap_https.verified_urls(urls, pages, identify, host=host)
    accepted = []
    for url in verified:
        rows = [row for row in pages or [] if isinstance(row, dict) and identify(row.get('url')) == url]
        snapshots = [snapshot(row) for row in rows]
        if (all(identify(row.get('canonical')) == url for row in rows)
                and snapshots and all(value == snapshots[0] for value in snapshots) and self_language(snapshots[0], url)):
            accepted.append(url)
        else:
            refused.append(url)
    return accepted, sorted(set(refused))


def literal(raw, canonical, inspect, identify):
    def bounded(value, **kw):
        return inspect(value, allow_missing_lang=True, **kw)
    if not canonical or len(raw.encode('utf-8')) > 80_000 or not sitemap_https.literal_destination(raw, canonical, bounded, identify):
        return None
    document, lines = bounded(raw, allow_noindex=True), raw.splitlines(keepends=True)

    class Language(HTMLParser):
        def __init__(self):
            super().__init__()
            self.stack, self.raw, self.mapped, self.root_end, self.ambiguous = [], [], {}, None, False

        def handle_starttag(self, tag, attrs):
            values = dict(attrs)
            if tag == 'html':
                text = self.get_starttag_text()
                line, column = self.getpos()
                start = sum(map(len, lines[:line - 1])) + column
                self.root_end = start + len(text) - 1
                self.ambiguous |= ('lang' in values or 'xml:lang' in values or text.rstrip().endswith('/>')
                                   or raw[start:self.root_end + 1] != text)
            if tag == 'link' and 'hreflang' in values:
                self.ambiguous |= (self.stack != ['html', 'head'] or len(values) != len(attrs)
                                   or 'alternate' not in str(values.get('rel') or '').lower().split())
                code, href = values.get('hreflang'), values.get('href')
                self.raw.append({'hreflang': code, 'href': href})
                if isinstance(code, str):
                    self.mapped[code.strip().lower()] = href
            if tag not in VOID:
                self.stack.append(tag)

        def handle_endtag(self, tag):
            if tag in VOID:
                return
            if not self.stack or self.stack[-1] != tag:
                self.ambiguous = True
            else:
                self.stack.pop()

    parser = Language()
    parser.feed(raw)
    parser.close()
    pairs = alternates(parser.raw, parser.mapped)
    language = self_language(pairs, canonical)
    if parser.ambiguous or parser.stack or parser.root_end is None or not language:
        return None
    return dict(document, language=language, alternates=pairs, root_end=parser.root_end)


def matches(raw, canonical, observed, inspect, identify):
    document = literal(raw, canonical, inspect, identify)
    return bool(document and document['alternates'] == snapshot(observed))


def add(raw, canonical, inspect, identify):
    document = literal(raw, canonical, inspect, identify)
    if not document:
        return raw, 0
    end = document['root_end']
    output = raw[:end] + ' lang="' + document['language'] + '"' + raw[end:]
    return (output, 1) if len(output.encode('utf-8')) <= 80_000 else (raw, 0)
