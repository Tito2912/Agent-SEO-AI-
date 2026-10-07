"""Insert an accessible name only into a provably empty literal anchor.

This rewriter consumes evidence; it does not attest its freshness or the destination.
ARIA labels describe link purpose (W3C ARIA8), not a guaranteed Google anchor signal.
"""
import html
from html.parser import HTMLParser
import re

LIMIT = 80_000
VOID = set('area base br col embed hr img input link meta param source track wbr'.split())
EMPTY = set('span i b em strong s small br img svg g path circle ellipse rect line polyline polygon defs clippath mask'.split())
CONTAINERS = set(('html head body header footer nav main section article aside div p ul ol li dl dt dd '
                  'h1 h2 h3 h4 h5 h6 figure figcaption table thead tbody tfoot tr td th form fieldset '
                  'legend details summary menu blockquote address a').split()) | EMPTY
BLOCKED = set('script style template noscript textarea title xmp iframe noembed plaintext listing'.split())
NAMING = {'aria-label', 'aria-labelledby', 'title', 'alt'}
TOKEN = re.compile(r'''\s+([^\s=<>/"']+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'))?''')
NAME = re.compile(r'[a-z_:][a-z0-9_.:-]*', re.I)


def literal_attributes(text, tag, attrs):
    prefix = re.match(r'<' + re.escape(tag) + r'\b', text, re.I)
    close = re.search(r'\s*/?>$', text)
    if not prefix or not close or len(dict(attrs)) != len(attrs):
        return None
    tokens, href, position = [], None, prefix.end()
    while position < close.start():
        match = TOKEN.match(text, position, close.start())
        if not match or not NAME.fullmatch(match[1]):
            return None
        key = match[1].lower()
        group = next((i for i in (2, 3) if match[i] is not None), None)
        value = html.unescape(match[group]) if group else None
        if value and any(marker in value for marker in ('{', '}', '<%', '%>')):
            return None
        tokens.append((key, value))
        if key == 'href' and group:
            href = (match.end(group) + 1, text[match.start(group) - 1])
        position = match.end()
    return (dict(tokens), href) if tokens == attrs else None


def uncertain(attrs):
    return ('hidden' in attrs or 'inert' in attrs or 'style' in attrs
            or str(attrs.get('aria-hidden') or '').strip().lower() == 'true'
            or any(key.startswith(('on', 'v-', 'x-')) or ':' in key or key == 'data-bind' for key in attrs)
            or 'role' in attrs and attrs['role'] != 'link')


class Anchors(HTMLParser):
    def __init__(self, raw):
        super().__init__()
        self.raw, self.stack, self.rows, self.active = raw, [], [], None
        self.ambiguous, self.offsets = False, [0]
        for line in raw.split('\n')[:-1]:
            self.offsets.append(self.offsets[-1] + len(line) + 1)

    def start(self, tag, attrs, closed):
        text, (line, column) = self.get_starttag_text(), self.getpos()
        offset = self.offsets[line - 1] + column
        parsed = literal_attributes(text, tag, attrs)
        if not parsed or self.raw[offset:offset + len(text)] != text:
            self.ambiguous = True
        values, href = parsed if parsed else (dict(attrs), None)
        if closed and tag not in VOID | EMPTY:
            self.ambiguous = True
        if self.active:
            self.active['blocked'] |= (tag not in EMPTY or uncertain(values)
                                       or bool((NAMING - {'alt'}) & values.keys()) or bool(values.get('alt')))
            self.active['named'] |= any(str(values.get(key) or '').strip() for key in NAMING)
            self.active['named'] |= 'aria-labelledby' in values
        if tag == 'a':
            if self.active or closed:
                self.ambiguous = True
            eligible = not uncertain(values) and all(parent in CONTAINERS and not uncertain(attributes)
                                                     for parent, attributes in self.stack)
            self.active = {'href': values.get('href'), 'span': (offset + href[0], href[1]) if href else None,
                           'eligible': eligible, 'blocked': bool(NAMING & values.keys()),
                           'named': any(str(values.get(key) or '').strip() for key in NAMING)
                                    or 'aria-labelledby' in values}
        if not closed and tag not in VOID:
            self.stack.append((tag, values))

    def handle_starttag(self, tag, attrs):
        self.start(tag, attrs, False)

    def handle_startendtag(self, tag, attrs):
        self.start(tag, attrs, True)

    def handle_endtag(self, tag):
        if tag in VOID or not self.stack or self.stack[-1][0] != tag:
            self.ambiguous = True
            return
        self.stack.pop()
        if tag == 'a' and self.active:
            self.rows.append(self.active)
            self.active = None

    def handle_data(self, value):
        if '<' in value and not any(tag in BLOCKED for tag, _ in self.stack):
            self.ambiguous = True
        if self.active and value.strip():
            self.active['blocked'] = self.active['named'] = True

    def handle_comment(self, value):
        line, column = self.getpos()
        offset = self.offsets[line - 1] + column
        text = '<!--' + value + '-->'
        if self.raw[offset:offset + len(text)] != text:
            self.ambiguous = True

    def unknown_decl(self, data):
        self.ambiguous = True


def rewrite(raw, items):
    if not isinstance(raw, str) or not isinstance(items, list) or not items:
        return raw, 0
    try:
        if len(raw.encode('utf-8')) > LIMIT:
            return raw, 0
        names = {}
        for item in items:
            if not isinstance(item, dict) or any(not isinstance(item.get(key), str) or not item[key].strip()
                                                for key in ('page', 'field', 'value')):
                return raw, 0
            href, name = item['field'], item['value'].strip()
            if any(ord(char) < 32 or ord(char) == 127 for char in href + name):
                return raw, 0
            if href in names and names[href] != name:
                return raw, 0
            names[href] = name
        parser = Anchors(raw)
        parser.feed(raw)
        parser.close()
        if parser.ambiguous or parser.stack or parser.active:
            return raw, 0
        named = {row['href'] for row in parser.rows if row['eligible'] and row['named']}
        insertions = []
        for row in parser.rows:
            if (row['eligible'] and not row['blocked'] and row['span'] and row['href'] in names
                    and row['href'] not in named):
                offset, quote = row['span']
                insertions.append((offset, ' aria-label=' + quote + html.escape(names[row['href']], quote=True) + quote))
        result = raw
        for offset, value in reversed(insertions):
            result = result[:offset] + value + result[offset:]
        return (result, len(insertions)) if len(result.encode('utf-8')) <= LIMIT else (raw, 0)
    except (UnicodeError, ValueError, AssertionError):
        return raw, 0
