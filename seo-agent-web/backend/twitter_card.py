"""Add a missing Twitter Card from coherent, literal metadata on that same page."""

import html
import ipaddress
import re
from html.parser import HTMLParser
from urllib.parse import urlsplit

try:
    from . import sitemap_https
except ImportError:
    import sitemap_https

FIELDS = ('twitter_card', 'twitter_title', 'twitter_description', 'twitter_image',
          'og_title', 'og_description', 'og_image', 'title', 'meta_description')
META = {key.replace('_', ':', 1): key for key in FIELDS if key.startswith(('twitter_', 'og_'))}
META.update({'description': 'meta_description', 'twitter:image:src': 'twitter_image'})
VOID = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'}


def snapshot(row):
    if not isinstance(row, dict) or any(key not in row for key in FIELDS[:4]):
        return None
    values = {}
    for key in FIELDS:
        value = row.get(key)
        if value is not None and not isinstance(value, str):
            return None
        if value and key == 'title':
            value = re.sub(r'\s+', ' ', value)
        values[key] = value.strip() or None if value else None
    return values


def image_url(value):
    if not isinstance(value, str) or re.search(r'[\s\x00-\x1f\x7f\\]', value):
        return False
    try:
        parsed = urlsplit(value)
        host = (parsed.hostname or '').lower()
        if (parsed.scheme != 'https' or not host or parsed.username or parsed.password or parsed.port is not None
                or parsed.fragment or host == 'localhost' or host.endswith(('.localhost', '.local'))):
            return False
        try:
            return ipaddress.ip_address(host).is_global
        except ValueError:
            return bool(re.fullmatch(r'(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z](?:[a-z0-9-]*[a-z0-9])?', host))
    except ValueError:
        return False


def addition(values):
    if not values or values['twitter_card']:
        return None
    # The auditor exempts OG-only pages: do not manufacture a missing-card anomaly.
    if not any(values[key] for key in FIELDS[1:4]) and any(values[key] for key in FIELDS[4:7]):
        return None
    chosen = {
        'twitter:title': values['twitter_title'] or values['og_title'] or values['title'],
        'twitter:description': values['twitter_description'] or values['og_description'] or values['meta_description'],
        'twitter:image': values['twitter_image'] or values['og_image'],
    }
    if not all(chosen.values()) or not image_url(chosen['twitter:image']):
        return None
    for key in ('twitter:title', 'twitter:description'):
        text = chosen[key]
        if any(ord(char) < 32 and not char.isspace() for char in text):
            return None
        if text.lower().startswith(('http://', 'https://')) and not image_url(text):
            return None
    return {'twitter:card': 'summary_large_image', **{key: value for key, value in chosen.items()
                                                   if not values[META[key]]}}


def verified_urls(urls, pages, identify, *, host):
    verified, refused = sitemap_https.verified_urls(urls, pages, identify, host=host)
    accepted = []
    for url in verified:
        rows = [snapshot(row) for row in pages or [] if isinstance(row, dict) and identify(row.get('url')) == url]
        if rows and all(row == rows[0] for row in rows) and addition(rows[0]):
            accepted.append(url)
        else:
            refused.append(url)
    return accepted, sorted(set(refused))


def literal(raw, canonical, inspect, identify):
    if len(raw.encode('utf-8')) > 80_000 or not sitemap_https.literal_destination(raw, canonical, inspect, identify):
        return None
    document = inspect(raw, allow_noindex=True)

    class Metadata(HTMLParser):
        def __init__(self):
            super().__init__()
            self.values, self.counts, self.stack, self.title, self.ambiguous = dict.fromkeys(FIELDS), {}, [], [], False

        def handle_starttag(self, tag, attrs):
            attributes = dict(attrs)
            keys = [str(attributes.get(key) or '').strip().lower() for key in ('name', 'property')]
            key = next((key for key in keys if key in META), '')
            if tag == 'title' or tag == 'meta' and key:
                field = 'title' if tag == 'title' else META[key]
                if (self.stack != ['html', 'head'] or len(attributes) != len(attrs)
                        or tag == 'meta' and (len([key for key in keys if key]) != 1
                                              or key.startswith('og:') and keys[1] != key)):
                    self.ambiguous = True
                self.counts[field] = self.counts.get(field, 0) + 1
                if tag == 'meta':
                    self.values[field] = str(attributes.get('content') or '').strip() or None
            if tag not in VOID:
                self.stack.append(tag)

        def handle_data(self, value):
            if self.stack == ['html', 'head', 'title']:
                self.title.append(value)

        def handle_endtag(self, tag):
            if tag in VOID:
                return
            if not self.stack or self.stack[-1] != tag:
                self.ambiguous = True
            else:
                self.stack.pop()

    parser = Metadata()
    parser.feed(raw)
    parser.close()
    parser.values['title'] = re.sub(r'\s+', ' ', ''.join(parser.title)).strip() or None
    if (parser.ambiguous or parser.stack or any(count > 1 for count in parser.counts.values())
            or any(not parser.values[field] for field in parser.counts)):
        return None
    return dict(document, values=parser.values, counts=parser.counts)


def matches(raw, canonical, observed, inspect, identify):
    document = literal(raw, canonical, inspect, identify)
    return bool(document and document['values'] == snapshot(observed) and addition(document['values']))


def add(raw, canonical, inspect, identify):
    document = literal(raw, canonical, inspect, identify)
    values = addition(document['values']) if document else None
    if not values:
        return raw, 0
    tags = ['<meta name="' + key + '" content="' + html.escape(value, quote=True) + '" />' for key, value in values.items()]
    end = document['head_end']
    line = raw.rfind('\n', 0, end) + 1
    indent = raw[line:end]
    if indent.strip():
        output = raw[:end] + ''.join(tags) + raw[end:]
    else:
        newline = '\r\n' if '\r\n' in raw else '\n'
        output = raw[:line] + ''.join(indent + tag + newline for tag in tags) + raw[line:]
    return (output, len(tags)) if len(output.encode('utf-8')) <= 80_000 else (raw, 0)
