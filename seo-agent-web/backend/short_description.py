"""Select a complete published excerpt for one proved literal short description."""
import html
import re

try:
    from . import anchor_text, anchor_proof, hreflang_canonical, sitemap_https
except ImportError:
    import anchor_text
    import anchor_proof
    import hreflang_canonical
    import sitemap_https

FIELDS = anchor_proof.FIELDS + ('meta_description', 'meta_description_tag_count')


def snapshot(row):
    known = anchor_proof.metadata(row)
    if not known or any(key not in row for key in FIELDS):
        return None
    value, count = row['meta_description'], row['meta_description_tag_count']
    if (type(count) is not int or count not in (0, 1) or count == 0 and value is not None
            or count == 1 and not isinstance(value, str)):
        return None
    return dict(known, meta_description=value.strip() or None if isinstance(value, str) else None,
                meta_description_tag_count=count)


def verified_urls(urls, pages, identify, *, host, samples, floor):
    if not isinstance(urls, list) or not isinstance(pages, list) or not isinstance(samples, dict):
        return [], ['page inconnue']
    accepted, refused = [], []
    for url in urls:
        if not isinstance(url, str):
            refused.append('page inconnue')
            continue
        rows = [row for row in pages if isinstance(row, dict) and row.get('url') == url]
        verified, _ = sitemap_https.verified_urls([url], rows, identify, host=host)
        values = [snapshot(row) for row in rows]
        sample = samples.get(url)
        if (not hreflang_canonical.direct(url, identify, host) or verified != [url] or not values or not values[0]
                or any(value != values[0] for value in values)
                or any(row.get('final_url') != url or row.get('canonical') != url for row in rows)
                or not isinstance(sample, dict) or not isinstance(sample.get('rendered'), str)
                or type(sample.get('len')) is not int or sample['len'] != len(sample['rendered'])
                or not 0 <= sample['len'] < floor or sample['rendered'] != (values[0]['meta_description'] or '')):
            refused.append(url if isinstance(url, str) else 'page inconnue')
        elif url not in accepted:
            accepted.append(url)
    return accepted, sorted(set(refused))


def excerpts(paragraphs, floor, ceiling):
    result = []
    for paragraph in paragraphs:
        sentences = re.split(r'(?<=[.!?])\s+', paragraph)
        for start in range(len(sentences)):
            value = ''
            for sentence in sentences[start:]:
                value = (value + ' ' + sentence).strip()
                if len(value) > ceiling:
                    break
                if (floor <= len(value) and value[-1:] in '.!?' and value in paragraph
                        and not any(ord(char) < 32 or ord(char) == 127 for char in value) and value not in result):
                    result.append(value)
                    if len(result) == 24:
                        return result
    return result


class Document(anchor_proof.Document):
    def __init__(self, raw):
        super().__init__(raw)
        self.descriptions, self.paragraphs, self.paragraph = [], [], None
        self.paragraph_ok = False
        self.head_end = None

    def start(self, tag, attrs, closed):
        parents = [parent for parent, _ in self.stack]
        values = dict(attrs)
        if self.paragraph is not None:
            self.paragraph_ok &= (tag in {'span', 'b', 'i', 'em', 'strong', 'small', 'br'}
                                  and not anchor_text.uncertain(values) and not (anchor_text.NAMING & values.keys()))
            if tag == 'br':
                self.paragraph.append(' ')
        if tag == 'p':
            self.paragraph = []
            self.paragraph_ok = ('body' in parents and all(parent in {'html', 'body', 'main', 'article', 'section', 'div'}
                and not anchor_text.uncertain(attributes) for parent, attributes in self.stack)
                and not anchor_text.uncertain(values) and not (anchor_text.NAMING & values.keys()))
        if tag == 'meta' and str(values.get('name') or '').strip().lower() == 'description':
            parsed = anchor_text.literal_tokens(self.get_starttag_text(), tag, attrs)
            span = parsed[1].get('content') if parsed else None
            self.ambiguous |= (parents != ['html', 'head'] or not span or 'property' in values
                               or anchor_text.uncertain(values))
            line, column = self.getpos()
            offset = self.offsets[line - 1] + column
            self.descriptions.append((str(values.get('content') or '').strip() or None,
                                      (offset + span[0], offset + span[1], span[2]) if span else None))
        super().start(tag, attrs, closed)

    def handle_data(self, value):
        super().handle_data(value)
        if self.paragraph is not None:
            self.paragraph.append(value)

    def handle_endtag(self, tag):
        if tag == 'head':
            line, column = self.getpos()
            self.head_end = self.offsets[line - 1] + column
        super().handle_endtag(tag)
        if tag == 'p' and self.paragraph is not None:
            if self.paragraph_ok:
                text = ' '.join(''.join(self.paragraph).split())
                if text and len(self.paragraphs) < 24:
                    self.paragraphs.append(text)
            self.paragraph = None


def document(raw, canonical, inspect, identify, *, floor=100, ceiling=160):
    known = anchor_proof.document(raw, canonical, inspect, identify)
    if not known:
        return None
    try:
        parser = Document(raw)
        parser.feed(raw)
        parser.close()
        if (parser.ambiguous or parser.stack or parser.paragraph is not None
                or parser.head_end is None or len(parser.descriptions) > 1):
            return None
        value, span = parser.descriptions[0] if parser.descriptions else (None, None)
        return {**{key: known[key] for key in anchor_proof.FIELDS}, 'meta_description': value,
                'meta_description_tag_count': len(parser.descriptions), 'span': span,
                'head_end': parser.head_end, 'paragraphs': parser.paragraphs,
                'candidates': excerpts(parser.paragraphs, floor, ceiling)}
    except (UnicodeError, ValueError, AssertionError):
        return None


def matches(raw, observed, inspect, identify, expected=None, *, floor=100, ceiling=160):
    known = snapshot(observed)
    canonical = observed.get('url') if isinstance(observed, dict) else None
    parsed = document(raw, canonical, inspect, identify, floor=floor, ceiling=ceiling)
    return bool(parsed and known and {key: parsed[key] for key in FIELDS} == known
        and len(known['meta_description'] or '') < floor and parsed['candidates']
        and (expected is None or all(parsed[key] == expected.get(key) for key in (*FIELDS, 'paragraphs', 'candidates'))))


def rewrite(raw, observed, value, inspect, identify, *, floor=100, ceiling=160):
    if not isinstance(value, str) or not matches(raw, observed, inspect, identify, floor=floor, ceiling=ceiling):
        return raw, 0
    parsed = document(raw, observed['url'], inspect, identify, floor=floor, ceiling=ceiling)
    if value not in parsed['candidates']:
        return raw, 0
    escaped = html.escape(value, quote=True)
    if parsed['span']:
        start, end, _ = parsed['span']
        output = raw[:start] + escaped + raw[end:]
    else:
        end = parsed['head_end']
        line = raw.rfind('\n', 0, end) + 1
        indent = raw[line:end]
        tag = '<meta name="description" content="' + escaped + '" />'
        if indent.strip():
            output = raw[:end] + tag + raw[end:]
        else:
            newline = '\r\n' if '\r\n' in raw else '\n'
            output = raw[:line] + indent + tag + newline + raw[line:]
    return (output, 1) if len(output.encode('utf-8')) <= anchor_text.LIMIT else (raw, 0)
