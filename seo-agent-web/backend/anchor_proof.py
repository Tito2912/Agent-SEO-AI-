"""Attest a literal source and the existing name of each internal destination."""
from urllib.parse import urljoin, urlsplit

try:
    from . import anchor_text, hreflang_canonical, sitemap_https
except ImportError:
    import anchor_text
    import hreflang_canonical
    import sitemap_https

FIELDS = ('lang', 'title', 'title_tag_count', 'h1', 'h1_tag_count')


def evidence(block):
    data = block.get('evidence') if isinstance(block, dict) else None
    items = data.get('items') if isinstance(data, dict) and data.get('kind') == 'page_values' else None
    return items if isinstance(items, list) and len(items) <= 40 else []


def metadata(row):
    if not isinstance(row, dict) or any(key not in row for key in FIELDS):
        return None
    language, title, headings = row['lang'], row['title'], row['h1']
    if (not isinstance(language, str) or not language.strip() or not isinstance(headings, list)
            or type(row['h1_tag_count']) is not int or row['h1_tag_count'] != len(headings)
            or any(not isinstance(value, str) or not value.strip() for value in headings)
            or type(row['title_tag_count']) is not int or row['title_tag_count'] not in (0, 1)
            or row['title_tag_count'] == 1 and (not isinstance(title, str) or not title.strip())
            or row['title_tag_count'] == 0 and title is not None):
        return None
    return {'lang': language.strip().lower(), 'title': ' '.join(title.split()) if title else None,
            'title_tag_count': row['title_tag_count'], 'h1': [' '.join(value.split()) for value in headings],
            'h1_tag_count': row['h1_tag_count']}


def name(row):
    values = metadata(row)
    return (values['h1'][0] if len(values['h1']) == 1 else values['title']) if values else None


def destination(item, identify, host):
    if (not isinstance(item, dict) or any(not isinstance(item.get(key), str) or not item[key].strip()
                                         for key in ('page', 'field', 'value'))
            or any(char.isspace() or ord(char) < 32 or ord(char) == 127 or char == '\\' for char in item['field'])
            or item['value'] != item['value'].strip() or any(ord(char) < 32 or ord(char) == 127 for char in item['value'])
            or not hreflang_canonical.direct(item['page'], identify, host)):
        return None
    try:
        item['value'].encode('utf-8')
        target = urljoin(item['page'], item['field'])
        return target if target != item['page'] and hreflang_canonical.direct(target, identify, host) else None
    except (UnicodeError, ValueError):
        return None


def source_links(row, item, target):
    rows = row.get('links_without_anchor_text')
    if not isinstance(rows, list):
        return None
    selected = [link for link in rows if isinstance(link, dict) and link.get('href') == item['field']]
    if not selected or any(link.get('source_url') != item['page'] or link.get('target_url') != target
        or link.get('internal') is not True or not isinstance(link.get('rel'), str)
        or any(not isinstance(link.get(key), str) or link[key] != '' for key in ('anchor_text', 'title', 'aria_label'))
        for link in selected):
        return None
    return sorted(link['rel'] for link in selected)


def witness(item, pages, identify, host):
    target = destination(item, identify, host)
    if not target or not isinstance(pages, list):
        return None
    observations = {}
    for url in (item['page'], target):
        rows = [row for row in pages if isinstance(row, dict) and row.get('url') == url]
        verified, _ = sitemap_https.verified_urls([url], rows, identify, host=host)
        snapshots = [metadata(row) for row in rows]
        if (verified != [url] or not snapshots or not snapshots[0] or any(value != snapshots[0] for value in snapshots)
                or any(row.get('final_url') != url or row.get('canonical') != url for row in rows)):
            return None
        if url == item['page']:
            links = [source_links(row, item, target) for row in rows]
            if not links[0] or any(value != links[0] for value in links):
                return None
        elif name(rows[0]) != item['value']:
            return None
        observations[url] = rows[0]
    return observations


def verified_items(items, pages, identify, *, host):
    if not isinstance(items, list):
        return [], ['page inconnue']
    groups, accepted, refused = {}, [], []
    for item in items:
        page = item.get('page') if isinstance(item, dict) else None
        groups.setdefault(page if isinstance(page, str) else '', []).append(item)
    for page, proposals in groups.items():
        unique = []
        for item in proposals:
            if item not in unique:
                unique.append(item)
        if any(not witness(item, pages, identify, host) for item in unique):
            refused.append(page or 'page inconnue')
        else:
            accepted.extend({key: item[key] for key in ('page', 'field', 'value')} for item in unique)
    return accepted, sorted(set(refused))


class Document(anchor_text.Anchors):
    def __init__(self, raw):
        super().__init__(raw)
        self.titles, self.headings, self.title, self.heading = [], [], None, None

    def start(self, tag, attrs, closed):
        parents = [parent for parent, _ in self.stack]
        values = dict(attrs)
        if tag == 'link' and 'canonical' in str(values.get('rel') or '').lower().split():
            self.ambiguous |= parents != ['html', 'head']
        if tag in {'title', 'h1'}:
            self.ambiguous |= (tag == 'title' and parents != ['html', 'head']
                               or tag == 'h1' and 'body' not in parents or anchor_text.uncertain(values)
                               or any(anchor_text.uncertain(attributes) for _, attributes in self.stack)
                               or bool(anchor_text.NAMING & values.keys()))
            if tag == 'title':
                self.title = []
            else:
                self.heading = []
        elif self.heading is not None:
            self.ambiguous |= (tag not in {'span', 'b', 'em', 'strong', 'small', 'br'}
                               or anchor_text.uncertain(values) or bool(anchor_text.NAMING & values.keys()))
            if tag == 'br':
                self.heading.append(' ')
        super().start(tag, attrs, closed)
        if tag == 'a' and self.active:
            self.active['rel'] = str(values.get('rel') or '').strip()
            self.active['eligible'] &= 'body' in parents and 'head' not in parents

    def handle_data(self, value):
        super().handle_data(value)
        if self.title is not None:
            self.title.append(value)
        if self.heading is not None:
            self.heading.append(value)

    def handle_endtag(self, tag):
        super().handle_endtag(tag)
        if tag == 'title' and self.title is not None:
            self.titles.append(' '.join(''.join(self.title).split()))
            self.title = None
        if tag == 'h1' and self.heading is not None:
            self.headings.append(' '.join(''.join(self.heading).split()))
            self.heading = None


def document(raw, canonical, inspect, identify):
    try:
        if not isinstance(raw, str) or len(raw.encode('utf-8')) > anchor_text.LIMIT:
            return None
        if not sitemap_https.literal_destination(raw, canonical, inspect, identify):
            return None
        parsed = inspect(raw, allow_noindex=True)
        if parsed['canonicals'] != [canonical]:
            return None
        parser = Document(raw)
        parser.feed(raw)
        parser.close()
        values = metadata({'lang': parsed['lang'], 'title': parser.titles[0] if parser.titles else None,
                           'title_tag_count': len(parser.titles), 'h1': parser.headings, 'h1_tag_count': len(parser.headings)})
        if parser.ambiguous or parser.stack or parser.active or not values:
            return None
        return dict(values, links=parser.rows)
    except (UnicodeError, ValueError, AssertionError):
        return None


def matches(raw, canonical, observed, inspect, identify, items):
    if not isinstance(items, list):
        return False
    parsed = document(raw, canonical, inspect, identify)
    known = metadata(observed)
    if not parsed or not known or {key: parsed[key] for key in FIELDS} != known:
        return False
    host = urlsplit(canonical).netloc.lower()
    for item in items:
        target = destination(item, identify, host)
        if not target or item['page'] != canonical:
            return False
        selected = [link for link in parsed['links'] if link['href'] == item['field']]
        if (not selected or any(not link['eligible'] or link['blocked'] or link['named'] or not link['span'] for link in selected)
                or source_links(observed, item, target) != sorted(link['rel'] for link in selected)):
            return False
        for link in parsed['links']:
            if link['eligible'] and link['named'] and isinstance(link['href'], str) and urljoin(canonical, link['href']) == target:
                return False
    return True


def rewrite(raw, items, observed, inspect, identify):
    canonical = observed.get('url') if isinstance(observed, dict) else None
    if not isinstance(items, list) or not items or not matches(raw, canonical, observed, inspect, identify, items):
        return raw, 0
    return anchor_text.rewrite(raw, items)
