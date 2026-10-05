"""Prove a missing viewport and append exactly one literal HTML head tag."""

from html.parser import HTMLParser

try:
    from . import sitemap_https
except ImportError:
    import sitemap_https

TAG = '<meta name="viewport" content="width=device-width, initial-scale=1" />'


def verified_urls(urls, pages, identify, *, host):
    verified, refused = sitemap_https.verified_urls(urls, pages, identify, host=host)
    accepted = []
    for url in verified:
        rows = [row for row in pages or [] if isinstance(row, dict) and identify(row.get('url')) == url]
        if all(type(row.get('meta_viewport_tag_count')) is int and row['meta_viewport_tag_count'] == 0
               and (row.get('meta_viewport') is None or row.get('meta_viewport') == '') for row in rows):
            accepted.append(url)
        else:
            refused.append(url)
    return accepted, sorted(set(refused))


def literal(raw, canonical, inspect, identify):
    if len(raw.encode('utf-8')) > 80_000:
        return None
    if not sitemap_https.literal_destination(raw, canonical, inspect, identify):
        return None
    document = inspect(raw, allow_noindex=True)

    class Viewports(HTMLParser):
        count = 0

        def handle_starttag(self, tag, attrs):
            if tag == 'meta' and str(dict(attrs).get('name') or '').strip().lower() == 'viewport':
                self.count += 1

    parser = Viewports()
    parser.feed(raw)
    parser.close()
    return dict(document, viewport_count=parser.count)


def add(raw, canonical, inspect, identify):
    document = literal(raw, canonical, inspect, identify)
    if not document or document['viewport_count']:
        return raw, 0
    end = document['head_end']
    line = raw.rfind('\n', 0, end) + 1
    indent = raw[line:end]
    if indent.strip():
        output = raw[:end] + TAG + raw[end:]
    else:
        newline = '\r\n' if '\r\n' in raw else '\n'
        output = raw[:line] + indent + TAG + newline + raw[line:]
    return (output, 1) if len(output.encode('utf-8')) <= 80_000 else (raw, 0)
