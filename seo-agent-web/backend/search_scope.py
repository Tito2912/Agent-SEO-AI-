"""A property must describe this exact URL scope, never a convenient sibling site."""
from urllib.parse import urlsplit


def same_property(base_url, site_url):
    try:
        values = []
        for value in (base_url, site_url):
            raw = str(value or '').strip()
            if any(ord(c)<33 for c in raw):return False
            url = urlsplit(raw)
            if (url.scheme not in {'http','https'} or not url.hostname or url.username or url.password
                    or url.query or url.fragment or url.port not in (None,80,443)):
                return False
            values.append((url.scheme,url.hostname.lower(),url.port or (443 if url.scheme == 'https' else 80),url.path.rstrip('/') or '/'))
        return values[0] == values[1]
    except ValueError:
        return False


def safe_report(report):
    if not isinstance(report,dict):
        return report
    meta = report.get('meta') if isinstance(report.get('meta'),dict) else {}
    bing = meta.get('bing') or {}
    if isinstance(bing,dict) and bing.get('ok') and not same_property(meta.get('base_url'),bing.get('site_url')):
        # Keep the saved artifact intact; quarantine only its unproven Bing component.
        return {**report,'meta':{**meta,'bing':{'enabled':bool(bing.get('enabled')),'ok':False,'reason':'site_mismatch'}},
            'issues':{key:value for key,value in (report.get('issues') if isinstance(report.get('issues'),dict) else {}).items() if not key.startswith('bing_')}}
    return report
