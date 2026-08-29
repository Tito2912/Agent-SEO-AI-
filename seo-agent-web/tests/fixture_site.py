"""A tiny site carrying exactly one defect per evidence family.

Each route is deliberately broken in ONE way, so a crawl's evidence can be checked field by
field instead of hoping a real site happens to have the right problems.

**This file earned its place.** Unit-testing the evidence WRITER could not catch the worst bug
this project shipped: `_score_issues` read `PageData.h1` as a string when it is a list of every
h1, so any site with a 2+ h1 page raised AttributeError inside the scorer and **the entire crawl
died** — in production, for a day. A real crawl against this site reproduced it immediately,
because `/` carries two h1 on purpose.

It lived in a scratch directory until 2026-08-29 and was one disk cleanup from being lost, the
same reason `ops/mail_doctor.py` had to ship.

Run a REAL crawl against it (needs a browser: `python -m playwright install chromium`):

    python seo-agent-web/tests/fixture_site.py 8731 &
    SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1 \\
    python skills/public/seo-autopilot/scripts/seo_audit.py http://127.0.0.1:8731/ \\
        --sitemap http://127.0.0.1:8731/sitemap.xml --check-resources --output-dir /tmp/fx

**`SEO_AUDIT_ALLOW_PRIVATE_HOSTS=1` is not optional, and the failure is silent.** The crawler's
SSRF guard refuses loopback and any port outside 80/443, but it only guards the `requests`-based
system fetches — robots.txt and the sitemap. Playwright still fetches pages, so WITHOUT the flag
the crawl appears to work: it prints `[SITEMAP] sitemaps=1 seed_urls=0`, walks whatever it can
reach by link, and quietly misses every page that is only listed in the sitemap. That is how
`hreflang_to_non_canonical` reported 0 here while the defect was sitting on `/es-source`.
Note the prefix: the crawler reads `SEO_AUDIT_ALLOW_PRIVATE_HOSTS` while the web app reads
`SEO_AGENT_ALLOW_PRIVATE_HOSTS` (`_validate_public_crawl_target`). Setting the wrong one changes
nothing and says nothing.

Expected result, verified 2026-08-29 — 11 pages and each of these exactly once:
canonical_points_to_redirect, multiple_h1, html_lang_attribute_invalid,
open_graph_tags_incomplete, hreflang_to_redirect_or_broken_page, hreflang_to_non_canonical,
page_has_redirected_image, image_redirects.
Three `pages[].error` entries are EXPECTED and not a fault: the crawler probes
`http://127.0.0.1/`, `http://www.127.0.0.1/` and `https://www.127.0.0.1/` for http→https and
www canonicalisation, which cannot resolve against a bare loopback IP.

**Always check `pages[].error` before trusting a 0-issue crawl**: with no browser installed every
page errors and the report looks exactly like a clean site.

CI cannot run that — the crawler has no browserless path and the workflow installs no Chromium.
What CI does run is `test_fixture_site.py`, which serves this site and asserts each route still
carries the defect it is supposed to carry. A fixture nobody exercises rots silently, and a
rotten fixture reports a clean site.
"""

from __future__ import annotations

import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_PORT = 8731

# The defect each route carries, so a reader can tell at a glance what this site is for.
DEFECTS: dict[str, str] = {
    "/": "canonical_points_to_redirect, multiple_h1, html_lang_attribute_invalid, "
         "open_graph_tags_incomplete, page_has_redirected_image",
    "/fr": "hreflang_to_redirect_or_broken_page",
    "/es-source": "hreflang_to_non_canonical",
    "/img/logo.png": "image_redirects",
}


def build_pages(base: str) -> dict[str, tuple[int, str, bytes]]:
    """Every 200 route, built against `base` so the site can run on any port."""
    # canonical -> a URL that 301s to /home  => canonical_points_to_redirect
    # lang="en_US" is not a language code    => html_lang_attribute_invalid
    # two <h1>                               => multiple_h1 (and the crawl-killing scorer bug)
    # og:title present, og:description not   => open_graph_tags_incomplete
    # <img> pointing at a redirecting asset  => page_has_redirected_image / image_redirects
    home = f"""<!doctype html>
<html lang="en_US">
<head>
<title>Accueil</title>
<meta name="description" content="Page d'accueil du site de test avec une description assez longue pour ne pas etre trop courte.">
<link rel="canonical" href="{base}/home-old">
<meta property="og:title" content="Accueil">
<link rel="alternate" hreflang="en" href="{base}/home">
<link rel="alternate" hreflang="fr" href="{base}/fr">
<link rel="alternate" hreflang="x-default" href="{base}/home">
</head>
<body>
<h1>Premier titre</h1>
<h1>Second titre en trop</h1>
<img src="{base}/img/logo.png" alt="Logo">
<a href="{base}/home">home</a> <a href="{base}/fr">fr</a> <a href="{base}/es-dup">es</a>
</body></html>"""

    # hreflang="en" -> /fr-old, which 301s to /fr  => hreflang_to_redirect_or_broken_page
    fr = f"""<!doctype html>
<html lang="fr">
<head>
<title>Page francaise</title>
<meta name="description" content="Version francaise de la page de test, avec une description de longueur raisonnable pour le crawl.">
<link rel="canonical" href="{base}/fr">
<link rel="alternate" hreflang="fr" href="{base}/fr">
<link rel="alternate" hreflang="en" href="{base}/fr-old">
<link rel="alternate" hreflang="x-default" href="{base}/fr">
</head>
<body><h1>Bonjour</h1><a href="{base}/home">home</a></body></html>"""

    # hreflang="es" -> /es-dup, whose own canonical is /es  => hreflang_to_non_canonical
    es_source = f"""<!doctype html>
<html lang="es">
<head>
<title>Pagina espanola</title>
<meta name="description" content="Version espanola de la pagina de prueba, con una descripcion de longitud razonable para el rastreo.">
<link rel="canonical" href="{base}/es-source">
<link rel="alternate" hreflang="es" href="{base}/es-dup">
<link rel="alternate" hreflang="en" href="{base}/home">
<link rel="alternate" hreflang="x-default" href="{base}/home">
</head>
<body><h1>Hola</h1><a href="{base}/home">home</a></body></html>"""

    es_dup = f"""<!doctype html>
<html lang="es">
<head>
<title>Pagina espanola duplicada</title>
<meta name="description" content="Duplicado que declara su canonical hacia la pagina principal espanola del sitio de prueba.">
<link rel="canonical" href="{base}/es">
</head>
<body><h1>Hola dup</h1></body></html>"""

    es = f"""<!doctype html>
<html lang="es">
<head><title>Pagina espanola canonica</title>
<meta name="description" content="Pagina espanola canonica del sitio de prueba, con descripcion suficientemente larga.">
<link rel="canonical" href="{base}/es"></head>
<body><h1>Hola canonica</h1></body></html>"""

    plain = f"""<!doctype html>
<html lang="en"><head><title>Home cible</title>
<meta name="description" content="Destination finale de la redirection, avec une description de longueur correcte pour l audit.">
<link rel="canonical" href="{base}/home"></head>
<body><h1>Home</h1></body></html>"""

    html = "text/html; charset=utf-8"
    return {
        "/": (200, html, home.encode()),
        "/home": (200, html, plain.encode()),
        "/fr": (200, html, fr.encode()),
        "/es-source": (200, html, es_source.encode()),
        "/es-dup": (200, html, es_dup.encode()),
        "/es": (200, html, es.encode()),
        "/img/logo-v2.png": (200, "image/png", PNG),
    }


# The smallest valid 1x1 PNG: the asset family needs a real image, not a placeholder.
PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000a49444154789c6360000002000100ffff0300000600"
    "0557bfabd40000000049454e44ae426082"
)

REDIRECTS: dict[str, str] = {
    "/home-old": "/home",                  # the canonical target redirects
    "/fr-old": "/fr",                      # the hreflang target redirects
    "/img/logo.png": "/img/logo-v2.png",   # the asset redirects
}

SITEMAP_PATHS = ("/", "/home", "/fr", "/es-source", "/es-dup", "/es")


def build_sitemap(base: str) -> str:
    entries = "".join(f"<url><loc>{base}{p}</loc></url>" for p in SITEMAP_PATHS)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{entries}\n</urlset>"
    )


def make_handler(base: str) -> type[BaseHTTPRequestHandler]:
    pages = build_pages(base)
    sitemap = build_sitemap(base).encode()
    robots = f"User-agent: *\nAllow: /\nSitemap: {base}/sitemap.xml\n".encode()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # keep the crawl output readable
            pass

        def _send(self, status: int, ctype: str, body: bytes, location: str | None = None) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            if location:
                self.send_header("Location", location)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?", 1)[0].rstrip("/") or "/"
            if path == "/robots.txt":
                return self._send(200, "text/plain", robots)
            if path == "/sitemap.xml":
                return self._send(200, "application/xml", sitemap)
            if path in REDIRECTS:
                return self._send(301, "text/html", b"", location=base + REDIRECTS[path])
            if path in pages:
                status, ctype, body = pages[path]
                return self._send(status, ctype, body)
            return self._send(404, "text/html", b"<html><body>404</body></html>")

        do_HEAD = do_GET

    return Handler


def make_server(port: int = 0) -> ThreadingHTTPServer:
    """Bind a server. Port 0 asks the OS for a free one — a test that hardcodes a port fails
    whenever anything else on the machine happens to hold it."""
    probe = ThreadingHTTPServer(("127.0.0.1", port), BaseHTTPRequestHandler)
    bound_port = probe.server_address[1]
    probe.server_close()
    base = f"http://127.0.0.1:{bound_port}"
    server = ThreadingHTTPServer(("127.0.0.1", bound_port), make_handler(base))
    server.base_url = base  # type: ignore[attr-defined]
    return server


if __name__ == "__main__":
    srv = make_server(int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT)
    print(f"fixture site on {srv.base_url}")  # type: ignore[attr-defined]
    srv.serve_forever()
