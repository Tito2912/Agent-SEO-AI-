"""Serve a generator's build output the way a static host would.

The per-stack fixtures prove something the fixture TREES cannot: that a patch the corrector
writes still compiles, and that rebuilding actually removes the anomaly. Getting there needs the
built site served the way Netlify/Vercel serve it, because the defect being reproduced — a
canonical pointing at a URL the host redirects away from — only exists once pretty URLs and
trailing-slash canonicalisation are in play.

Deliberately generic: `dist/` for Astro, `public/` for Hugo, `_site/` for Jekyll and Eleventy,
`.output/public` for Nuxt. One server, one behaviour, every stack.

Behaviour, matching a static host with `trailingSlash: never`:
  * `/x`  -> `x.html`, else `x/index.html`
  * `/x/` -> 301 to `/x` (this is what makes a trailing-slash canonical a real defect)
  * `/`   -> `index.html`
  * unknown path -> 404
"""

from __future__ import annotations

import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".xml": "application/xml",
    ".txt": "text/plain; charset=utf-8",
    ".css": "text/css",
    ".js": "application/javascript",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".svg": "image/svg+xml",
    ".webp": "image/webp",
}


def _resolve(root: Path, path: str) -> Path | None:
    """Map a URL path to a file inside `root`, refusing anything that escapes it."""
    rel = path.lstrip("/")
    candidates = (
        [root / "index.html"]
        if not rel
        else [root / rel, root / f"{rel}.html", root / rel / "index.html"]
    )
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
            resolved.relative_to(root.resolve())
        except (ValueError, OSError):
            continue  # `..` traversal, or an unreadable path
        if resolved.is_file():
            return resolved
    return None


def make_handler(root: Path, base: str) -> type[BaseHTTPRequestHandler]:
    root = Path(root)

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
            path = self.path.split("?", 1)[0]
            # The trailing-slash 301 is the whole point: without it a canonical ending in `/`
            # is merely unusual, not a defect the crawler can see.
            if len(path) > 1 and path.endswith("/"):
                return self._send(301, "text/html", b"", location=base + path.rstrip("/"))
            found = _resolve(root, path)
            if found is None:
                return self._send(404, "text/html; charset=utf-8", b"<html><body>404</body></html>")
            ctype = _TYPES.get(found.suffix.lower(), "application/octet-stream")
            return self._send(200, ctype, found.read_bytes())

        do_HEAD = do_GET

    return Handler


def make_server(root: Path, port: int = 0) -> ThreadingHTTPServer:
    probe = ThreadingHTTPServer(("127.0.0.1", port), BaseHTTPRequestHandler)
    bound = probe.server_address[1]
    probe.server_close()
    base = f"http://127.0.0.1:{bound}"
    server = ThreadingHTTPServer(("127.0.0.1", bound), make_handler(Path(root), base))
    server.base_url = base  # type: ignore[attr-defined]
    return server


class serving:
    """Context manager: `with serving(dist) as base: ...`."""

    def __init__(self, root: Path, port: int = 0) -> None:
        self.server = make_server(root, port)
        self.base_url: str = self.server.base_url  # type: ignore[attr-defined]

    def __enter__(self) -> str:
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()
        return self.base_url

    def __exit__(self, *exc) -> None:
        self.server.shutdown()
        self.server.server_close()
        self._thread.join(timeout=5)


if __name__ == "__main__":
    directory = Path(sys.argv[1] if len(sys.argv) > 1 else "dist")
    srv = make_server(directory, int(sys.argv[2]) if len(sys.argv) > 2 else 8741)
    print(f"serving {directory} on {srv.base_url}")  # type: ignore[attr-defined]
    srv.serve_forever()
