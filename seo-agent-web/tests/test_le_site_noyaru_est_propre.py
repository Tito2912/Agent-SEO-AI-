# -*- coding: utf-8 -*-
"""Chaque page du sitemap de noyaru.com passe les règles que le produit vend.

12/10/2026 : premier crawl du site par Noyaru et par Ahrefs — 0 erreur mais 42 avertissements,
le même compte des deux côtés : aucune image de partage (33 « Open Graph incomplet »), 7 pages
sans carte X, /status et /support sans description, /mentions-legales trop courte, deux
descriptions et un titre trop longs. Corrigé dans nos gabarits ; ce test le tient. Les bornes
sont celles du crawler (seo_audit.py) : description 100-160, titre 15-70.
"""
from __future__ import annotations

import html
import os
import re
import struct
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-site-propre-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("APP_NAME", "Noyaru")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402

IMAGE = WEB_ROOT / "static" / "brand" / "noyaru-partage.png"
META_RE = re.compile(r"""<meta\b[^>]*\b(property|name)\s*=\s*"([^"]+)"[^>]*\bcontent\s*=\s*"([^"]*)"[^>]*>""", re.I)


def _pages() -> list[tuple[str, str]]:
    client = TestClient(m.app)
    sitemap = client.get("/sitemap.xml")
    assert sitemap.status_code == 200
    paths = [re.sub(r"^https?://[^/]+", "", u) or "/" for u in re.findall(r"<loc>([^<]+)</loc>", sitemap.text)]
    assert len(paths) >= 30, paths
    out = []
    for path in paths:
        r = client.get(path)
        assert r.status_code == 200, path
        out.append((path, r.text.split("</head>", 1)[0]))
    return out


def test_the_share_image_is_a_real_1200_by_630_png() -> None:
    data = IMAGE.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n" and data[12:16] == b"IHDR"
    assert struct.unpack(">II", data[16:24]) == (1200, 630)


def test_every_sitemap_page_has_one_complete_share_card_and_honest_lengths() -> None:
    problemes = []
    for path, head in _pages():
        metas: dict[str, list[str]] = {}
        for _kind, key, value in META_RE.findall(head):
            metas.setdefault(key.lower(), []).append(html.unescape(value))
        for key in ("og:image", "og:type", "og:title", "og:description", "og:url", "twitter:card", "description"):
            if len(metas.get(key, [])) != 1:
                problemes.append(f"{path}: {key} x{len(metas.get(key, []))}")
        image = (metas.get("og:image") or [""])[0]
        if not (image.startswith("http") and image.endswith("/static/brand/noyaru-partage.png")):
            problemes.append(f"{path}: og:image {image!r}")
        if (metas.get("twitter:card") or [""])[0] != "summary_large_image":
            problemes.append(f"{path}: twitter:card {metas.get('twitter:card')}")
        desc = (metas.get("description") or [""])[0]
        if not 100 <= len(desc) <= 160:
            problemes.append(f"{path}: description {len(desc)} car.")
        title = html.unescape(re.search(r"<title>(.*?)</title>", head, re.S).group(1).strip())
        if not 15 <= len(title) <= 70:
            problemes.append(f"{path}: titre {len(title)} car. ({title})")
    assert problemes == []
