# -*- coding: utf-8 -*-
"""Une page anglaise montre l'interface anglaise.

02/11/2026 : l'interface, la documentation et les guides étaient traduits, mais leurs captures
d'écran montraient encore les écrans français. Chaque capture a désormais sa jumelle prise dans
l'interface anglaise, `static/captures/en/<nom>.webp`, et une page anglaise la reçoit sans rien
écrire de plus : elle garde l'adresse française, comme pour ses liens.
"""
from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

import pytest
from PIL import Image

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import content_library as cl  # noqa: E402

SRC = re.compile(r'src="/static/captures/([a-z0-9/-]+)\.webp"')


def test_une_langue_sans_sa_capture_garde_la_francaise(tmp_path, monkeypatch) -> None:
    (tmp_path / "captures" / "en").mkdir(parents=True)
    for chemin in ("captures/vue.webp", "captures/en/vue.webp", "captures/seule.webp"):
        Image.new("RGB", (8, 8)).save(tmp_path / chemin, "WEBP")
    monkeypatch.setattr(cl, "STATIC_DIR", tmp_path)
    assert cl.capture_localisee("vue", "en") == "en/vue"
    assert cl.capture_localisee("vue", "fr") == "vue"
    assert cl.capture_localisee("seule", "en") == "seule"
    md = cl._markdown_instance()
    html = cl._figures(md.convert('![View](/static/captures/vue.webp "Caption")'), source=Path("p.md"), lang="en")
    assert 'src="/static/captures/en/vue.webp"' in html and 'href="/static/captures/en/vue.webp"' in html


@pytest.mark.parametrize("collection", ["docs", "blog"])
def test_les_pages_anglaises_montrent_les_captures_anglaises(collection) -> None:
    anglaises = [p for p in cl.translated_pages(collection, "en") if SRC.search(p["body_html"])]
    francaises = [p for p in (cl.docs_pages("fr") if collection == "docs" else cl.blog_pages("fr"))
                  if SRC.search(p["body_html"])]
    assert anglaises and francaises, "le témoin : des pages ont des captures"
    for page in anglaises:
        assert all(n.startswith("en/") for n in SRC.findall(page["body_html"])), page["source"]
    for page in francaises:
        assert not any(n.startswith("en/") for n in SRC.findall(page["body_html"])), page["source"]


def test_l_accueil_anglais_montre_les_captures_anglaises() -> None:
    os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-img-"))
    os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-img-runs-"))
    os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
    os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
    from fastapi.testclient import TestClient

    from backend import app as m

    client = TestClient(m.app)
    fr, en = client.get("/").text, client.get("/en/").text
    images_en = re.findall(r'<img src="/static/captures/([a-z0-9/-]+)\.webp" alt="[^"]+" width="(\d+)" height="(\d+)"', en)
    assert len(images_en) >= 4, images_en
    for nom, largeur, hauteur in images_en:
        assert nom.startswith("en/"), nom
        assert (int(largeur), int(hauteur)) == cl.dimensions_capture(nom), nom
    assert 'href="/static/captures/en/anomalies.webp"' in en
    assert "/static/captures/en/" not in fr
