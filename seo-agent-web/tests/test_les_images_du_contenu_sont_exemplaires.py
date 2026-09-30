# -*- coding: utf-8 -*-
"""Les images de la documentation et des guides sont exemplaires, ou refusées.

30/09/2026 : le propriétaire veut des captures d'écran dans ses pages. Un produit qui vend du SEO
doit être irréprochable sur ses propres pages : dimensions déclarées (aucun décalage au
chargement), chargement différé, texte alternatif obligatoire, légende. Une image sans
alternative, ou absente de `static/`, est refusée au chargement — comme une page mal formée.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from PIL import Image

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import content_library as cl  # noqa: E402

PAGE = Path("page-essai.md")


@pytest.fixture()
def statique(tmp_path, monkeypatch):
    (tmp_path / "captures").mkdir()
    Image.new("RGB", (1600, 900), (20, 20, 22)).save(tmp_path / "captures" / "vue.webp", "WEBP")
    monkeypatch.setattr(cl, "STATIC_DIR", tmp_path)
    return tmp_path


def _rendu(markdown_source: str) -> str:
    md = cl._markdown_instance()
    return cl._figures(md.convert(markdown_source), source=PAGE)


def test_une_image_devient_une_FIGURE_complete(statique) -> None:
    html = _rendu('Texte.\n\n![Vue d\'ensemble du projet](/static/captures/vue.webp "La vue après un crawl")\n\nSuite.')
    assert '<figure class="content-figure">' in html
    assert 'width="1600" height="900"' in html, "dimensions lues dans le fichier"
    assert 'loading="lazy"' in html and 'decoding="async"' in html
    assert "<figcaption>La vue après un crawl</figcaption>" in html
    assert 'alt="Vue d&#x27;ensemble du projet"' in html
    assert "<p><img" not in html


def test_un_clic_ouvre_l_image_en_GRAND(statique) -> None:
    html = _rendu("![Vue d'ensemble](/static/captures/vue.webp)")
    assert '<a class="content-figure-lien" href="/static/captures/vue.webp" target="_blank" rel="noopener"' in html


def test_toutes_les_captures_publiees_sont_UTILISEES_et_toutes_les_utilisees_EXISTENT() -> None:
    """Pas d'image orpheline dans static/captures/, pas de lien vers une image absente."""
    captures = {p.stem for p in (cl.STATIC_DIR / "captures").glob("*.webp")}
    textes = [p.read_text(encoding="utf-8") for p in cl.CONTENT_ROOT.rglob("*.md")]
    textes += [p.read_text(encoding="utf-8") for p in (WEB_ROOT / "templates").glob("*.html")]
    import re
    utilisees = set()
    for t in textes:
        utilisees |= set(re.findall(r"/static/captures/([a-z0-9-]+)\.webp", t))
        utilisees |= set(re.findall(r'capture\("([a-z0-9-]+)"', t))
    assert captures, "le témoin : il y a des captures"
    assert not captures - utilisees, "captures inutilisées : %s" % sorted(captures - utilisees)
    assert not utilisees - captures, "captures absentes : %s" % sorted(utilisees - captures)


def test_les_captures_de_l_ACCUEIL_declarent_leurs_vraies_dimensions() -> None:
    import os
    import re
    import tempfile

    os.environ.setdefault("SEO_AGENT_DATA_DIR", tempfile.mkdtemp(prefix="seo-agent-img-"))
    os.environ.setdefault("SEO_AGENT_RUNS_DIR", tempfile.mkdtemp(prefix="seo-agent-img-runs-"))
    os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
    os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
    from fastapi.testclient import TestClient

    from backend import app as m

    page = TestClient(m.app).get("/").text
    images = re.findall(r'<img src="/static/captures/([a-z0-9-]+)\.webp" alt="([^"]+)" width="(\d+)" height="(\d+)"', page)
    assert len(images) >= 4, images
    for nom, alt, largeur, hauteur in images:
        assert (int(largeur), int(hauteur)) == cl.dimensions_capture(nom), nom
        assert alt.strip(), nom


def test_sans_legende_pas_de_figcaption_vide(statique) -> None:
    html = _rendu("![Vue d'ensemble](/static/captures/vue.webp)")
    assert "<figcaption>" not in html and "<figure" in html


def test_les_caracteres_ne_sont_pas_DOUBLEMENT_echappes(statique) -> None:
    html = _rendu('![Anomalies & priorités](/static/captures/vue.webp "Titres & descriptions")')
    assert "&amp;amp;" not in html
    assert "<figcaption>Titres &amp; descriptions</figcaption>" in html


def test_une_image_SANS_texte_alternatif_est_refusee(statique) -> None:
    with pytest.raises(cl.ContentError, match="texte alternatif"):
        _rendu("![](/static/captures/vue.webp)")


def test_une_image_INTROUVABLE_est_refusee(statique) -> None:
    with pytest.raises(cl.ContentError, match="introuvable"):
        _rendu("![Vue](/static/captures/absente.webp)")


@pytest.mark.parametrize("src", ["https://ailleurs.test/x.png", "/static/../backend/app.py",
                                 # même longueur que « /static/ » : sans le contrôle du préfixe, la
                                 # suite du chemin tombe sur un vrai fichier et l'image passe, cassée.
                                 "/public/captures/vue.webp"])
def test_une_image_HORS_de_static_est_refusee(statique, src) -> None:
    with pytest.raises(cl.ContentError):
        _rendu("![Vue](%s)" % src)


def test_les_vraies_pages_se_chargent_toutes() -> None:
    """Les pages publiées passent ces règles (sinon LOAD_ERRORS les signalerait)."""
    assert cl.LOAD_ERRORS == []
