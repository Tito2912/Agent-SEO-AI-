# -*- coding: utf-8 -*-
"""Le HTML d'un article rédigé ne porte aucun code actif écrit par le modèle.

Revue du 20/11/2026 : les chevrons étaient échappés avant la conversion, mais le Markdown sait
poser du code actif SANS chevron. `attr_list`, comprise dans l'extension `extra`, faisait de
`{: onclick="..." }` un vrai attribut ; un lien `[x](javascript:...)` passait tel quel, et même
déguisé en entité (`&#106;avascript:`), que le sérialiseur garde et que le navigateur décode. Ce
HTML repart chez le client : .zip, copier-coller du mode contenu seul, publication dans son CMS.

Les assertions lisent le HTML comme un NAVIGATEUR le lirait (entités décodées, blancs retirés du
protocole), pas comme une chaîne : un test sur la chaîne aurait laissé passer l'entité.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")

from backend import app as m  # noqa: E402
from backend import content_library as cl  # noqa: E402

_PROTOCOLE = re.compile(r"^([a-z][a-z0-9+.\-]*):", re.I)


def _protocole_lu_par_le_navigateur(valeur: str) -> str:
    """BeautifulSoup a déjà décodé les entités ; le navigateur ignore aussi blancs et contrôles."""
    compacte = re.sub(r"[\x00-\x20\x7f]", "", valeur)
    trouve = _PROTOCOLE.match(compacte)
    return trouve.group(1).lower() if trouve else ""


def _code_actif(html_texte: str) -> list[str]:
    """Tout ce qu'un navigateur pourrait exécuter dans ce HTML : attributs `on*`, adresses actives."""
    trouve = []
    for balise in BeautifulSoup(html_texte, "html.parser").find_all(True):
        for nom, valeur in balise.attrs.items():
            valeur = " ".join(valeur) if isinstance(valeur, list) else str(valeur)
            if nom.lower().startswith("on"):
                trouve.append("<%s %s>" % (balise.name, nom))
            elif nom.lower() in {"href", "src"} and _protocole_lu_par_le_navigateur(valeur) not in {
                    "", "http", "https", "mailto", "tel"}:
                trouve.append("<%s %s=%r>" % (balise.name, nom, valeur))
    return trouve


# ── les attributs posés par `{: ... }` ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("markdown_texte", [
    'Un paragraphe.\n{: onclick="alert(1)" }\n',
    '## Un titre {: onmouseover="alert(1)" }\n\nTexte.\n',
    '- un point\n{: onfocus="alert(1)" autofocus }\n',
    '```{ .python onclick="alert(1)" }\nprint(1)\n```\n',
], ids=["paragraphe", "titre", "liste", "bloc-de-code"])
def test_la_syntaxe_d_attributs_ne_pose_aucun_attribut_d_evenement(markdown_texte) -> None:
    html_texte = m._article_en_html(markdown_texte)
    assert _code_actif(html_texte) == [], html_texte


# ── les liens et images à protocole actif ──────────────────────────────────────────────────────

@pytest.mark.parametrize("markdown_texte", [
    "[clic](javascript:alert(1))",
    "[clic](JaVaScRiPt:alert(1))",
    "[clic](&#106;avascript:alert(1))",
    "[clic](&#x6A;avascript:alert(1))",
    "[clic](javascript&colon;alert(1))",
    "[clic](java&#9;script:alert(1))",
    "[clic](vbscript:msgbox(1))",
    "[clic](data:text/html;base64,PHNjcmlwdD5hbGVydCgxKTwvc2NyaXB0Pg==)",
    "[clic][1]\n\n[1]: javascript:alert(1)\n",
    "![image](javascript:alert(1))",
    "![image](data:image/svg+xml;base64,PHN2Zz48L3N2Zz4=)",
], ids=["direct", "majuscules", "entite-decimale", "entite-hexa", "entite-nommee",
        "tabulation", "vbscript", "data-html", "reference", "image-javascript", "image-data"])
def test_aucun_lien_ni_image_ne_garde_un_protocole_actif(markdown_texte) -> None:
    html_texte = m._article_en_html(markdown_texte)
    assert _code_actif(html_texte) == [], html_texte
    # Le texte du lien reste lisible : on retire l'adresse, pas les mots du rédacteur.
    if markdown_texte.startswith("[clic]"):
        assert "clic" in BeautifulSoup(html_texte, "html.parser").get_text()


# ── ce qui doit survivre ───────────────────────────────────────────────────────────────────────

def test_le_contenu_editorial_reste_intact() -> None:
    markdown_texte = (
        "## Partie\n\n"
        "Voir [le guide](https://exemple.fr/guide?a=1&b=2), [nos offres](/offres/), "
        "[plus bas](#partie), [écrire](mailto:contact@exemple.fr) ou [appeler](tel:+33100000000).\n\n"
        "![Une chaîne usée](images/chaine-usee-1.webp)\n\n"
        "| Offre | Prix |\n|:---|---:|\n| Solo | 10 |\n\n"
        "Une source[^1].\n\n[^1]: La note.\n\n"
        "```\ncode\n```\n"
    )
    html_texte = m._article_en_html(markdown_texte)
    soupe = BeautifulSoup(html_texte, "html.parser")
    assert _code_actif(html_texte) == []
    assert soupe.find("h2", id="partie") is not None
    assert [a.get("href") for a in soupe.find_all("a") if not a.get("class")] == [
        "https://exemple.fr/guide?a=1&b=2", "/offres/", "#partie",
        "mailto:contact@exemple.fr", "tel:+33100000000"]
    assert soupe.find("img").get("src") == "images/chaine-usee-1.webp"
    assert soupe.find("th").get("style") == "text-align: left;"
    assert soupe.find("a", class_="footnote-ref").get("href") == "#fn:1"
    assert soupe.find("pre") is not None


def test_les_chevrons_restent_echappes() -> None:
    """Le garde-fou d'avant ne régresse pas : aucune balise écrite par le modèle."""
    html_texte = m._article_en_html("## Partie\n\n<script>alert(1)</script>\n\n> Une citation.\n")
    assert "<script>" not in html_texte and "&lt;script&gt;" in html_texte
    assert "<blockquote>" in html_texte


def test_la_version_servie_au_client_est_couverte_de_bout_en_bout() -> None:
    """Le chemin réel : la version telle que l'écran, le .zip et le CMS la reçoivent."""
    version = {"langue": "Français", "code": "fr", "titre": "Changer sa chaîne", "slug": "changer-chaine",
               "description": "Quand changer sa chaîne.", "adresse": "/guides/changer-chaine",
               "markdown": ('Intro.\n{: onclick="alert(1)" }\n\n'
                            "![Une chaîne](images/changer-chaine-1.webp)\n\n"
                            "[Source](&#106;avascript:alert(1))\n")}
    servie = m._versions_servies([version], {"changer-chaine-1.webp": (1200, 800)})[0]
    assert _code_actif(servie["html"]) == [], servie["html"]
    image = BeautifulSoup(servie["html"], "html.parser").find("img")
    assert (image.get("width"), image.get("height")) == ("1200", "800")


# ── la règle elle-même ─────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("valeur,sure", [
    ("https://exemple.fr/a", True), ("http://exemple.fr", True), ("/chemin", True),
    ("images/x.webp", True), ("#ancre", True), ("mailto:a@b.fr", True), ("tel:+33", True),
    ("?page=2", True), ("", True),
    ("javascript:alert(1)", False), (" JAVASCRIPT:alert(1)", False),
    ("&#106;avascript:alert(1)", False), ("java\tscript:alert(1)", False),
    ("java&#x0A;script:alert(1)", False), ("data:text/html,x", False), ("vbscript:x", False),
])
def test_la_regle_des_adresses(valeur, sure) -> None:
    assert cl.adresse_sans_code_actif(valeur) is sure


def test_la_documentation_garde_son_convertisseur() -> None:
    """Les pages écrites par nous ne changent pas de convertisseur : seul l'article est visé."""
    assert "extra" in cl._MARKDOWN_EXTENSIONS
    assert "attr_list" not in cl._ARTICLE_MARKDOWN_EXTENSIONS
    assert "extra" not in cl._ARTICLE_MARKDOWN_EXTENSIONS
