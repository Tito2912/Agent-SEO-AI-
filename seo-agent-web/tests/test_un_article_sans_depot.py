# -*- coding: utf-8 -*-
"""Le mode « contenu seul » : un article rédigé depuis le CRAWL, sans dépôt GitHub.

Décision du propriétaire, 28/09/2026 : le client repart avec l'article (Markdown et HTML) et le
publie lui-même. La source n'est plus un dépôt mais le crawl du site, qui garde pour chaque page
son titre, sa description, sa langue, son plan (H2) et ses hreflang.

La fixture reprend la FORME mesurée sur prosperfactory.com : guides en français et en allemand,
anglais servi à la racine, hreflang entre les versions — et le piège qui a déjà coûté cher au mode
GitHub : une page d'UNE seule langue en tête de l'alphabet.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")

from backend import app as m  # noqa: E402

S = "https://site.fr"


def _p(chemin, lang, titre, *, h2=(), hreflang=None, article=True, mots=800, status=200,
       robots=""):
    return {"url": S + chemin, "final_url": S + chemin, "status_code": status, "title": titre,
            "meta_description": "Description de " + titre, "served_lang": lang, "lang": lang,
            "h2": list(h2), "hreflang": hreflang or {}, "article_like": article,
            "text_word_count": mots, "meta_robots": robots}


DCA = {"fr": S + "/fr/guides/dca-crypto/", "de": S + "/de/guides/krypto-dca/",
       "en": S + "/guides/crypto-dca/"}
DEBUTER = {"fr": S + "/fr/guides/debuter/", "de": S + "/de/guides/anfangen/",
           "en": S + "/guides/start/"}
CRAWL = [
    _p("/fr/guides/", "fr", "Guides", article=False),
    # Le piege : francais seul, premier de l'alphabet, article court.
    _p("/fr/guides/aaa-page-seule/", "fr", "Page seule", mots=300),
    _p("/fr/guides/dca-crypto/", "fr", "DCA crypto : guide", h2=["Définition", "Frais"],
       hreflang=DCA, mots=1500),
    _p("/fr/guides/debuter/", "fr", "Débuter", hreflang=DEBUTER),
    _p("/de/guides/krypto-dca/", "de", "Krypto DCA", h2=["Definition", "Kosten"], hreflang=DCA),
    _p("/de/guides/anfangen/", "de", "Anfangen", hreflang=DEBUTER),
    _p("/guides/crypto-dca/", "en", "Crypto DCA", h2=["Definition", "Fees"], hreflang=DCA),
    _p("/guides/start/", "en", "Start", hreflang=DEBUTER),
    _p("/fr/guides/disparue/", "fr", "Disparue", status=404),
    _p("/fr/guides/retiree/", "fr", "Retirée", robots="noindex, follow"),
]
PAGES = m._pages_du_crawl(CRAWL)

# Ce que le modele ecrit dans chaque langue : il DEVINE les liens traduits, comme en production
# le 28/09/2026 (`/de/guides/dca-krypto/` pour `krypto-dca`).
LIENS = {
    "fr": "[DCA](/fr/guides/dca-crypto/) et [inventé](/fr/guides/levier-explique/)",
    "de": "[DCA](/de/guides/dca-krypto/) und [erfunden](/de/guides/hebel-erklaert/)",
    "en": "[DCA](https://site.fr/guides/dca-crypto/) and [made up](/guides/leverage-explained/)",
}


@pytest.fixture()
def modele(monkeypatch):
    vues: dict[str, str] = {}

    def _ai(*, system, user_msg, **kw):
        langue = re.search(r"LANGUE DE L'ARTICLE : (\S+)", user_msg).group(1)
        vues[langue] = user_msg
        return {"titre": "Titre %s" % langue, "description": "Desc %s" % langue,
                "slug": "Slug %s!" % langue,
                "markdown": "# Titre en trop\n\n## Partie\n\nTexte. %s\n" % LIENS[langue]}

    monkeypatch.setattr(m, "_correction_ai_json", _ai)
    return vues


def _preparer(**kw):
    return m._preparer_des_articles(pages=PAGES, sujet="Le levier", site_name="site.fr", **kw)


# ── ce que le crawl fournit ───────────────────────────────────────────────────────────────────

def test_seules_les_pages_PUBLIEES_comptent() -> None:
    chemins = {p["chemin"] for p in PAGES}
    assert "/fr/guides/disparue" not in chemins and "/fr/guides/retiree" not in chemins


def test_les_sections_se_mesurent_les_plus_fournies_d_abord() -> None:
    assert m._sections_du_crawl(PAGES)[0] == {"section": "/fr/guides", "pages": 3, "articles": 3}
    assert {"section": "/de/guides", "pages": 2, "articles": 2} in m._sections_du_crawl(PAGES)


def test_une_RUBRIQUE_passe_devant_le_premier_niveau_d_une_langue_plus_fourni() -> None:
    """Mesure du 28/09/2026 : `/fr` (a propos, contact…) passait devant `/fr/guides` et aurait
    ete l'option choisie par defaut. Sur ce site TOUTES les pages portent `<article>` et un H1 —
    un seul gabarit : le critere « article » du crawler ne departage rien, d'ou des pages
    institutionnelles marquees article ICI AUSSI."""
    pages = m._pages_du_crawl(CRAWL + [_p("/fr/%s/" % n, "fr", n, article=True)
                                       for n in ("a-propos", "contact", "mentions", "cgu")])
    sections = m._sections_du_crawl(pages)
    assert sections[0]["section"] == "/fr/guides", sections
    assert {"section": "/fr", "pages": 5, "articles": 4} in sections, "proposee quand meme"


def test_la_langue_SERVIE_prime_sur_celle_corrigee_par_JavaScript() -> None:
    """Mesure du 12/09/2026 sur un site client : 93 pages servent `lang="en"` et ne passent en
    fr/de/es qu'une fois JavaScript execute. Un moteur lit d'abord la premiere."""
    page = dict(CRAWL[2], served_lang="en-US", lang="fr")
    assert m._pages_du_crawl([page])[0]["langue"] == "en"


def test_une_section_d_UNE_seule_page_n_est_pas_proposee() -> None:
    """Une seule page ne dit pas quelle forme ont les suivantes."""
    pages = m._pages_du_crawl(CRAWL + [_p("/fr/outils/unique/", "fr", "Unique")])
    assert "/fr/outils" not in {s["section"] for s in m._sections_du_crawl(pages)}


def test_le_modele_est_un_ARTICLE_fourni_pas_le_premier_de_l_alphabet() -> None:
    assert m._modele_de_section(PAGES, "/fr/guides")["chemin"] == "/fr/guides/dca-crypto"


# ── la redaction ──────────────────────────────────────────────────────────────────────────────

def test_chaque_version_suit_les_HREFLANG_de_la_page_modele(modele) -> None:
    out = _preparer(section="/fr/guides", langues=["de", ""])
    assert out["ok"], out
    v = {x["code"]: x for x in out["versions"]}
    assert v["fr"]["adresse"] == "/fr/guides/slug-fr/"
    assert v["de"]["adresse"] == "/de/guides/slug-de/"
    # L'anglais est servi a la RACINE : sa langue se mesure sur les pages qui y vivent.
    assert v["en"]["adresse"] == "/guides/slug-en/"
    assert "Krypto DCA" in modele["de"] and "Crypto DCA" in modele["en"]


def test_une_traduction_recoit_le_TEXTE_de_la_principale(modele) -> None:
    _preparer(section="/fr/guides", langues=["de"])
    assert "CET ARTICLE EST UNE TRADUCTION" in modele["de"]
    assert "[inventé]" in modele["de"], "le texte francais n'a pas ete transmis"
    assert "TRADUCTION" not in modele["fr"]


def test_les_liens_proposes_sont_ceux_de_SA_langue(modele) -> None:
    _preparer(section="/fr/guides", langues=["de"])
    assert "/de/guides/krypto-dca/" in modele["de"] and "/fr/guides/" not in modele["de"].split(
        "CET ARTICLE")[0]


def test_un_lien_devine_est_repare_par_les_hreflang_et_un_lien_invente_retire(modele) -> None:
    out = _preparer(section="/fr/guides", langues=["de", ""])
    v = {x["code"]: x["contenu"] for x in out["versions"]}
    assert "](/de/guides/krypto-dca/)" in v["de"] and "dca-krypto" not in v["de"]
    assert "hebel-erklaert" not in v["de"] and "erfunden" in v["de"], "le texte du lien reste"
    assert "levier-explique" not in v["fr"]
    # Un lien ABSOLU vers le site est ramene en relatif, donc controle.
    assert "](/guides/crypto-dca/)" in v["en"] and "leverage-explained" not in v["en"]


def test_le_titre_de_niveau_1_du_corps_est_retire(modele) -> None:
    out = _preparer(section="/fr/guides")
    assert not out["versions"][0]["contenu"].startswith("# ")


def test_une_section_SANS_page_est_refusee_avant_le_modele(modele) -> None:
    out = _preparer(section="/fr/blog")
    assert not out["ok"] and out["status"] == 422 and modele == {}, out


def test_une_langue_que_le_site_ne_sert_pas_est_refusee_avant_le_modele(modele) -> None:
    out = _preparer(section="/fr/guides", langues=["it"])
    assert not out["ok"] and out["status"] == 400 and modele == {}, out


# ── le HTML rendu ─────────────────────────────────────────────────────────────────────────────

def test_le_HTML_n_embarque_aucune_balise_ecrite_par_le_modele() -> None:
    html = m._article_en_html("## Partie\n\n<script>alert(1)</script>\n\n> Une citation.\n")
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "<blockquote>" in html, "la citation Markdown doit survivre a l'echappement"
    assert '<h2 id="partie">' in html
