# -*- coding: utf-8 -*-
"""Build a defect gauntlet: one page per never-exercised family, one defect each.

Thirty-four families are claimed by the corrector and have never occurred on any real site, so
they rest on unit tests alone — the exact status the families that produced today's eight defects
had this morning. Waiting for a customer to carry them is not a plan.

Each page below is a MINIMAL, realistic page that is correct in every respect except one. The
comment in each says which family it is for, so a reader of the repository can tell a fixture
from a mistake.
"""
import io
import os

BASE = "https://noyaru-stack-static-html.netlify.app"
OUT = os.environ["GAUNTLET_DIR"]

HEAD_OK = (
    '    <meta charset="utf-8" />\n'
    '    <meta name="viewport" content="width=device-width" />\n'
)
OG_OK = (
    '    <meta property="og:title" content="{t}" />\n'
    '    <meta property="og:description" content="{d}" />\n'
    '    <meta property="og:url" content="{u}" />\n'
    '    <meta property="og:image" content="' + BASE + '/img/cover.png" />\n'
    '    <meta property="og:type" content="article" />\n'
)
TW_OK = (
    '    <meta name="twitter:card" content="summary_large_image" />\n'
    '    <meta name="twitter:title" content="{t}" />\n'
    '    <meta name="twitter:description" content="{d}" />\n'
    '    <meta name="twitter:image" content="' + BASE + '/img/cover.png" />\n'
)
DESC = ("Page du parcours d'obstacles : elle est correcte partout sauf sur un point precis, "
        "afin que la famille visee soit la seule a se declencher au crawl.")
TITLE = "Page de test du parcours d'obstacles Noyaru"


def page(slug, *, family, note, lang='lang="fr"', title=TITLE, desc=DESC, head_extra="",
         body_extra="", canonical=None, with_og=True, with_tw=True, with_h1=True,
         with_viewport=True, robots=""):
    url = f"{BASE}/gauntlet/{slug}.html"
    can = canonical if canonical is not None else url
    head = '    <meta charset="utf-8" />\n'
    if with_viewport:
        head += '    <meta name="viewport" content="width=device-width" />\n'
    if title is not None:
        head += f"    <title>{title}</title>\n"
    if desc is not None:
        head += f'    <meta name="description" content="{desc}" />\n'
    if robots:
        head += f'    <meta name="robots" content="{robots}" />\n'
    if can:
        head += f'    <link rel="canonical" href="{can}" />\n'
    if with_og:
        head += OG_OK.format(t=title or "", d=desc or "", u=url)
    if with_tw:
        head += TW_OK.format(t=title or "", d=desc or "")
    head += head_extra
    body = "    <h1>Parcours d'obstacles</h1>\n" if with_h1 else ""
    body += ("    <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a "
             "provoquer UNE anomalie et une seule.</p>\n")
    body += '    <p><a href="/">Retour a l accueil</a></p>\n'
    body += body_extra
    return (
        "<!doctype html>\n"
        f"<html {lang}>\n"
        "  <head>\n"
        f"    <!-- FAMILLE VISEE : {family}\n         {note} -->\n"
        f"{head}"
        "  </head>\n"
        "  <body>\n"
        f"{body}"
        "  </body>\n"
        "</html>\n"
    )


PAGES: dict[str, str] = {}

# ── A. balises <head> manquantes ou dupliquees ────────────────────────────────────────────────
PAGES["missing-title"] = page("missing-title", family="missing_title",
    note="aucune balise <title>.", title=None)
PAGES["missing-meta-description"] = page("missing-meta-description",
    family="missing_meta_description", note="aucune meta description.", desc=None)
PAGES["missing-h1"] = page("missing-h1", family="missing_h1",
    note="aucun <h1> dans le corps.", with_h1=False)
PAGES["multiple-h1"] = page("multiple-h1", family="multiple_h1",
    note="deux <h1> sur la meme page.",
    body_extra="    <h1>Un second titre de niveau 1</h1>\n")
PAGES["multiple-title-tags"] = page("multiple-title-tags", family="multiple_title_tags",
    note="deux balises <title>.", head_extra="    <title>Un second titre</title>\n")
PAGES["multiple-meta-description-tags"] = page("multiple-meta-description-tags",
    family="multiple_meta_description_tags", note="deux meta description.",
    head_extra='    <meta name="description" content="Une seconde description." />\n')
PAGES["title-too-short"] = page("title-too-short", family="title_too_short",
    note="titre de moins de 15 caracteres.", title="Test")
PAGES["viewport-not-set"] = page("viewport-not-set", family="viewport_not_set",
    note="aucune meta viewport.", with_viewport=False)
# duplicate_titles + duplicate_meta_descriptions : il en faut DEUX identiques.
DUP_T = "Deux pages qui portent exactement le meme titre pour le test"
DUP_D = "Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons."
PAGES["duplicate-a"] = page("duplicate-a", family="duplicate_titles + duplicate_meta_descriptions",
    note="jumelle de duplicate-b : meme titre ET meme description.", title=DUP_T, desc=DUP_D)
PAGES["duplicate-b"] = page("duplicate-b", family="duplicate_titles + duplicate_meta_descriptions",
    note="jumelle de duplicate-a.", title=DUP_T, desc=DUP_D)

# ── B. canonical ──────────────────────────────────────────────────────────────────────────────
PAGES["canonical-http"] = page("canonical-http", family="canonical_from_https_to_http",
    note="page servie en https, canonical en http.",
    canonical="http://noyaru-stack-static-html.netlify.app/gauntlet/canonical-http.html")
PAGES["canonical-other"] = page("canonical-other",
    family="non_canonical_page_specified_as_canonical_one",
    note="canonical vers une page qui porte elle-meme un autre canonical.",
    canonical=f"{BASE}/gauntlet/canonical-http.html")
PAGES["no-canonical-a"] = page("no-canonical-a", family="duplicate_pages_without_canonical",
    note="jumelle de no-canonical-b, aucune des deux ne declare de canonical.",
    canonical="", title="Deux pages jumelles sans canonical declare", desc=DUP_D)
PAGES["no-canonical-b"] = page("no-canonical-b", family="duplicate_pages_without_canonical",
    note="jumelle de no-canonical-a.", canonical="",
    title="Deux pages jumelles sans canonical declare", desc=DUP_D)

# ── C. hreflang ───────────────────────────────────────────────────────────────────────────────
PAGES["hreflang-invalid"] = page("hreflang-invalid", family="hreflang_annotation_invalid",
    note="code de langue qui n existe pas.",
    head_extra=f'    <link rel="alternate" hreflang="zz-ZZ" href="{BASE}/gauntlet/hreflang-invalid.html" />\n')
PAGES["hreflang-no-html-lang"] = page("hreflang-no-html-lang",
    family="hreflang_defined_but_html_lang_missing",
    note="hreflang declare mais <html> sans attribut lang.", lang="",
    head_extra=(f'    <link rel="alternate" hreflang="fr" href="{BASE}/gauntlet/hreflang-no-html-lang.html" />\n'
                f'    <link rel="alternate" hreflang="en" href="{BASE}/gauntlet/hreflang-to-non-canonical.html" />\n'))
PAGES["hreflang-to-non-canonical"] = page("hreflang-to-non-canonical",
    family="hreflang_to_non_canonical",
    note="hreflang pointant vers une page dont le canonical est ailleurs.",
    head_extra=(f'    <link rel="alternate" hreflang="fr" href="{BASE}/gauntlet/hreflang-to-non-canonical.html" />\n'
                f'    <link rel="alternate" hreflang="en" href="{BASE}/gauntlet/canonical-other.html" />\n'))

# ── D. Open Graph / Twitter ───────────────────────────────────────────────────────────────────
PAGES["og-missing"] = page("og-missing", family="open_graph_tags_missing",
    note="aucune balise Open Graph.", with_og=False)
PAGES["og-incomplete"] = page("og-incomplete", family="open_graph_tags_incomplete",
    note="og:title seul, sans description ni image ni url.", with_og=False,
    head_extra='    <meta property="og:title" content="Open Graph incomplet" />\n')
PAGES["twitter-missing"] = page("twitter-missing", family="twitter_card_missing",
    note="aucune balise twitter.", with_tw=False)
PAGES["twitter-incomplete"] = page("twitter-incomplete", family="twitter_card_incomplete",
    note="twitter:card seul.", with_tw=False,
    head_extra='    <meta name="twitter:card" content="summary" />\n')

# ── E. http / https et doubles barres ─────────────────────────────────────────────────────────
PAGES["mixed-image"] = page("mixed-image", family="https_page_links_to_http_image + https_http_mixed_content",
    note="image chargee en http sur une page https.",
    body_extra='    <img src="http://noyaru-stack-static-html.netlify.app/img/cover.png" alt="Illustration de test" />\n')
PAGES["mixed-css"] = page("mixed-css", family="https_page_links_to_http_css",
    note="feuille de style chargee en http.",
    head_extra='    <link rel="stylesheet" href="http://noyaru-stack-static-html.netlify.app/style.css" />\n')
PAGES["mixed-js"] = page("mixed-js", family="https_page_links_to_http_javascript",
    note="script charge en http.",
    body_extra='    <script src="http://noyaru-stack-static-html.netlify.app/app.js"></script>\n')
PAGES["link-http"] = page("link-http", family="https_page_has_internal_links_to_http",
    note="lien interne ecrit en http.",
    body_extra='    <p><a href="http://noyaru-stack-static-html.netlify.app/a-propos.html">A propos</a></p>\n')
PAGES["double-slash"] = page("double-slash", family="double_slash_in_url",
    note="lien interne avec une double barre.",
    body_extra='    <p><a href="//noyaru-stack-static-html.netlify.app//a-propos.html">A propos</a></p>\n')
PAGES["link-to-redirect"] = page("link-to-redirect", family="page_has_links_to_redirect",
    note="lien vers une URL qui redirige (declaree dans _redirects).",
    body_extra='    <p><a href="/gauntlet/ancienne-page.html">Ancienne page</a></p>\n')

# ── G. donnees structurees ────────────────────────────────────────────────────────────────────
PAGES["schema-invalid"] = page("schema-invalid", family="structured_data_schema_org_validation_error",
    note="Offer dont le prix est une chaine.",
    head_extra=('    <script type="application/ld+json">\n'
                '      {"@context":"https://schema.org","@type":"Product","name":"Produit de test",'
                '"offers":{"@type":"Offer","price":"0","priceCurrency":"EUR"}}\n'
                '    </script>\n'))

# ── H. attribut lang ──────────────────────────────────────────────────────────────────────────
PAGES["html-lang-missing"] = page("html-lang-missing", family="html_lang_attribute_missing",
    note="<html> sans attribut lang.", lang="")
PAGES["html-lang-invalid"] = page("html-lang-invalid", family="html_lang_attribute_invalid",
    note="attribut lang syntaxiquement invalide.", lang='lang="francais"')
LONG_T = ("Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, "
          "ecrit pour depasser nettement le plafond")
LONG_D = ("Une meta description volontairement tres longue, ecrite pour depasser le plafond de "
          "cent soixante caracteres retenu par le crawler, afin de declencher la famille des "
          "descriptions trop longues sur une page non indexable.")
PAGES["noindex-long"] = page("noindex-long",
    family="title_too_long_not_indexable + meta_description_too_long_not_indexable",
    note="page noindex portant un titre et une description hors plafond.",
    title=LONG_T, desc=LONG_D, robots="noindex, follow")

os.makedirs(OUT, exist_ok=True)
for slug, html in PAGES.items():
    io.open(os.path.join(OUT, slug + ".html"), "w", encoding="utf-8", newline="\n").write(html)
print(f"{len(PAGES)} pages ecrites dans {OUT}")
for slug in sorted(PAGES):
    print("   gauntlet/" + slug + ".html")
