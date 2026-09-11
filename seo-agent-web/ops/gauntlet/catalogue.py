# -*- coding: utf-8 -*-
"""Le parcours d'obstacles, decrit SANS HTML : une anomalie par entree, neuf ecritures possibles.

`build_pages.py` decrit les memes 31 pages, mais directement en HTML statique — ce qui les rend
intransportables. Or les neuf stacks n'ecrivent pas un `<head>` de la meme facon : Next App
Router le declare dans un objet TypeScript, Nuxt dans un appel `useHead()`, Astro/Jekyll/Hugo
dans du front matter (JS, YAML, TOML), SvelteKit et Next Pages dans du balisage encapsule,
Gatsby dans une fonction `Head()`. Le correcteur doit savoir ecrire dans les neuf, et c'est
precisement ce qui n'a jamais ete exerce : le defaut du 09/09 (un objet `metadata` casse par le
garde-fou de plafond) vivait dans la seule ecriture que le banc ne portait pas.

Une `Spec` decrit donc l'anomalie, jamais sa syntaxe. Chaque emetteur de `emit_stacks.py` la rend
dans l'idiome de sa stack, et DECLARE ce qu'il ne peut pas exprimer plutot que d'inventer : une
famille qu'un generateur rend structurellement impossible est un renseignement (le correcteur ne
la verra jamais sur cette stack), pas un trou a combler.
"""

from __future__ import annotations

from dataclasses import dataclass, field

TITLE = "Page de test du parcours d'obstacles Noyaru"
DESC = ("Page du parcours d'obstacles : elle est correcte partout sauf sur un point precis, "
        "afin que la famille visee soit la seule a se declencher au crawl.")

# duplicate_titles / duplicate_meta_descriptions ont besoin de DEUX pages identiques.
DUP_T = "Deux pages qui portent exactement le meme titre pour le test"
DUP_D = ("Deux pages qui portent exactement la meme meta description, afin de declencher la "
         "famille des doublons.")

LONG_T = ("Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, "
          "ecrit pour depasser nettement le plafond")
LONG_D = ("Une meta description volontairement tres longue, ecrite pour depasser le plafond de "
          "cent soixante caracteres retenu par le crawler, afin de declencher la famille des "
          "descriptions trop longues sur une page non indexable.")


@dataclass(frozen=True)
class Spec:
    """Une page du parcours : correcte partout, sauf sur le point que la famille visee mesure.

    Les champs decrivent un ETAT observable au crawl, pas une syntaxe. `title=None` veut dire
    « cette page ne declare pas de titre », charge a l'emetteur de savoir si cela s'obtient en
    retirant une balise, une cle de front matter ou une propriete d'objet.
    """

    slug: str
    family: str
    note: str

    title: str | None = TITLE
    desc: str | None = DESC
    # None = pas d'attribut lang du tout ; sinon la valeur telle quelle (y compris invalide).
    lang: str | None = "fr"
    # None = canonical vers soi-meme ; "" = aucun canonical ; sinon un slug du parcours, ou une
    # URL absolue si elle commence par http.
    canonical: str | None = None
    robots: str = ""

    og: str = "full"          # full | none | partial (og:title seul)
    tw: str = "full"          # full | none | partial (twitter:card seul)
    h1: int = 1               # 0, 1 ou 2
    viewport: bool = True

    second_title: bool = False
    second_desc: bool = False

    # (code de langue, cible) — cible = slug du parcours ou URL absolue.
    hreflang: tuple[tuple[str, str], ...] = ()
    jsonld_bad_price: bool = False

    # Ressources et liens du corps, tous exprimes en intention plutot qu'en balisage.
    http_image: bool = False
    http_css: bool = False
    http_js: bool = False
    http_link: str = ""        # slug interne a lier en http
    double_slash_link: str = ""
    link_to_redirect: bool = False

    # Les RESSOURCES. Sept familles revendiquees par le correcteur n'avaient aucune page pour
    # les porter — le parcours ne chargeait que des assets sains et directs. Elles exigent aussi
    # `--check-resources` au crawl, faute de quoi 26 cles n'existent meme pas dans le rapport
    # (voir crawler-detection-gaps en memoire).
    img_no_alt: bool = False        # missing_alt_text
    redirected_image: bool = False  # image_redirects + page_has_redirected_image
    redirected_css: bool = False    # css_redirects + page_has_redirected_css
    redirected_js: bool = False     # javascript_redirects + page_has_redirected_javascript

    # Renseigne par l'emetteur quand la stack ne peut pas exprimer la spec (voir le module
    # emetteur) — jamais ecrit a la main ici.
    tags: tuple[str, ...] = field(default_factory=tuple)


CATALOGUE: list[Spec] = [
    # ── A. balises <head> manquantes ou dupliquees ────────────────────────────────────────
    Spec("missing-title", "missing_title", "aucun titre declare.", title=None),
    # La famille N'EST PAS `missing_meta_description` : le crawler ne la reserve qu'aux pages
    # NON indexables, et replie le cas indexable dans `meta_description_too_short_indexable`
    # — parite Ahrefs, ecrite noir sur blanc dans seo_audit.py. Mesure : la page est bien servie
    # sans aucune balise description, et c'est bien la variante « trop courte » qui se declenche.
    Spec("missing-meta-description", "meta_description_too_short_indexable",
         "aucune meta description sur une page indexable.", desc=None),
    Spec("missing-h1", "missing_h1", "aucun <h1> dans le corps.", h1=0),
    Spec("multiple-h1", "multiple_h1", "deux <h1> sur la meme page.", h1=2),
    Spec("multiple-title-tags", "multiple_title_tags", "deux balises <title>.",
         second_title=True),
    Spec("multiple-meta-description-tags", "multiple_meta_description_tags",
         "deux meta description.", second_desc=True),
    Spec("title-too-short", "title_too_short", "titre de moins de 15 caracteres.",
         title="Test"),
    Spec("viewport-not-set", "viewport_not_set", "aucune meta viewport.", viewport=False),
    Spec("duplicate-a", "duplicate_titles + duplicate_meta_descriptions",
         "jumelle de duplicate-b : meme titre ET meme description.", title=DUP_T, desc=DUP_D),
    Spec("duplicate-b", "duplicate_titles + duplicate_meta_descriptions",
         "jumelle de duplicate-a.", title=DUP_T, desc=DUP_D),

    # ── B. canonical ──────────────────────────────────────────────────────────────────────
    Spec("canonical-http", "canonical_from_https_to_http",
         "page servie en https, canonical en http.", canonical="!http:canonical-http"),
    # Le maillon PROPRE de la chaine canonique, et il a fallu le mesurer pour comprendre qu'il
    # manquait. `canonical-other` a besoin d'une cible qui declare elle-meme un canonical
    # DIFFERENT et en https. Tant que cette cible etait `canonical-http`, dont le canonical est
    # en http par construction, suivre la chaine revenait a ecrire une URL http — ce que le
    # garde-fou anti-retrogradation refuse a juste titre. La famille etait donc structurellement
    # inexercable : le correcteur avait raison, c'est le parcours qui enchainait deux defauts.
    Spec("canonical-relay", "sitemap_non_canonical_page",
         "page non canonique listee au sitemap : son canonical vise une autre page, en https.",
         canonical="missing-h1"),
    Spec("canonical-other", "non_canonical_page_specified_as_canonical_one",
         "canonical vers une page qui porte elle-meme un autre canonical.",
         canonical="canonical-relay"),
    Spec("no-canonical-a", "duplicate_pages_without_canonical",
         "jumelle de no-canonical-b, aucune des deux ne declare de canonical.",
         canonical="", title="Deux pages jumelles sans canonical declare", desc=DUP_D),
    Spec("no-canonical-b", "duplicate_pages_without_canonical", "jumelle de no-canonical-a.",
         canonical="", title="Deux pages jumelles sans canonical declare", desc=DUP_D),

    # ── C. hreflang ───────────────────────────────────────────────────────────────────────
    # `zz-ZZ` etait un mauvais choix, mesure : le controle du crawler est SYNTAXIQUE
    # (`^(x-default|[a-z]{2}(-[a-z0-9]{2,8})*)$`) et `zz-ZZ` le satisfait parfaitement. Un code
    # inexistant mais bien forme ne declenche rien. Le vrai defaut de terrain est le
    # soulignement a la place du tiret, qu aucun registre n accepte.
    Spec("hreflang-invalid", "hreflang_annotation_invalid",
         "code de langue mal forme (soulignement au lieu du tiret).",
         hreflang=(("fr_FR", "hreflang-invalid"),)),
    Spec("hreflang-no-html-lang", "hreflang_defined_but_html_lang_missing",
         "hreflang declare mais <html> sans attribut lang.", lang=None,
         hreflang=(("fr", "hreflang-no-html-lang"), ("en", "hreflang-to-non-canonical"))),
    Spec("hreflang-to-non-canonical", "hreflang_to_non_canonical",
         "hreflang pointant vers une page dont le canonical est ailleurs.",
         hreflang=(("fr", "hreflang-to-non-canonical"), ("en", "canonical-other"))),
    # Le controle du crawler fusionne les hreflang de la TETE de page et ceux du sitemap, puis
    # regarde si un meme code mene a plus d'une URL (`_code_to_urls[code]`). Deux annotations
    # `fr` vers deux pages differentes suffisent donc, sans rien demander au sitemap.
    Spec("hreflang-same-language", "more_than_one_page_for_same_language_in_hreflang",
         "deux annotations hreflang pour le meme code, vers deux pages differentes.",
         hreflang=(("fr", "hreflang-same-language"), ("fr", "canonical-other"))),

    # ── D. Open Graph / Twitter ───────────────────────────────────────────────────────────
    Spec("og-missing", "open_graph_tags_missing", "aucune balise Open Graph.", og="none"),
    Spec("og-incomplete", "open_graph_tags_incomplete",
         "og:title seul, sans description ni image ni url.", og="partial"),
    Spec("twitter-missing", "twitter_card_missing", "aucune balise twitter.", tw="none"),
    Spec("twitter-incomplete", "twitter_card_incomplete", "twitter:card seul.", tw="partial"),

    # ── E. http / https et doubles barres ─────────────────────────────────────────────────
    Spec("mixed-image", "https_page_links_to_http_image + https_http_mixed_content",
         "image chargee en http sur une page https.", http_image=True),
    Spec("mixed-css", "https_page_links_to_http_css",
         "feuille de style chargee en http.", http_css=True),
    Spec("mixed-js", "https_page_links_to_http_javascript",
         "script charge en http.", http_js=True),
    Spec("link-http", "https_page_has_internal_links_to_http",
         "lien interne ecrit en http.", http_link="a-propos"),
    Spec("double-slash", "double_slash_in_url", "lien interne avec une double barre.",
         double_slash_link="a-propos"),
    Spec("link-to-redirect", "page_has_links_to_redirect",
         "lien vers une URL qui redirige (declaree cote hote).", link_to_redirect=True),

    # ── F. ressources : alt manquant et assets qui redirigent ─────────────────────────────
    # Quatre pages pour SEPT familles. Chaque famille de redirection en produit deux — une sur
    # la ressource (`image_redirects`), une sur la page qui la charge
    # (`page_has_redirected_image`) — et le crawler les leve ensemble.
    Spec("missing-alt", "missing_alt_text",
         "image sans attribut alt.", img_no_alt=True),
    Spec("redirected-image", "image_redirects + page_has_redirected_image",
         "image chargee depuis une URL qui redirige.", redirected_image=True),
    Spec("redirected-css", "css_redirects + page_has_redirected_css",
         "feuille de style chargee depuis une URL qui redirige.", redirected_css=True),
    Spec("redirected-js", "javascript_redirects + page_has_redirected_javascript",
         "script charge depuis une URL qui redirige.", redirected_js=True),

    # ── G. donnees structurees ────────────────────────────────────────────────────────────
    # `SCHEMA_ORG_HARD_ERRORS` ne contient que `invalid_json` et `missing_type` ; un prix en
    # chaine appartient a `RICH_RESULTS_ERRORS`. Mesure : la page produit bien
    # `schema_org_errors: ['offer_price_is_string']` et c'est la famille rich results qui compte.
    # `structured_data_schema_org_validation_error` demanderait un JSON casse ou sans @type —
    # une page a ajouter au parcours, pas une correction de celle-ci.
    Spec("schema-invalid", "structured_data_google_rich_results_validation_error",
         "Offer dont le prix est une chaine.", jsonld_bad_price=True),

    # ── H. attribut lang ──────────────────────────────────────────────────────────────────
    Spec("html-lang-missing", "html_lang_attribute_missing",
         "<html> sans attribut lang.", lang=None),
    Spec("html-lang-invalid", "html_lang_attribute_invalid",
         "attribut lang syntaxiquement invalide.", lang="francais"),

    # ── I. longueurs, sur une page non indexable ──────────────────────────────────────────
    # `missing_meta_description` ne tire QUE sur une page non indexable — parite Ahrefs, ecrite
    # dans seo_audit.py : `not _non_empty(p.meta_description) and not _is_indexable(p)`. Le cas
    # indexable se replie dans `meta_description_too_short_indexable`, que porte deja la page
    # `missing-meta-description`. Il faut donc une SECONDE page : sans description ET noindex.
    Spec("noindex-no-description", "missing_meta_description",
         "page noindex ne declarant aucune meta description.",
         desc=None, robots="noindex, follow"),
    Spec("noindex-long", "title_too_long_not_indexable + meta_description_too_long_not_indexable",
         "page noindex portant un titre et une description hors plafond.",
         title=LONG_T, desc=LONG_D, robots="noindex, follow"),
]


def families() -> list[str]:
    """Les familles distinctes que le parcours declenche, une par ligne du catalogue."""
    out: list[str] = []
    for spec in CATALOGUE:
        for name in spec.family.split("+"):
            name = name.strip()
            if name and name not in out:
                out.append(name)
    return out


if __name__ == "__main__":
    print(f"{len(CATALOGUE)} pages, {len(families())} familles")
    for name in families():
        print("   " + name)
