# -*- coding: utf-8 -*-
"""Rendre le catalogue d'anomalies dans l'idiome de chacune des neuf stacks.

Une `Spec` de `catalogue.py` dit CE QUI est anormal ; ce module dit COMMENT cela s'ecrit chez
Astro, Hugo, Jekyll, Gatsby, Nuxt, SvelteKit, Next Pages, Next App et en HTML pur. C'est la
seule chose que le banc ne mesurait pas, et c'est exactement la ou le defaut du 09/09 se cachait :
un objet `metadata` TypeScript, ecriture qu'aucune des 31 pages HTML du parcours ne portait.

Deux regles de methode, toutes deux apprises a nos depens :

* **Une stack qui ne peut pas exprimer une anomalie le DECLARE** (`CANNOT`), avec la raison, et
  la page n'est pas ecrite. Fabriquer la page en sortant de l'idiome — un `<html lang>` brut
  colle dans une page Next App Router — testerait du code que personne n'ecrit, et masquerait le
  vrai renseignement : sur cette stack, le correcteur ne rencontrera jamais cette famille.
* **Rien n'est deduit de la documentation d'un generateur.** Ce fichier ecrit des fichiers ; ce
  qui compte est ce que l'hote sert ensuite, mesure au crawl. Les `CANNOT` ci-dessous sont des
  proprietes de l'architecture des gabarits (un fichier partage par tout le site), pas des
  suppositions sur un moteur de rendu.
"""

from __future__ import annotations

import io
import os
from pathlib import Path

try:
    from ops.gauntlet.catalogue import CATALOGUE, Spec
except ImportError:  # lance directement depuis le dossier
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from ops.gauntlet.catalogue import CATALOGUE, Spec

FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures"

# L'URL publique de chaque stack. Elle sert a batir canonical, og:url et hreflang : ces valeurs
# doivent etre ABSOLUES et pointer sur l'hote reel, sinon les familles canonical ne se
# declenchent pas du tout.
SITES: dict[str, str] = {
    "static-html": "https://noyaru-stack-static-html.netlify.app",
    "astro": "https://noyaru-stack-astro.netlify.app",
    "hugo": "https://noyaru-stack-hugo.netlify.app",
    "jekyll": "https://noyaru-stack-jekyll.netlify.app",
    "gatsby": "https://noyaru-stack-gatsby.netlify.app",
    "nuxt": "https://noyaru-stack-nuxt.netlify.app",
    "sveltekit": "https://noyaru-stack-sveltekit.netlify.app",
    "next-pages": "https://noyaru-stack-next-pages.netlify.app",
    "next-app": "https://noyaru-stack-next-app.netlify.app",
}

_VIEWPORT_SHELL = (
    "MESURE sur le site deploye : la page servie porte UNE balise viewport alors qu'elle n'en "
    "declare aucune. Le generateur l'injecte depuis sa coquille partagee, et rien dans l'API "
    "d'une page ne l'enleve — la famille ne peut donc pas s'y produire.")

_HEAD_API_DEDUPES = (
    "MESURE sur le site deploye : la page servie n'en porte qu'UNE alors qu'elle en declare "
    "deux. L'API de tete de cette stack est un dictionnaire, pas une liste de balises : la "
    "seconde valeur ecrase la premiere au lieu de s'y ajouter.")

# Ce qu'une stack ne peut pas exprimer PAR PAGE, et pourquoi. La raison est toujours
# architecturale : le fragment concerne vit dans un fichier partage par tout le site, donc le
# modifier depuis une page changerait les autres — ce que le correcteur refuse a juste titre
# (`_PER_PAGE_ONLY_KEYS`, `repo_index.is_shared_path`) — ou bien l'API de tete deduplique.
CANNOT: dict[str, dict[str, str]] = {
    "next-app": {
        "html_lang_attribute_missing":
            "<html lang> n'existe que dans app/layout.tsx, racine partagee ; un layout imbrique "
            "ne rend pas <html>.",
        "html_lang_attribute_invalid": "meme raison : la racine est partagee.",
        "hreflang_defined_but_html_lang_missing": "idem, le lang vient de la racine partagee.",
        "multiple_title_tags":
            "l'API metadata emet un <title> et un seul ; deux titres demanderaient de sortir de "
            "l'idiome que le correcteur doit apprendre.",
        "multiple_meta_description_tags":
            "meme raison : metadata.description est une cle unique d'un objet.",
        "viewport_not_set":
            "MESURE, pas deduit : `next build` sur cette fixture emet "
            "`width=device-width, initial-scale=1` sur TOUTES les pages, y compris celle qui "
            "exporte `viewport = {}`. Next.js pose sa viewport par defaut et rien dans l'API "
            "d'une page ne l'enleve — la famille ne peut donc pas se produire sur cette stack.",
    },
    "next-pages": {
        "html_lang_attribute_missing":
            "<Html lang> vit dans pages/_document.js, partage par tout le site.",
        "html_lang_attribute_invalid": "meme raison.",
        "hreflang_defined_but_html_lang_missing": "meme raison.",
        "viewport_not_set": _VIEWPORT_SHELL,
        "multiple_title_tags": _HEAD_API_DEDUPES,
        "multiple_meta_description_tags": _HEAD_API_DEDUPES,
    },
    "sveltekit": {
        "html_lang_attribute_missing":
            "<html lang> est dans src/app.html, coquille partagee ; <svelte:head> n'injecte que "
            "dans <head>.",
        "html_lang_attribute_invalid": "meme raison.",
        "hreflang_defined_but_html_lang_missing": "meme raison.",
        "viewport_not_set": _VIEWPORT_SHELL,
        "multiple_title_tags": _HEAD_API_DEDUPES,
    },
    "gatsby": {
        "html_lang_attribute_missing":
            "les attributs de <html> passent par onRenderBody dans gatsby-ssr.js, global au site.",
        "html_lang_attribute_invalid": "meme raison.",
        "hreflang_defined_but_html_lang_missing": "meme raison.",
        "viewport_not_set": _VIEWPORT_SHELL,
    },
    "nuxt": {
        "viewport_not_set": _VIEWPORT_SHELL,
        "multiple_title_tags": _HEAD_API_DEDUPES,
        "multiple_meta_description_tags": _HEAD_API_DEDUPES,
    },
}

# Les deux pieces communes a toutes les stacks.
OG_IMAGE = "/og.png"
BODY_TEXT = ("Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer "
             "UNE anomalie et une seule.")


def url_for(site: str, slug: str) -> str:
    return f"{site}/gauntlet/{slug}"


def _target(site: str, value: str) -> str:
    """Resoudre une cible de spec : slug du parcours, URL absolue, ou variante http forcee."""
    if value.startswith("!http:"):
        return url_for(site, value[len("!http:"):]).replace("https://", "http://", 1)
    if value.startswith("http"):
        return value
    return url_for(site, value)


def canonical_of(spec: Spec, site: str) -> str:
    """'' = la page n'en declare pas ; sinon la valeur absolue a ecrire."""
    if spec.canonical == "":
        return ""
    if spec.canonical is None:
        return url_for(site, spec.slug)
    return _target(site, spec.canonical)


# ── Les balises de <head>, decrites une fois pour toutes ───────────────────────────────────
# Chaque entree est ('title'|'meta'|'link'|'script', attributs). Les emetteurs les rendent
# ensuite dans leur syntaxe ; aucun ne recopie la liste.

def head_tags(spec: Spec, site: str) -> list[tuple[str, dict]]:
    url = url_for(site, spec.slug)
    can = canonical_of(spec, site)
    title = spec.title
    desc = spec.desc
    tags: list[tuple[str, dict]] = []

    if spec.viewport:
        tags.append(("meta", {"name": "viewport", "content": "width=device-width"}))
    if title is not None:
        tags.append(("title", {"text": title}))
    if spec.second_title:
        tags.append(("title", {"text": "Un second titre"}))
    if desc is not None:
        tags.append(("meta", {"name": "description", "content": desc}))
    if spec.second_desc:
        tags.append(("meta", {"name": "description", "content": "Une seconde description."}))
    if spec.robots:
        tags.append(("meta", {"name": "robots", "content": spec.robots}))
    if can:
        tags.append(("link", {"rel": "canonical", "href": can}))
    for code, target in spec.hreflang:
        tags.append(("link", {"rel": "alternate", "hreflang": code,
                              "href": _target(site, target)}))

    # Une valeur ABSENTE ne laisse pas une balise sociale VIDE derriere elle. Mesure sur la
    # verification des neuf stacks : la page sans description portait `og:description content=""`,
    # le correcteur a rempli cette balise-la et considere le travail fait — la meta description,
    # elle, manquait toujours. Une page reelle sans description n'a pas d'Open Graph vide : la
    # fixture fabriquait une situation qui n'existe pas, et le correcteur s'y est laisse prendre.
    if spec.og == "full":
        tags.append(("meta", {"property": "og:type", "content": "article"}))
        if title is not None:
            tags.append(("meta", {"property": "og:title", "content": title}))
        if desc is not None:
            tags.append(("meta", {"property": "og:description", "content": desc}))
        tags += [
            ("meta", {"property": "og:url", "content": url}),
            ("meta", {"property": "og:image", "content": site + OG_IMAGE}),
        ]
    elif spec.og == "partial":
        tags.append(("meta", {"property": "og:title", "content": "Open Graph incomplet"}))

    if spec.tw == "full":
        tags.append(("meta", {"name": "twitter:card", "content": "summary_large_image"}))
        if title is not None:
            tags.append(("meta", {"name": "twitter:title", "content": title}))
        if desc is not None:
            tags.append(("meta", {"name": "twitter:description", "content": desc}))
        tags.append(("meta", {"name": "twitter:image", "content": site + OG_IMAGE}))
    elif spec.tw == "partial":
        tags.append(("meta", {"name": "twitter:card", "content": "summary"}))

    if spec.http_css:
        tags.append(("link", {"rel": "stylesheet",
                              "href": site.replace("https://", "http://") + "/style.css"}))
    if spec.redirected_css:
        # `/ancienne.css` redirige vers `/style.css` — la regle est posee par l'echafaudage.
        tags.append(("link", {"rel": "stylesheet", "href": site + "/ancienne.css"}))
    if spec.jsonld_bad_price:
        # `SoftwareApplication` et pas `Product` : MESURE sur le banc deploye — le
        # controle `offer_price_is_string` du crawler est conditionne a ce type-la, donc
        # un Offer au prix en chaine sur un Product ne declenche RIEN. La fixture doit
        # porter ce que le crawler sait voir, sinon elle mesure un silence.
        tags.append(("script", {
            "type": "application/ld+json",
            "text": ('{"@context":"https://schema.org","@type":"SoftwareApplication","name":"Application de '
                     'test","offers":{"@type":"Offer","price":"0","priceCurrency":"EUR"}}'),
        }))
    return tags


def body_bits(spec: Spec, site: str) -> list[str]:
    """Le corps, en HTML : il est identique sur les neuf stacks (JSX mis a part, gere plus bas)."""
    http = site.replace("https://", "http://")
    out: list[str] = []
    if spec.h1 >= 1:
        out.append("<h1>Parcours d'obstacles</h1>")
    if spec.h1 >= 2:
        out.append("<h1>Un second titre de niveau 1</h1>")
    out.append(f"<p>{BODY_TEXT}</p>")
    out.append('<p><a href="/">Retour a l accueil</a></p>')
    if spec.http_image:
        out.append(f'<img src="{http}/og.png" alt="Illustration de test" />')
    if spec.img_no_alt:
        # Pas d'attribut alt du tout : c'est l'anomalie visee. L'image doit EXISTER, sinon on
        # mesurerait un 404 au lieu d'un alt manquant.
        out.append(f'<img src="{site}/og.png" />')
    if spec.redirected_image:
        out.append(f'<img src="{site}/img/ancienne.png" alt="Illustration de test" />')
    if spec.http_link:
        out.append(f'<p><a href="{http}/{spec.http_link}">A propos</a></p>')
    if spec.double_slash_link:
        host = site.split("//", 1)[1]
        out.append(f'<p><a href="//{host}//{spec.double_slash_link}">A propos</a></p>')
    if spec.link_to_redirect:
        out.append('<p><a href="/gauntlet/ancienne-page">Ancienne page</a></p>')
    if spec.http_js:
        out.append(f'<script src="{http}/app.js"></script>')
    if spec.redirected_js:
        out.append(f'<script src="{site}/ancien.js"></script>')
    return out


# ── Rendus ─────────────────────────────────────────────────────────────────────────────────

def _attrs(d: dict) -> str:
    return " ".join(f'{k}="{v}"' for k, v in d.items() if k != "text")


def render_html_tags(tags: list[tuple[str, dict]], indent: str = "    ") -> str:
    lines = []
    for kind, at in tags:
        if kind == "comment":
            # Le commentaire nomme la famille visee, pour qu'un lecteur du depot distingue une
            # fixture d'une erreur. Les chevrons y sont NEUTRALISES : plusieurs notes du
            # catalogue parlent de `<html>` ou `<h1>`, et une balise ecrite dans un commentaire
            # est exactement ce qui avait casse le garde-fou de plafond le 09/09. Le banc n'a pas
            # a re-tendre ce piege a chaque page qu'il genere.
            text = at["text"].replace("<", "«").replace(">", "»")
            lines.append(f"{indent}<!-- FAMILLE VISEE : {text} -->")
        elif kind == "title":
            lines.append(f"{indent}<title>{at['text']}</title>")
        elif kind == "script":
            lines.append(f"{indent}<script type=\"{at['type']}\">{at['text']}</script>")
        else:
            lines.append(f"{indent}<{kind} {_attrs(at)} />")
    return "\n".join(lines)


def render_jsx_tags(tags: list[tuple[str, dict]], indent: str = "        ") -> str:
    """Le meme <head>, en JSX : `<title>` porte son texte en enfant, le JSON-LD passe par
    dangerouslySetInnerHTML — c'est l'idiome, et l'ecrire autrement casse le build."""
    lines = []
    for kind, at in tags:
        if kind == "title":
            lines.append(f"{indent}<title>{at['text']}</title>")
        elif kind == "script":
            payload = at["text"].replace("\\", "\\\\").replace("`", "\\`")
            lines.append(f'{indent}<script type="{at["type"]}" '
                         f"dangerouslySetInnerHTML={{{{ __html: `{payload}` }}}} />")
        else:
            lines.append(f"{indent}<{kind} {_attrs(at)} />")
    return "\n".join(lines)


def header_comment(spec: Spec, marker: str = "<!--", close: str = "-->") -> str:
    return (f"{marker} FAMILLE VISEE : {spec.family}\n     {spec.note}\n"
            f"     Page generee par ops/gauntlet/emit_stacks.py — c'est une fixture, pas une "
            f"erreur. {close}")


def emit_static_html(spec: Spec, site: str) -> tuple[str, str]:
    lang = "" if spec.lang is None else f' lang="{spec.lang}"'
    head = ('    <meta charset="utf-8" />\n' + render_html_tags(head_tags(spec, site)))
    body = "\n".join("    " + b for b in body_bits(spec, site))
    doc = (f"<!doctype html>\n<html{lang}>\n  <head>\n"
           f"    {header_comment(spec)}\n{head}\n  </head>\n  <body>\n{body}\n  </body>\n</html>\n")
    return f"gauntlet/{spec.slug}.html", doc


def _html_attrs(spec: Spec) -> str:
    """L'attribut lang tel qu'il doit apparaitre sur <html>, vide quand la page n'en declare pas."""
    return "" if spec.lang is None else f' lang="{spec.lang}"'


def emit_jekyll(spec: Spec, site: str) -> tuple[str, str]:
    """Jekyll : tout en front matter YAML, rendu par le gabarit `gauntlet`.

    Le gabarit `default` du site code son <head> en dur — realiste pour un site, mais il ne peut
    donc porter ni page sans titre ni page sans viewport. Le parcours a le sien, qui n'imprime
    que ce que la page lui donne : la page reste la SEULE source de ses valeurs.

    Le <head> et le corps passent en scalaires de bloc (`|`) plutot que dans le contenu de la
    page, pour que le gabarit n'ait rien a decouper.
    """
    head = render_html_tags([("comment", {"text": spec.family + " — " + spec.note})]
                            + head_tags(spec, site), indent="  ")
    body = "\n".join("  " + b for b in body_bits(spec, site))
    doc = ("---\n"
           "layout: gauntlet\n"
           f"permalink: /gauntlet/{spec.slug}\n"
           f"html_attrs: '{_html_attrs(spec)}'\n"
           f"raw_head: |\n{head}\n"
           f"raw_body: |\n{body}\n"
           "---\n")
    return f"gauntlet/{spec.slug}.html", doc


def emit_hugo(spec: Spec, site: str) -> tuple[str, str]:
    """Hugo : front matter TOML, en chaines litterales (`'''`) qui n'interpretent aucun echappement.

    `baseof.html` applique une branche dediee des que `raw_head` est renseigne — Hugo enveloppe
    TOUJOURS une page single avec le gabarit de base, donc un layout separe aurait ete enveloppe
    lui aussi.
    """
    head = render_html_tags([("comment", {"text": spec.family + " — " + spec.note})]
                            + head_tags(spec, site), indent="  ")
    body = "\n".join("  " + b for b in body_bits(spec, site))
    doc = ("+++\n"
           f'url = "/gauntlet/{spec.slug}"\n'
           f"title = '{(spec.title or 'Parcours').replace(chr(39), chr(32))}'\n"
           f"html_attrs = '{_html_attrs(spec)}'\n"
           f"raw_head = '''\n{head}\n'''\n"
           f"raw_body = '''\n{body}\n'''\n"
           "+++\n")
    return f"content/gauntlet/{spec.slug}.md", doc


def emit_astro(spec: Spec, site: str) -> tuple[str, str]:
    """Astro : la page du parcours emet son document entier plutot que d'appeler Base.astro.

    Base.astro est le gabarit PARTAGE ; lui ajouter dix props pour les besoins du banc changerait
    le fichier que le correcteur ne doit justement jamais toucher.
    """
    lang = "" if spec.lang is None else f' lang="{spec.lang}"'
    head = render_html_tags(head_tags(spec, site))
    # `is:inline` est OBLIGATOIRE ici. Sans lui, Astro TRAITE le script : mesure sur le site
    # deploye, `<script src="http://.../app.js">` ressortait en
    # `<script type="module" src="/_astro/….js">` — l'URL http avait disparu et la famille ne
    # pouvait plus se declencher. `is:inline` est l'idiome Astro pour un script laisse tel quel.
    body = "\n".join(
        "    " + (b.replace("<script ", "<script is:inline ", 1) if b.startswith("<script") else b)
        for b in body_bits(spec, site))
    doc = (f"---\n// FAMILLE VISEE : {spec.family}\n// {spec.note}\n---\n"
           f"<!doctype html>\n<html{lang}>\n  <head>\n"
           f'    <meta charset="utf-8" />\n{head}\n  </head>\n'
           f"  <body>\n{body}\n  </body>\n</html>\n")
    return f"src/pages/gauntlet/{spec.slug}.astro", doc


def emit_sveltekit(spec: Spec, site: str) -> tuple[str, str]:
    head = render_html_tags(head_tags(spec, site), indent="  ")
    # Svelte traite `<script>` comme un bloc du composant, pas comme du balisage : un
    # `<script src>` ecrit tel quel dans le template fait ECHOUER la compilation (mesure — le
    # build de la stack est tombe dessus). `{@html}` le rend a la serialisation, donc l'hote
    # sert bien la balise que le crawler doit voir.
    lines = []
    for bit in body_bits(spec, site):
        if bit.startswith("<script"):
            lines.append("{@html " + repr(bit) + "}")
        else:
            lines.append(bit)
    body = "\n".join(lines)
    doc = (f"<script>\n  // FAMILLE VISEE : {spec.family}\n  // {spec.note}\n</script>\n\n"
           f"<svelte:head>\n{head}\n</svelte:head>\n\n{body}\n")
    return f"src/routes/gauntlet/{spec.slug}/+page.svelte", doc


def emit_next_pages(spec: Spec, site: str) -> tuple[str, str]:
    head = render_jsx_tags(head_tags(spec, site), indent="        ")
    body = "\n".join("      " + b for b in body_bits(spec, site))
    doc = ("import Head from 'next/head';\n\n"
           f"// FAMILLE VISEE : {spec.family}\n// {spec.note}\n\n"
           "export default function Page() {\n  return (\n    <main>\n"
           f"      <Head>\n{head}\n      </Head>\n{body}\n    </main>\n  );\n}}\n")
    return f"pages/gauntlet/{spec.slug}.js", doc


def emit_gatsby(spec: Spec, site: str) -> tuple[str, str]:
    head = render_jsx_tags(head_tags(spec, site), indent="      ")
    body = "\n".join("      " + b for b in body_bits(spec, site))
    doc = ("import * as React from 'react';\n\n"
           f"// FAMILLE VISEE : {spec.family}\n// {spec.note}\n\n"
           "export default function Page() {\n  return (\n    <main>\n"
           f"{body}\n    </main>\n  );\n}}\n\n"
           "// Gatsby Head API — l'idiome de cette stack. `next/head` n'existe pas ici.\n"
           f"export function Head() {{\n  return (\n    <>\n{head}\n    </>\n  );\n}}\n")
    return f"src/pages/gauntlet/{spec.slug}.js", doc


def _js_str(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def emit_nuxt(spec: Spec, site: str) -> tuple[str, str]:
    """Nuxt : useHead(), la seule stack ou l'attribut lang se pilote PAR PAGE (`htmlAttrs`)."""
    tags = head_tags(spec, site)
    metas, links, title_txt, scripts = [], [], None, []
    for kind, at in tags:
        if kind == "title" and title_txt is None:
            title_txt = at["text"]
        elif kind == "meta":
            metas.append("      { " + ", ".join(
                f"{k}: {_js_str(v)}" for k, v in at.items()) + " }")
        elif kind == "link":
            links.append("      { " + ", ".join(
                f"{k}: {_js_str(v)}" for k, v in at.items()) + " }")
        elif kind == "script":
            scripts.append("      { type: " + _js_str(at["type"]) +
                           ", innerHTML: " + _js_str(at["text"]) + " }")
    head_obj = []
    if title_txt is not None:
        head_obj.append(f"    title: {_js_str(title_txt)},")
    if spec.lang is None:
        # `htmlAttrs: {}` ne suffit PAS : nuxt.config.ts pose `app.head.htmlAttrs.lang = 'fr'`
        # comme defaut global, et un objet vide se contente de fusionner par-dessus sans rien
        # retirer. Il faut ecraser la cle explicitement pour que l'attribut disparaisse.
        head_obj.append("    htmlAttrs: { lang: null },  // langue retiree : l'anomalie visee")
    else:
        head_obj.append(f"    htmlAttrs: {{ lang: {_js_str(spec.lang)} }},")
    if metas:
        head_obj.append("    meta: [\n" + ",\n".join(metas) + "\n    ],")
    if links:
        head_obj.append("    link: [\n" + ",\n".join(links) + "\n    ],")
    if spec.redirected_js:
        scripts.append("      { src: " + _js_str(site + "/ancien.js") + " }")
    if spec.http_js:
        # Le script passe par useHead, PAS par le template. Mesure sur le site deploye : ecrit
        # dans le template, il figurait bien dans le HTML servi mais avait disparu du DOM une
        # fois Vue hydrate — et c'est le DOM rendu que le crawler mesure. useHead est de toute
        # facon l'idiome Nuxt pour une balise de tete, donc c'est ce que le correcteur verra.
        scripts.append("      { src: "
                       + _js_str(site.replace("https://", "http://") + "/app.js") + " }")
    if scripts:
        head_obj.append("    script: [\n" + ",\n".join(scripts) + "\n    ],")
    body = "\n".join("    " + b for b in body_bits(spec, site)
                     if not b.startswith("<script"))
    doc = ("<script setup>\n"
           f"// FAMILLE VISEE : {spec.family}\n// {spec.note}\n"
           "useHead({\n" + "\n".join(head_obj) + "\n});\n</script>\n\n"
           f"<template>\n  <main>\n{body}\n  </main>\n</template>\n")
    return f"pages/gauntlet/{spec.slug}.vue", doc


def emit_next_app(spec: Spec, site: str) -> tuple[str, str]:
    """Next App Router : un objet `metadata` exporte — l'ecriture qui a casse le garde-fou.

    C'est la seule stack ou les valeurs de page sont des PROPRIETES D'OBJET dans un fichier
    TypeScript. Le parcours ne la portait pas, et c'est la que le defaut du 09/09 vivait.
    """
    url = url_for(site, spec.slug)
    can = canonical_of(spec, site)
    lines = ["export const metadata = {"]
    if spec.title is not None:
        lines.append(f"  title: {_js_str(spec.title)},")
    if spec.desc is not None:
        lines.append(f"  description: {_js_str(spec.desc)},")
    if spec.robots:
        lines.append(f"  robots: {_js_str(spec.robots)},")
    alt: list[str] = []
    if can:
        alt.append(f"    canonical: {_js_str(can)},")
    if spec.hreflang:
        langs = ", ".join(f"{_js_str(c)}: {_js_str(_target(site, t))}" for c, t in spec.hreflang)
        alt.append("    languages: { " + langs + " },")
    if alt:
        lines.append("  alternates: {\n" + "\n".join(alt) + "\n  },")
    if spec.og == "full":
        # Meme regle que dans `head_tags` : pas de valeur sociale VIDE derriere une valeur
        # absente, sinon le correcteur remplit la copie et laisse l'original manquant.
        lines += ["  openGraph: {", "    type: 'article',"]
        if spec.title is not None:
            lines.append(f"    title: {_js_str(spec.title)},")
        if spec.desc is not None:
            lines.append(f"    description: {_js_str(spec.desc)},")
        lines += [f"    url: {_js_str(url)},",
                  f"    images: [{_js_str(site + OG_IMAGE)}],", "  },"]
    elif spec.og == "partial":
        lines += ["  openGraph: { title: 'Open Graph incomplet' },"]
    if spec.tw == "full":
        lines += ["  twitter: {", "    card: 'summary_large_image',"]
        if spec.title is not None:
            lines.append(f"    title: {_js_str(spec.title)},")
        if spec.desc is not None:
            lines.append(f"    description: {_js_str(spec.desc)},")
        lines += [f"    images: [{_js_str(site + OG_IMAGE)}],", "  },"]
    elif spec.tw == "partial":
        lines += ["  twitter: { card: 'summary' },"]
    lines.append("};")
    viewport = ("" if spec.viewport else
                "\n// Aucune viewport exportee : c'est l'anomalie visee sur cette page.\n"
                "export const viewport = {};\n")
    body = "\n".join("      " + b for b in body_bits(spec, site))
    extra = ""
    if spec.redirected_css:
        extra += '\n      <link rel="stylesheet" href="' + site + '/ancienne.css" />'
    if spec.http_css:
        # L'objet `metadata` ne sait pas declarer un <link rel="stylesheet"> : il n'a de cle que
        # pour les balises qu'il connait. Mesure : `https_page_links_to_http_css` etait la SEULE
        # famille manquante sur cette stack, parce que cet emetteur ne passe pas par
        # `head_tags`. Next remonte dans <head> un <link> rendu par la page.
        extra += ('\n      <link rel="stylesheet" href="'
                  + site.replace("https://", "http://") + '/style.css" />')
    if spec.jsonld_bad_price:
        extra += ('\n      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: '
                 '`{"@context":"https://schema.org","@type":"SoftwareApplication","name":"Application de test",'
                 '"offers":{"@type":"Offer","price":"0","priceCurrency":"EUR"}}` }} />')
    doc = (f"// FAMILLE VISEE : {spec.family}\n// {spec.note}\n"
           + "\n".join(lines) + "\n" + viewport +
           f"\nexport default function Page() {{\n  return (\n    <main>\n{body}{extra}\n"
           "    </main>\n  );\n}\n")
    return f"app/gauntlet/{spec.slug}/page.tsx", doc


EMITTERS = {
    "static-html": emit_static_html,
    "jekyll": emit_jekyll,
    "hugo": emit_hugo,
    "astro": emit_astro,
    "sveltekit": emit_sveltekit,
    "next-pages": emit_next_pages,
    "gatsby": emit_gatsby,
    "nuxt": emit_nuxt,
    "next-app": emit_next_app,
}


def skipped(stack: str, spec: Spec) -> str:
    """La raison, si cette stack ne peut pas porter cette anomalie par page. '' sinon."""
    table = CANNOT.get(stack, {})
    for name in spec.family.split("+"):
        reason = table.get(name.strip())
        if reason:
            return reason
    return ""


def build(stack: str, root: Path | None = None) -> tuple[list[str], list[tuple[str, str]]]:
    site = SITES[stack]
    emit = EMITTERS[stack]
    base = Path(root) if root else (FIXTURES / stack)
    written: list[str] = []
    refused: list[tuple[str, str]] = []
    for spec in CATALOGUE:
        reason = skipped(stack, spec)
        if reason:
            refused.append((spec.slug, reason))
            # Un refus doit aussi EFFACER la page ecrite par un passage precedent. Sans cela,
            # une famille declaree impossible continue d'etre deployee et crawlee, et le banc
            # se contredit lui-meme : la page produit l'anomalie que le refus dit inatteignable.
            rel, _ = emit(spec, site)
            stale = base / rel
            # Defensif de bout en bout : sous Windows, OneDrive garde des verrous et un
            # `rmdir` refuse a fait tomber toute la generation au milieu des neuf stacks. Un
            # dossier vide qui survit est sans consequence ; une generation interrompue, non.
            try:
                if stale.exists():
                    stale.unlink()
                # SvelteKit range chaque route dans son propre dossier : le laisser vide ferait
                # un 404 la ou l'index du parcours annonce une page.
                if stale.parent.is_dir() and not any(stale.parent.iterdir()):
                    stale.parent.rmdir()
            except OSError as exc:
                print(f"   (nettoyage impossible pour {rel} : {exc.strerror})")
            continue
        rel, content = emit(spec, site)
        path = base / rel
        os.makedirs(path.parent, exist_ok=True)
        io.open(path, "w", encoding="utf-8", newline="\n").write(content)
        written.append(rel)
    return written, refused


if __name__ == "__main__":
    import sys
    only = sys.argv[1:] or list(EMITTERS)
    total_w = total_r = 0
    for stack in only:
        written, refused = build(stack)
        total_w += len(written)
        total_r += len(refused)
        print(f"{stack:<12} {len(written):>3} pages ecrites, {len(refused)} refusees")
        for slug, reason in refused:
            print(f"             - {slug} : {reason.splitlines()[0]}")
    print(f"\nTOTAL {total_w} pages sur les {len(only)} stacks, {total_r} refus motives")
