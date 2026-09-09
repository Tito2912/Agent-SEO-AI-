# -*- coding: utf-8 -*-
"""L'echafaudage autour du parcours, pour chacune des neuf stacks.

Une page du parcours doit porter UNE anomalie et une seule. Sans cet echafaudage elle en
porterait trois de plus, dont aucune n'est celle qu'on veut mesurer :

* **orpheline** — rien ne lie `/gauntlet/<slug>`, donc `orphan_page` et
  `page_has_only_one_dofollow_incoming_internal_link` s'ajoutent a chaque page ;
* **ressource introuvable** — les pages qui chargent une image, une CSS ou un JS en http
  doivent les trouver, sinon c'est un 404 qu'on mesure et pas du contenu mixte ;
* **redirection absente** — `link-to-redirect` a besoin qu'une URL redirige vraiment.

Chaque stack a son dossier statique et son sitemap ; c'est la seule chose qui change ici.
"""

from __future__ import annotations

import base64
import io
import os
import re
from pathlib import Path

try:
    from ops.gauntlet.catalogue import CATALOGUE
    from ops.gauntlet.emit_stacks import FIXTURES, SITES, skipped
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from ops.gauntlet.catalogue import CATALOGUE
    from ops.gauntlet.emit_stacks import FIXTURES, SITES, skipped

# Le dossier dont le contenu est copie TEL QUEL a la racine du site publie. C'est la que
# doivent atterrir les assets, `_redirects` et le sitemap — jamais dans la sortie de build,
# qui n'est pas versionnee (voir `Stack.ignore` dans stack_loop.py).
STATIC_DIR: dict[str, str] = {
    "static-html": ".",
    "jekyll": ".",
    "astro": "public",
    "nuxt": "public",
    "next-pages": "public",
    "next-app": "public",
    "hugo": "static",
    "gatsby": "static",
    "sveltekit": "static",
}

# Un PNG 1x1 valide : les pages qui referencent une image doivent en trouver une.
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")

INDEX_TITLE = "Parcours d'obstacles du correcteur — pages de test"
INDEX_DESC = ("Index des pages de test du parcours d'obstacles : chacune porte une anomalie et "
              "une seule, pour exercer une famille du correcteur.")


def slugs_for(stack: str) -> list[str]:
    """Les pages reellement ecrites pour cette stack — les refus n'ont pas de page a lier."""
    return [s.slug for s in CATALOGUE if not skipped(stack, s)]


def index_path(stack: str) -> str:
    """L'URL a laquelle l'index du parcours est REELLEMENT servi.

    Mesure sur les neuf sites deployes, pas deduite : partout `/gauntlet/`, sauf SvelteKit. La,
    l'index est une ROUTE et non un fichier statique, et `trailingSlash: 'never'` — le defaut du
    framework — la sert a `/gauntlet` en faisant 301 sur `/gauntlet/`.

    Se tromper de forme coute deux anomalies parasites, sur les deux pages qui doivent rester
    irreprochables : un lien vers une redirection sur l'ACCUEIL, et un canonical vers une
    redirection sur l'INDEX lui-meme.
    """
    return "/gauntlet" if stack == "sveltekit" else "/gauntlet/"


def index_html(stack: str, suffix: str) -> tuple[str, str]:
    """L'index du parcours, dans le seul idiome dont il a besoin : du HTML statique.

    Il est servi depuis le dossier statique de chaque stack, donc il echappe au generateur. Ce
    n'est pas de la triche : cette page doit rester IRREPROCHABLE — elle existe pour que les
    autres ne soient pas orphelines, pas pour porter un defaut — et la faire passer par le
    generateur l'exposerait aux anomalies que le parcours injecte justement.
    """
    site = SITES[stack]
    here = site + index_path(stack)
    items = "\n".join(
        f'      <li><a href="/gauntlet/{s}{suffix}">{s}</a></li>' for s in slugs_for(stack))
    doc = f"""<!doctype html>
<html lang="fr">
  <head>
    <!-- Index du parcours d'obstacles. Cette page doit rester IRREPROCHABLE : elle existe pour
         que les pages du parcours ne soient pas orphelines, pas pour porter un defaut. -->
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width" />
    <title>{INDEX_TITLE}</title>
    <meta name="description" content="{INDEX_DESC}" />
    <link rel="canonical" href="{here}" />
    <meta property="og:type" content="website" />
    <meta property="og:title" content="{INDEX_TITLE}" />
    <meta property="og:description" content="{INDEX_DESC}" />
    <meta property="og:url" content="{here}" />
    <meta property="og:image" content="{site}/og.png" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{INDEX_TITLE}" />
    <meta name="twitter:description" content="{INDEX_DESC}" />
    <meta name="twitter:image" content="{site}/og.png" />
  </head>
  <body>
    <h1>Parcours d'obstacles</h1>
    <p>Chaque page ci-dessous porte une anomalie et une seule.</p>
    <ul>
{items}
    </ul>
    <p><a href="/">Retour a l accueil</a></p>
  </body>
</html>
"""
    return "gauntlet/index.html", doc


def scaffold(stack: str, root: Path | None = None) -> list[str]:
    base = Path(root) if root else (FIXTURES / stack)
    static = base / STATIC_DIR[stack] if STATIC_DIR[stack] != "." else base
    site = SITES[stack]
    # Hugo, Nuxt et Gatsby servent des index de repertoire : `/gauntlet/<slug>/` est la page et
    # `/gauntlet/<slug>` redirige. Mesure sur les sites deployes, pas deduite des documentations
    # (`Stack.defect_style` dans stack_loop.py porte le meme fait).
    suffix = "/" if stack in {"hugo", "nuxt", "gatsby"} else ""
    done: list[str] = []

    if stack == "sveltekit":
        # SvelteKit PRERENDER suit les liens et refuse un 404 : il resout les fichiers de
        # `static/` par chemin EXACT, donc `/gauntlet/` — un index de repertoire — lui est
        # inconnu et le build echoue (mesure : « Error: 404 /gauntlet/ (linked from /) »).
        # Netlify, lui, le servirait tres bien ; c'est le build qui tombe avant. Sur cette
        # stack seule, l'index est donc une ROUTE. Il reste irreprochable : son <head> est
        # ecrit ici en entier, il ne passe par aucun gabarit qui pourrait lui ajouter un defaut.
        items = "\n".join(
            f'  <li><a href="/gauntlet/{s}{suffix}">{s}</a></li>' for s in slugs_for(stack))
        site_ = SITES[stack]
        route = (f"<svelte:head>\n  <title>{INDEX_TITLE}</title>\n"
                 f'  <meta name="description" content="{INDEX_DESC}" />\n'
                 f'  <link rel="canonical" href="{site_}{index_path(stack)}" />\n'
                 f'  <meta property="og:type" content="website" />\n'
                 f'  <meta property="og:title" content="{INDEX_TITLE}" />\n'
                 f'  <meta property="og:description" content="{INDEX_DESC}" />\n'
                 f'  <meta property="og:url" content="{site_}{index_path(stack)}" />\n'
                 f'  <meta property="og:image" content="{site_}/og.png" />\n'
                 f'  <meta name="twitter:card" content="summary_large_image" />\n'
                 f'  <meta name="twitter:title" content="{INDEX_TITLE}" />\n'
                 f'  <meta name="twitter:description" content="{INDEX_DESC}" />\n'
                 f'  <meta name="twitter:image" content="{site_}/og.png" />\n'
                 "</svelte:head>\n\n<h1>Parcours d'obstacles</h1>\n"
                 "<p>Chaque page ci-dessous porte une anomalie et une seule.</p>\n"
                 f"<ul>\n{items}\n</ul>\n"
                 '<p><a href="/">Retour a l accueil</a></p>\n')
        route_path = base / "src" / "routes" / "gauntlet" / "+page.svelte"
        os.makedirs(route_path.parent, exist_ok=True)
        io.open(route_path, "w", encoding="utf-8", newline="\n").write(route)
        done.append("src/routes/gauntlet/+page.svelte")
    else:
        os.makedirs(static / "gauntlet", exist_ok=True)
        rel, doc = index_html(stack, suffix)
        io.open(static / rel, "w", encoding="utf-8", newline="\n").write(doc)
        done.append(str(Path(STATIC_DIR[stack]) / rel))

    # ── les ressources que les pages referencent ──────────────────────────────────────────
    if not (static / "og.png").exists():
        io.open(static / "og.png", "wb").write(PNG)
        done.append("og.png")
    io.open(static / "style.css", "w", encoding="utf-8", newline="\n").write(
        "/* Feuille minimale : elle existe pour que la page qui la charge en http ne porte pas\n"
        "   AUSSI une ressource introuvable. Une anomalie par page. */\n"
        "body { font-family: system-ui; }\n")
    io.open(static / "app.js", "w", encoding="utf-8", newline="\n").write(
        "// Script minimal, meme raison que style.css : une seule anomalie par page.\n")
    done += ["style.css", "app.js"]

    # next-app etait la seule fixture sans robots.txt — mesure : 404 en ligne, alors que les
    # huit autres en servaient un. Deux familles se declenchent la-dessus
    # (`robots_txt_not_found`, `sitemap_not_in_robots`), donc cette absence aurait compte comme
    # deux anomalies parasites sur cette stack et faussé la comparaison entre les neuf.
    robots = static / "robots.txt"
    if not robots.exists():
        io.open(robots, "w", encoding="utf-8", newline="\n").write(
            f"User-agent: *\nAllow: /\n\nSitemap: {site}/sitemap.xml\n")
        done.append("robots.txt")

    # ── la redirection dont `link-to-redirect` a besoin ───────────────────────────────────
    io.open(static / "_redirects", "w", encoding="utf-8", newline="\n").write(
        "# /gauntlet/ancienne-page n'existe pas : elle redirige, pour que la page\n"
        "# link-to-redirect porte bien la famille page_has_links_to_redirect.\n"
        f"/gauntlet/ancienne-page   {index_path(stack)}   301\n")
    done.append("_redirects")

    # ── le sitemap : sans entree, les pages du parcours ne sont pas crawlees ──────────────
    sm = static / "sitemap.xml"
    if sm.exists():
        text = io.open(sm, encoding="utf-8").read()
        # Pas de barre obligatoire apres `/gauntlet` : sur SvelteKit l'index est `/gauntlet`
        # tout court, et un motif qui l'exigeait ne le retirait pas — l'entree se serait
        # dupliquee a chaque regeneration. Ce nettoyage est ce qui rend l'echafaudage rejouable.
        text = re.sub(r"\n?  <url><loc>[^<]*/gauntlet[^<]*</loc></url>", "", text)
        extra = [f"  <url><loc>{site}{index_path(stack)}</loc></url>"]
        extra += [f"  <url><loc>{site}/gauntlet/{s}{suffix}</loc></url>"
                  for s in slugs_for(stack)]
        if "</urlset>" in text:
            text = text.replace("</urlset>", "\n".join(extra) + "\n</urlset>")
            io.open(sm, "w", encoding="utf-8", newline="\n").write(text)
            done.append("sitemap.xml")

    return done


def link_from_home(stack: str, root: Path | None = None) -> bool:
    """Lier l'index du parcours depuis l'accueil, pour qu'il ne soit pas orphelin lui-meme.

    L'accueil de chaque stack est ecrit dans son propre idiome ; on n'y touche que si le lien
    manque, et on signale quand on n'a pas su le poser plutot que de le supposer fait.
    """
    base = Path(root) if root else (FIXTURES / stack)
    candidates = {
        "static-html": "index.html", "jekyll": "index.html",
        "astro": "src/pages/index.astro", "hugo": "content/_index.md",
        "gatsby": "src/pages/index.js", "nuxt": "pages/index.vue",
        "sveltekit": "src/routes/+page.svelte", "next-pages": "pages/index.js",
        "next-app": "app/page.tsx",
    }
    path = base / candidates[stack]
    if not path.exists():
        return False
    text = io.open(path, encoding="utf-8").read()
    want = index_path(stack)
    if f'"{want}"' in text or f"]({want})" in text:
        return True
    # Un lien vers le parcours existe deja, mais dans l'autre forme. Le laisser tel quel etait
    # le comportement d'avant — et sur SvelteKit cela faisait pointer l'accueil vers une
    # redirection. On CORRIGE au lieu de s'arreter au premier `/gauntlet` rencontre.
    fixed = re.sub(r'(?<=["(])/gauntlet/?(?=["\)])', want, text)
    if fixed != text:
        io.open(path, "w", encoding="utf-8", newline="\n").write(fixed)
        return True
    # La navigation des neuf fixtures vit dans le GABARIT PARTAGE, pas dans l'accueil. Poser le
    # lien la-bas le mettrait sur chaque page du site et changerait le graphe interne de toutes
    # les stacks a la fois ; static-html, lui, ne le porte que sur son accueil. On s'accroche
    # donc au texte du corps de l'accueil, qui est le meme partout a la stack pres.
    anchor = "Site fixture pour prouver"
    if anchor not in text:
        return False
    line = next(ln for ln in text.splitlines() if anchor in ln)
    indent = line[: len(line) - len(line.lstrip())]
    if path.suffix == ".md":  # Hugo : le corps est du markdown
        added = f"\n{indent}[Parcours d obstacles]({index_path(stack)})"
    else:
        added = f'\n{indent}<p><a href="{index_path(stack)}">Parcours d obstacles</a></p>'
    io.open(path, "w", encoding="utf-8", newline="\n").write(
        text.replace(line, line + added, 1))
    return True


if __name__ == "__main__":
    import sys
    only = sys.argv[1:] or list(SITES)
    for stack in only:
        done = scaffold(stack)
        linked = link_from_home(stack)
        print(f"{stack:<12} {len(slugs_for(stack)):>2} pages liees, "
              f"{len(done)} fichiers d'echafaudage, accueil lie : "
              f"{'oui' if linked else 'NON — a poser a la main'}")
