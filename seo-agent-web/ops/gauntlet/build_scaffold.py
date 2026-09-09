# -*- coding: utf-8 -*-
"""Scaffolding around the gauntlet: an index so nothing is orphaned, the assets its pages
reference, the redirect one page needs, and the sitemap entries.

Every page in the gauntlet must carry exactly ONE defect. Missing assets or missing incoming
links would add a second one and make the run unreadable.
"""
import base64
import io
import os
import re

ROOT = os.environ["FIXTURE_DIR"]
BASE = "https://noyaru-stack-static-html.netlify.app"
G = os.path.join(ROOT, "gauntlet")
slugs = sorted(f[:-5] for f in os.listdir(G) if f.endswith(".html") and f != "index.html")

# ── an index page, so no gauntlet page is an orphan ───────────────────────────────────────────
items = "\n".join(
    f'      <li><a href="/gauntlet/{s}.html">{s}</a></li>' for s in slugs)
index = f"""<!doctype html>
<html lang="fr">
  <head>
    <!-- Index du parcours d'obstacles. Cette page-ci doit rester IRREPROCHABLE : elle existe
         pour que les pages du parcours ne soient pas orphelines, pas pour porter un defaut. -->
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width" />
    <title>Parcours d'obstacles du correcteur — pages de test</title>
    <meta name="description" content="Index des pages de test du parcours d'obstacles : chacune porte une anomalie et une seule, pour exercer une famille du correcteur." />
    <link rel="canonical" href="{BASE}/gauntlet/" />
    <meta property="og:title" content="Parcours d'obstacles du correcteur" />
    <meta property="og:description" content="Index des pages de test du parcours d'obstacles." />
    <meta property="og:url" content="{BASE}/gauntlet/" />
    <meta property="og:image" content="{BASE}/img/cover.png" />
    <meta property="og:type" content="website" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="Parcours d'obstacles du correcteur" />
    <meta name="twitter:description" content="Index des pages de test du parcours d'obstacles." />
    <meta name="twitter:image" content="{BASE}/img/cover.png" />
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
io.open(os.path.join(G, "index.html"), "w", encoding="utf-8", newline="\n").write(index)

# ── the assets the pages reference (absent files would add a second defect each) ───────────────
os.makedirs(os.path.join(ROOT, "img"), exist_ok=True)
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")
io.open(os.path.join(ROOT, "img", "cover.png"), "wb").write(PNG)
io.open(os.path.join(ROOT, "style.css"), "w", encoding="utf-8", newline="\n").write(
    "/* Feuille de style minimale : elle existe pour que les pages du parcours qui la chargent\n"
    "   en http ne portent pas AUSSI une ressource introuvable. */\nbody { font-family: system-ui; }\n")
io.open(os.path.join(ROOT, "app.js"), "w", encoding="utf-8", newline="\n").write(
    "// Script minimal, meme raison que style.css : une seule anomalie par page.\n")

# ── the redirect that `link-to-redirect` points at ────────────────────────────────────────────
io.open(os.path.join(ROOT, "_redirects"), "w", encoding="utf-8", newline="\n").write(
    "# La page /gauntlet/ancienne-page.html n'existe pas : elle redirige, pour que\n"
    "# gauntlet/link-to-redirect.html porte bien la famille page_has_links_to_redirect.\n"
    "/gauntlet/ancienne-page.html   /gauntlet/index.html   301\n")

# ── sitemap: the gauntlet pages must be crawled ───────────────────────────────────────────────
sm_path = os.path.join(ROOT, "sitemap.xml")
sm = io.open(sm_path, encoding="utf-8").read()
extra = "\n".join(f"  <url><loc>{BASE}/gauntlet/{s}.html</loc></url>" for s in slugs)
extra = f"  <url><loc>{BASE}/gauntlet/</loc></url>\n" + extra
assert "</urlset>" in sm
sm = sm.replace("</urlset>", extra + "\n</urlset>")
io.open(sm_path, "w", encoding="utf-8", newline="\n").write(sm)

# ── link the index from the home page, so it is not an orphan either ──────────────────────────
idx_path = os.path.join(ROOT, "index.html")
home = io.open(idx_path, encoding="utf-8").read()
if "/gauntlet/" not in home:
    home = re.sub(r"(<a href=\"/a-propos\"[^<]*</a>)",
                  r'\1\n    <a href="/gauntlet/">Parcours d obstacles</a>', home, count=1)
    io.open(idx_path, "w", encoding="utf-8", newline="\n").write(home)

print(f"index + {len(slugs)} entrees de sitemap, assets, redirection")
print("home lie le parcours :", "/gauntlet/" in io.open(idx_path, encoding="utf-8").read())
