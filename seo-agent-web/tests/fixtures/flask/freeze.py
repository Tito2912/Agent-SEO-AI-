"""Geler le site en HTML pour Netlify, qui ne sert que du statique.

1. Frozen-Flask rend chaque route dans `build/` — les pages du parcours via le generateur.
2. Une route sans extension (`/a-propos`) est ecrite en fichier SANS extension, que Netlify
   servirait en binaire. On la renomme en `a-propos.html` : Netlify sert alors `/a-propos` depuis
   ce fichier (« pretty URLs »), exactement comme le site en HTML pur.
3. `public/` est copie TEL QUEL a la racine : assets, `_redirects`, `robots.txt`, sitemap et
   l'index du parcours — la ou chaque autre stack pose les siens.
"""
import shutil
from pathlib import Path

from flask_frozen import Freezer

from app import GAUNTLET, app

ROOT = Path(app.root_path)
BUILD = ROOT / "build"
app.config.update(FREEZER_DESTINATION=str(BUILD), FREEZER_IGNORE_MIMETYPE_WARNINGS=True)
freezer = Freezer(app)


@freezer.register_generator
def parcours():
    for page in sorted(GAUNTLET.glob("*.html")):
        if not page.stem.startswith("_"):
            yield {"slug": page.stem}


if __name__ == "__main__":
    freezer.freeze()
    for page in [p for p in BUILD.rglob("*") if p.is_file() and not p.suffix]:
        page.rename(page.with_name(page.name + ".html"))
    shutil.copytree(ROOT / "public", BUILD, dirs_exist_ok=True)
    print("site gele :", sum(1 for p in BUILD.rglob("*.html")), "pages HTML dans", BUILD)
