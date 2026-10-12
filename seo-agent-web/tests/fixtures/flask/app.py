"""Site de test Flask : des gabarits Jinja, choisis par des routes Python.

Dixieme stack du banc (12/10/2026). Ce que les neuf autres ne portent pas, et que le correcteur
a rate sur noyaru.com : une URL ne se lit pas dans un chemin de fichier, c'est une ROUTE qui
choisit son gabarit, et les gabarits heritent les uns des autres (`{% extends %}`).
Netlify ne sert que du statique : `freeze.py` gele le site en HTML a la construction.
"""
from pathlib import Path

from flask import Flask, abort, render_template

app = Flask(__name__)
GAUNTLET = Path(app.root_path) / "templates" / "gauntlet"


@app.route("/")
def accueil():
    return render_template("index.html")


@app.route("/a-propos")
def a_propos():
    return render_template("a_propos.html")


@app.route("/blog")
def blog():
    return render_template("blog.html")


@app.route("/gauntlet/<slug>")
def parcours(slug):
    # Une page du parcours par gabarit ; `_layout.html` n'est pas une page.
    if slug.startswith("_") or not (GAUNTLET / f"{slug}.html").is_file():
        abort(404)
    return render_template(f"gauntlet/{slug}.html")


if __name__ == "__main__":
    app.run(debug=True)
