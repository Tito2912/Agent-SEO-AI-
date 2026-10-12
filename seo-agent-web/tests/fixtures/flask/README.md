# Fixture flask — gabarits serveur

Le dixième stack du banc, ajouté le 12/10/2026 après la PR #1 de noyaru.com : le premier site à
**gabarits serveur** que le correcteur ait touché (Jinja servi par FastAPI). Il y avait écrit
`url_for('static', filename=…)` — une syntaxe Flask qui fait planter Starlette —, posé des
balises dans la mise en page EN PLUS des pages qui l'étendent, et visé une fixture de tests.

Ce que ce stack porte, et qu'aucun des neuf autres ne porte :

- **une URL n'est pas un chemin de fichier** : c'est une route Python (`@app.route`) qui choisit
  son gabarit (`render_template`) ;
- **les gabarits héritent** les uns des autres (`{% extends %}`, `{% block %}`) ;
- **`url_for('static', filename=…)` est ici l'idiome correct** — le garde-fou qui l'a refusé
  sur FastAPI doit le laisser passer quand le fichier l'emploie déjà.

## Construction

Netlify ne sert que du statique : `freeze.py` gèle le site avec Frozen-Flask dans `build/`,
renomme les pages en `x.html` (servies à `/x`) et copie `public/` à la racine.

    pip install -r requirements.txt
    python freeze.py

## Le défaut injecté

Comme le site en HTML pur : `blog.html` déclare un canonical avec un slash final, alors que
l'hôte sert `/blog`. La page `a_propos.html` est le témoin, qui doit rester intact.
Les pages du parcours (`templates/gauntlet/`) sont générées par `ops/gauntlet/emit_stacks.py`.
