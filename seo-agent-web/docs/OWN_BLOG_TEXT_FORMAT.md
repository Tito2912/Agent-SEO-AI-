# Ponctuation des brouillons et fichiers du blog

Base : `main 07fb68b692561e17fc7fcce61501e18492eab995`, 14 novembre 2026.

## Problème et comportement corrigé

L’export d’un brouillon contenant `L'audit` produisait `L&\#x27;audit`.
Les guillemets devenaient `&quot;`. Le même traitement était utilisé pour le
Markdown présenté dans la proposition de publication et la carte Telegram.
Cela compliquait la relecture du fichier exact avant validation.
Le rendu HTML final décodait déjà ces entités ; aucune corruption de la page
publique n’est déduite de ce défaut de source Markdown.

Les deux formateurs conservent désormais apostrophes et guillemets littéraux
avec `html.escape(..., quote=False)`. Les chevrons et esperluettes restent
échappés, ainsi que la syntaxe Markdown. Le texte reste dans un contexte de
prose, jamais dans un attribut HTML. Le rendu et les sources autorisées restent
contrôlés par les règles existantes.

Seules deux lignes applicatives changent. Le JSON du brouillon relu, son
empreinte, les sources, les droits, les quotas et les décisions ne changent pas.
Une proposition de publication créée avant ce correctif doit conserver son
fichier exact ou passer par la nouvelle préparation prévue par le produit ;
aucun accord n’est transféré silencieusement à un fichier différent.

## Validations

- Quatre cas FR/EN reproduisent le défaut dans les fichiers bruts avant le
  correctif ; ils passent ensuite. Vérification du texte après le vrai parseur
  Markdown et le chargeur de catalogue, sources conservées, données d’entrée
  inchangées et marqueurs Markdown inertes.
- 162 tests rédaction, publication et Telegram réussis. Transports simulés :
  aucun appel IA, message Telegram ou écriture GitHub de produit.
- Ruff F/E9 et `git diff --check` réussis.
- Dix observations Chromium locales : FR/EN, 1440/1024/768/390/360 px, texte exact,
  citations et ponctuation lisibles, aucun débordement ou erreur JavaScript.
  Réseau bloqué, exemples fictifs et aucun accès de production.

Le navigateur utilise les fonctions des deux formateurs extraites du code et
le convertisseur Markdown réel, dans une page locale de contrôle. Ce n’est pas
une recette authentifiée de toutes les pages de l’application. Le chargeur de
l’article complet et les protections de publication sont exercés par pytest.

Script reproductible : `own-blog-text-format/browser_smoke.py` ; relevé :
`own-blog-text-format/browser.json`. Deux captures fictives sont conservées localement dans
`/tmp/noyaru-own-blog-text-browser/`, hors du dépôt.
Aucun brouillon opérateur ni donnée client n’est ajouté au dépôt. Aucun article
n’est publié par ce correctif et aucun nouvel appel IA n’est nécessaire.
