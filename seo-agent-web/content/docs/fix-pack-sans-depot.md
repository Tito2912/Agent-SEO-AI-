---
title: "Corriger sans dépôt Git : le fix-pack"
meta_title: "Fix-pack : corriger un site sans dépôt — documentation {{app_name}}"
description: "L'archive de correctifs prête à appliquer pour les sites WordPress, Shopify, Webflow ou tout site dont le code n'est pas accessible : contenu, format, mode d'emploi."
kind: "Documentation"
section: "Corriger"
order: 24
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["fix pack", "WordPress", "Shopify", "Webflow", "sans dépôt"]
app_href: "/"
related: ["corrections-automatiques", "anomalies-et-priorites", "exports-et-rapports"]
faq:
  - question: "Le fix-pack consomme-t-il mon quota de corrections IA ?"
    answer: "Non. C'est un export construit à partir des données du crawl. Le quota de corrections ne compte que les fichiers écrits par le modèle dans un dépôt."
  - question: "Puis-je importer le CSV de redirections directement chez mon hébergeur ?"
    answer: "Chez ceux qui acceptent un fichier de règles — Netlify, Vercel, Cloudflare — la conversion est immédiate. Sur WordPress, un plugin de redirection accepte généralement un import CSV. Vérifiez toujours quelques lignes avant d'appliquer en masse."
  - question: "Le robots.txt fourni remplace-t-il le mien ?"
    answer: "C'est un modèle, pas un remplacement. Comparez-le au vôtre avant d'écraser quoi que ce soit : votre robots.txt actuel contient peut-être des règles volontaires que le modèle ignore."
---

Tous les sites n'ont pas de dépôt Git accessible. WordPress géré par un tiers, Shopify,
Webflow, Wix, ou simplement un code auquel vous n'avez pas les droits : la
[correction automatique en pull request](/docs/corrections-automatiques) est alors hors de
portée.

Le fix-pack existe pour ces cas. C'est une archive ZIP construite à partir du dernier crawl,
qui contient ce qu'il faut appliquer et le mode d'emploi pour le faire.

## Le télécharger

Page du projet → export **Fix-pack**. L'archive porte sur le dernier crawl disponible ;
relancez un crawl d'abord si le vôtre date.

Aucun quota de correction n'est consommé : c'est un export, pas un travail de modèle.

## Ce qu'il y a dedans

```
README.md                         Le contexte : site, date du crawl, ce que contient le pack
HOW_TO_APPLY.md                   Le mode d'emploi, par plateforme
meta.json                         Les métadonnées du pack
robots.txt                        Un robots.txt proposé, avec la ligne Sitemap
exports/pages_seo.csv             Une ligne par page, toutes les balises constatées
exports/issues_summary.csv        Les anomalies : type, gravité, volume, priorité, effort
exports/redirects_observed.csv    Les redirections réellement rencontrées, avec leur chaîne
exports/redirects_to_fill.csv     Les 404 à rediriger — la colonne destination est à vous
exports/sitemap_urls.txt          La liste des URLs indexables, à transformer en sitemap
schema/website.jsonld             Modèle de données structurées WebSite
schema/organization.jsonld        Modèle de données structurées Organization
```

Deux fichiers méritent qu'on s'y arrête.

### `pages_seo.csv`

Une ligne par page, avec ce que le crawler a vu : `title`, `meta_description`, `canonical`,
`lang`, `meta_robots`, `x_robots_tag`, premier `h1` et nombre de `h1`, nombre de mots, nombre
d'images et combien sans `alt`, poids de la réponse, temps de réponse.

C'est le fichier avec lequel on travaille vraiment. Ouvrez-le dans un tableur, triez sur la
colonne `title` pour voir les doublons apparaître d'un bloc, filtrez les
`meta_description` vides, repérez les `canonical` qui ne pointent pas sur l'URL de la ligne.

### `redirects_to_fill.csv`

La liste des URLs en 404, avec une colonne de destination **vide**. C'est délibéré : choisir
vers quoi rediriger une page supprimée est une décision éditoriale, pas un calcul.

Remplissez la colonne, puis importez le fichier chez votre hébergeur ou votre plugin de
redirection.

!!! warning "Ne redirigez pas tout vers la home"
    C'est le réflexe le plus courant et le plus coûteux : une redirection massive vers
    l'accueil est traitée comme une page d'erreur déguisée. Redirigez vers la page la plus
    proche, ou laissez le 404 quand il n'y a pas d'équivalent.

## Appliquer, par plateforme

Le fichier `HOW_TO_APPLY.md` détaille chaque cas. En résumé :

**WordPress** — `title`, description et canonicals via votre extension SEO (Yoast, Rank Math)
ou l'éditeur. Redirections via une extension dédiée, qui accepte généralement un import CSV.

**Shopify** — `title` et descriptions dans l'admin, produit par produit ou par import. Les
données structurées sont dans le thème, le plus souvent `theme.liquid`.

**Webflow** — balises dans les paramètres de page et de collection. Redirections dans les
réglages du projet.

**Sites codés sans dépôt connecté** — appliquez en code puis, si vous finissez par connecter
le dépôt, la correction automatique prendra le relais et vérifiera les crawls suivants.

## Vérifier ensuite

Le fix-pack sort du produit : {{app_name}} ne sait pas ce que vous en avez fait. La
[vérification automatique](/docs/verifier-une-correction) ne s'applique donc pas.

La méthode reste la même, en manuel : relancez un crawl après avoir appliqué le lot, et
comparez-le au précédent. Les anomalies traitées doivent avoir disparu, et le compteur
d'anomalies apparues doit rester à zéro.

→ [Lire le rapport](/docs/lire-le-rapport)
