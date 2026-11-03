---
title: "CMS et plateformes compatibles"
meta_title: "CMS et plateformes compatibles — documentation {{app_name}}"
description: "Ce que {{app_name}} corrige sur votre plateforme : WordPress, Ghost, PrestaShop connectés ; Shopify, Wix, Webflow, Squarespace, Systeme.io guidés ; code sur GitHub."
kind: "Documentation"
section: "Corriger"
order: 25
updated_at: "2026-11-03"
audience: "Tous les plans"
keywords: ["CMS", "WordPress", "Ghost", "PrestaShop", "Shopify", "Wix", "Webflow"]
app_href: "/"
related: ["corriger-dans-wordpress", "corriger-dans-ghost", "corriger-dans-prestashop", "fix-pack-sans-depot"]
faq:
  - question: "Mon CMS n'est pas dans la liste : que faire ?"
    answer: "Choisissez Autre CMS. L'audit, les rapports et l'explication de chaque anomalie fonctionnent de la même façon ; seules les étapes propres à votre outil manquent. Dès le plan Solo, le fix-pack fournit les données à appliquer."
  - question: "Puis-je connecter deux CMS au même projet ?"
    answer: "Non. Un projet a une seule plateforme : en choisir une autre retire la connexion existante."
  - question: "La connexion à un CMS est-elle incluse dans le plan gratuit ?"
    answer: "Oui. Les textes proposés consomment les corrections IA du forfait, {{corrections_free}} par mois avec le plan Free. Un texte que vous écrivez vous-même ne consomme rien."
---

L'audit fonctionne sur tous les sites : son adresse suffit. Ce qui change d'une plateforme à
l'autre, c'est la façon de **corriger**. {{app_name}} connaît trois cas.

## Les trois façons de corriger

| Votre site | Ce que fait {{app_name}} | Ce qu'il faut |
|---|---|---|
| Code sur GitHub (Next.js, Astro, Nuxt, Hugo…) | prépare les corrections dans une pull request, à relire avant publication | un dépôt d'une technologie prise en charge |
| WordPress avec Yoast SEO | rédige et enregistre les titres SEO et méta-descriptions des pages et articles | WordPress 6.9, Yoast SEO 28.7, un mot de passe d'application |
| Ghost | rédige et enregistre les titres SEO et méta-descriptions des articles et pages | Ghost 5, une clé Admin API |
| PrestaShop | rédige et enregistre les titres SEO et méta-descriptions des produits et catégories | PrestaShop 8, une clé Webservice |
| Shopify, Wix, Webflow, Squarespace, Systeme.io, WordPress avec Rank Math | indique, pour chaque anomalie, où faire le changement dans votre outil | rien à connecter |

→ [Connecter GitHub](/docs/connecter-github)

## Choisir votre plateforme

Dans le projet, ouvrez **CMS et intégrations**, répondez à **Sur quelle plateforme est votre
site ?**, puis choisissez **Enregistrer ma plateforme**. Ce choix adapte les instructions de
correction de chaque anomalie. Pour WordPress, Ghost et PrestaShop, la carte de connexion
s'affiche sur le même écran.

Un projet a une seule plateforme : en choisir une autre retire la connexion existante.

## Les CMS connectés

Sur WordPress, Ghost et PrestaShop, la connexion couvre le **titre SEO** et la
**méta-description**. Le parcours est le même partout :

- {{app_name}} propose un texte, dans la langue de la page et dans la bonne longueur ; vous
  pouvez aussi écrire le vôtre ;
- rien n'est enregistré avant que vous ayez validé l'aperçu ;
- une anomalie qui touche plusieurs pages se corrige en un lot, vérifié par une seule analyse ;
- chaque texte proposé compte pour une correction IA de votre forfait ; un texte que vous
  écrivez vous-même ne consomme rien.

Chaque CMS a ses prérequis et son périmètre :

- [Corriger dans WordPress avec Yoast SEO](/docs/corriger-dans-wordpress)
- [Corriger dans Ghost](/docs/corriger-dans-ghost)
- [Corriger dans PrestaShop](/docs/corriger-dans-prestashop)

## Les plateformes guidées

Sur Shopify, Wix, Webflow, Squarespace et Systeme.io, {{app_name}} n'écrit pas sur votre site.
Chaque anomalie affiche les étapes à suivre dans votre outil, l'écran à ouvrir et le champ à
remplir, puis la nouvelle analyse contrôle le résultat. Il en va de même sur WordPress avec
Rank Math : la connexion passe par Yoast SEO.

Dès le plan Solo, le fix-pack rassemble les données à appliquer et leur mode d'emploi, pour vous
ou pour votre prestataire.

→ [Fix-pack : corriger sans dépôt](/docs/fix-pack-sans-depot)

## Ce qui reste hors des CMS connectés

Les redirections, les canonicals, les données structurées et le contenu des pages ne passent
pas par la connexion. Les instructions guidées de chaque anomalie indiquent où faire la
modification ; sur un dépôt GitHub compatible, {{app_name}} les prépare dans une pull request.

→ [Corrections automatiques](/docs/corrections-automatiques)
