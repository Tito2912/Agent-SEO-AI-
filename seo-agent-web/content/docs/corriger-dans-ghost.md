---
title: "Corriger dans Ghost"
meta_title: "Corriger dans Ghost : titres SEO et méta-descriptions"
description: "Connecter Ghost avec une clé Admin API, faire rédiger les titres SEO et méta-descriptions des articles et pages, les corriger en lot, puis vérifier le résultat."
kind: "Documentation"
section: "Corriger"
order: 27
updated_at: "2026-11-03"
audience: "Tous les plans"
keywords: ["Ghost", "clé Admin API", "intégration personnalisée", "titre SEO", "méta-description"]
app_href: "/"
related: ["cms-compatibles", "verifier-une-correction", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Pourquoi Ghost refuse-t-il la connexion ?"
    answer: "Le plus souvent : une version de Ghost antérieure à 5, une clé Admin API incomplète (elle a la forme identifiant:secret), la clé d'un autre site Ghost, ou une adresse qui n'est ni celle du site ni celle de son administration en .ghost.io. Le message affiché nomme la cause."
  - question: "Puis-je corriger la page d'accueil ?"
    answer: "Pas depuis {{app_name}} : Ghost règle le titre et la description de l'accueil dans ses réglages généraux, section Meta data, et non article par article."
  - question: "{{app_name}} peut-il modifier autre chose que le titre et la description ?"
    answer: "Non. La connexion ne touche que le titre SEO et la méta-description des articles et pages publiés. Pour le reste, les instructions guidées et le fix-pack indiquent quoi changer."
  - question: "Comment retirer l'accès de {{app_name}} ?"
    answer: "Supprimez l'intégration personnalisée dans Ghost : sa clé cesse aussitôt de fonctionner. Déconnecter Ghost dans {{app_name}} retire en plus la connexion du projet."
---

Sur un site Ghost, {{app_name}} corrige les **titres SEO** et les **méta-descriptions** des
articles et des pages directement, sans dépôt GitHub : il rédige le texte si vous le souhaitez,
vous le relisez, et rien n'est écrit sur votre site avant votre validation. Une nouvelle analyse
confirme ensuite le résultat.

Cette connexion est disponible en pilote.

## Ce qu'il faut

- **Ghost 5** ou plus récent, hébergé par Ghost(Pro) ou par vos soins ;
- une **clé Admin API**, celle d'une intégration personnalisée ;
- l'adresse HTTPS du site, sur le même domaine que le projet, ou celle de son administration
  en `.ghost.io`.

La clé se crée dans Ghost, sous **Settings → Integrations → Add custom integration**. Nommez
l'intégration « {{app_name}} » et copiez l'**Admin API key** affichée. Supprimer cette
intégration retire l'accès de {{app_name}}, sans toucher à vos autres intégrations.

## Connecter Ghost

1. Dans le projet, ouvrez **CMS et intégrations** et choisissez **Ghost** comme plateforme.
2. Saisissez l'adresse de Ghost et la clé Admin API.
3. Choisissez **Vérifier et connecter Ghost**.

{{app_name}} vérifie la version de Ghost, que l'adresse est bien celle du site du projet et que
la clé est acceptée, avant d'enregistrer la connexion. La clé est chiffrée, n'est jamais
réaffichée, et l'accès est revérifié à chaque opération.

## Corriger une page

Depuis **Correction guidée** ou **Corrections**, choisissez **Préparer dans Ghost**.

1. **Proposer un texte** : {{app_name}} rédige une valeur à partir de ce que l'analyse a lu sur
   la page (titres, intertitres), dans la langue de la page et dans la bonne longueur. Vous
   pouvez aussi écrire le texte vous-même.
2. **Voir l’avant/après** : l'aperçu montre la valeur actuelle et la valeur proposée. Il reste
   valable 20 minutes.
3. **Appliquer sur mon site** : l'article est relu dans Ghost juste avant l'écriture, puis relu
   après pour confirmer l'enregistrement.
4. **Vérifier le résultat** : une nouvelle analyse, décomptée de votre quota habituel de pages,
   confirme que l'anomalie a disparu.

→ [Vérifier une correction](/docs/verifier-une-correction)

### Ce que coûte une proposition

Un texte proposé compte pour **une correction IA** de votre forfait. S'il ne trouve pas de texte
fidèle à la page dans la bonne longueur, {{app_name}} n'en propose aucun et rien n'est décompté.
Le texte que vous écrivez vous-même ne consomme rien.

→ [Plans et quotas](/docs/plans-et-quotas)

## Corriger toutes les pages d'une anomalie

Quand une anomalie touche plusieurs pages, sa fiche propose **Corriger ces pages dans Ghost**.
Les textes sont rédigés une page après l'autre ; vous les relisez sur un seul écran, décochez
les pages à laisser de côté, puis enregistrez la sélection en une fois. **Une seule analyse de
contrôle** vérifie ensuite toutes les pages enregistrées. Un lot affiche jusqu'à 100 pages à la
fois.

## Ce qui protège votre site

- Rien n'est écrit sans un aperçu validé, et seuls le titre SEO et la méta-description des
  articles et pages publiés peuvent l'être.
- L'article est relu juste avant l'écriture : s'il a changé depuis l'aperçu, l'opération
  s'arrête pour conserver vos modifications.
- Si la réponse de Ghost se perd, l'opération reste **à confirmer** : {{app_name}} relit les
  valeurs, sans jamais réécrire automatiquement.
- **Rétablir la valeur précédente** annule une modification si les métadonnées n'ont pas changé
  depuis son application.

Si l'analyse de contrôle lit encore l'ancienne valeur alors que Ghost a bien enregistré la
nouvelle, un cache (CDN ou hébergeur) sert l'ancienne version de la page : le suivi l'indique au
lieu d'annoncer un échec. Videz ce cache, puis relancez le contrôle.

## Ce que la connexion ne fait pas

La page d'accueil se règle dans les réglages généraux de Ghost, section **Meta data**. Les pages
de tag et d'auteur, les redirections et les données structurées restent hors de son périmètre :
les instructions guidées de chaque anomalie indiquent où faire la modification.

→ [CMS et plateformes compatibles](/docs/cms-compatibles)

Les membres de l'équipe d'un projet peuvent connecter Ghost et appliquer des corrections.

→ [Comptes d'équipe](/docs/comptes-d-equipe)
