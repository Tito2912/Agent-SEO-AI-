---
title: "Corriger dans Webflow"
meta_title: "Corriger dans Webflow : titre SEO et méta-description"
description: "Connecter Webflow avec un jeton de site, faire rédiger le titre SEO et la méta-description des pages statiques, publier dans Webflow, puis vérifier."
kind: "Documentation"
section: "Corriger"
order: 30
updated_at: "2026-11-04"
audience: "Tous les plans"
keywords: ["Webflow", "jeton de site Webflow", "titre SEO", "méta-description", "publier Webflow"]
app_href: "/"
related: ["cms-compatibles", "verifier-une-correction", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Pourquoi {{app_name}} ne publie-t-il pas mon site ?"
    answer: "Parce qu'une publication Webflow met en ligne tout ce qui attend dans le Designer, y compris un travail que vous n'avez pas terminé. {{app_name}} enregistre donc ses corrections et vous laisse publier quand vous le décidez ; le jeton conseillé ne permet d'ailleurs pas de publier."
  - question: "Pourquoi Webflow refuse-t-il la connexion ?"
    answer: "Le plus souvent : un jeton sans les droits Pages en écriture, un jeton supprimé, un jeton d'un autre site, ou un site publié à une autre adresse que celle du projet. Le message affiché nomme la cause."
  - question: "Pourquoi les articles du blog ne sont-ils pas modifiables ?"
    answer: "Les articles, comme toutes les pages d'une collection CMS, prennent leur titre et leur description dans le modèle de la collection, commun à tous ses éléments. Le modifier changerait chaque article. Pour ces pages, les instructions guidées de l'anomalie indiquent où faire la correction dans Webflow."
  - question: "{{app_name}} peut-il modifier autre chose que le titre et la description ?"
    answer: "Non. La connexion ne touche que le titre SEO et la méta-description des pages statiques ; seul le champ corrigé est envoyé à Webflow."
---

Sur un site Webflow, {{app_name}} corrige le **titre SEO** et la **méta-description** des
**pages statiques** directement, sans dépôt GitHub : il rédige le texte si vous le souhaitez,
vous le relisez, et rien n'est écrit avant votre validation. {{app_name}} **ne publie jamais
votre site** : la correction attend votre prochaine publication dans Webflow, puis une nouvelle
analyse confirme le résultat.

Cette connexion est disponible en pilote.

## Ce qu'il faut

- un site Webflow **publié**, à l'adresse que le projet analyse (domaine personnalisé ou adresse
  en `webflow.io`) ;
- un **jeton de site** créé dans les réglages du site, avec les droits ci-dessous.

Le jeton se crée dans Webflow, dans les réglages du site (**Site settings**), sous
**Apps & integrations → API access** : générez un jeton nommé « {{app_name}} » avec ces droits,
et aucun autre :

| Droit | Niveau à choisir |
|---|---|
| Pages | lecture et écriture (*Read and write*) |
| Sites | lecture seule (*Read-only*) : il ne permet pas de publier |

Copiez le jeton : Webflow ne l'affiche qu'une fois.

## Connecter Webflow

1. Dans le projet, ouvrez **CMS et intégrations** et choisissez **Webflow** comme plateforme.
2. Collez le jeton de site.
3. Choisissez **Vérifier et connecter Webflow**.

{{app_name}} vérifie les droits du jeton et que le site est publié à l'adresse du projet avant
d'enregistrer la connexion. Le jeton est chiffré, n'est jamais réaffiché, et l'accès est
revérifié à chaque opération.

## Corriger une page

Depuis **Correction guidée** ou **Corrections**, choisissez **Préparer dans Webflow**.

1. **Proposer un texte** : {{app_name}} rédige une valeur à partir de ce que l'analyse a lu sur
   la page (titres, intertitres), dans la langue de la page et dans la bonne longueur. Vous
   pouvez aussi écrire le texte vous-même.
2. **Voir l’avant/après** : l'aperçu montre la valeur actuelle et la valeur proposée. Il reste
   valable 20 minutes.
3. **Appliquer sur mon site** : la page est relue juste avant l'écriture ; seul le champ corrigé
   est envoyé à Webflow, puis la page est relue pour confirmer l'enregistrement.
4. **Publier dans Webflow** : dans le Designer, choisissez **Publish**. La correction est en
   ligne à ce moment-là, avec vos autres modifications en attente.
5. **Vérifier le résultat** : une nouvelle analyse, décomptée de votre quota habituel de pages,
   confirme que l'anomalie a disparu.

→ [Vérifier une correction](/docs/verifier-une-correction)

### Les pages modifiables

{{app_name}} modifie les **pages statiques** publiées, accueil compris, dans la langue
principale du site. Les éléments d'une **collection CMS** (articles de blog, fiches…) prennent
leur titre et leur description dans le modèle de la collection, commun à tous : ils se
corrigent dans Webflow, avec les instructions guidées de l'anomalie. Les brouillons, les pages
utilitaires (404, page protégée) et les autres langues restent aussi hors de la connexion.

### Ce que coûte une proposition

Un texte proposé compte pour **une correction IA** de votre forfait. S'il ne trouve pas de texte
fidèle à la page dans la bonne longueur, {{app_name}} n'en propose aucun et rien n'est décompté.
Le texte que vous écrivez vous-même ne consomme rien.

→ [Plans et quotas](/docs/plans-et-quotas)

## Corriger toutes les pages d'une anomalie

Quand une anomalie touche plusieurs pages, sa fiche propose **Corriger ces pages dans Webflow**.
Les textes sont rédigés une page après l'autre ; vous les relisez sur un seul écran, décochez
les pages à laisser de côté, puis enregistrez la sélection en une fois. **Une seule
publication** dans Webflow les met toutes en ligne, et **une seule analyse de contrôle** les
vérifie ensuite. Un lot affiche jusqu'à 100 pages à la fois.

## Ce qui protège votre site

- Rien n'est écrit sans un aperçu validé, et seuls le titre SEO et la méta-description des pages
  statiques peuvent l'être.
- **Rien n'est publié** : vos autres modifications en cours dans le Designer restent en attente
  tant que vous ne publiez pas.
- La page est relue juste avant l'écriture : si elle a changé depuis l'aperçu, l'opération
  s'arrête pour conserver vos modifications.
- Si la réponse de Webflow se perd ou signale une erreur, l'opération reste **à confirmer** :
  {{app_name}} relit les valeurs, sans jamais réécrire automatiquement.
- **Rétablir la valeur précédente** annule une modification si les métadonnées n'ont pas changé
  depuis son application ; comme une correction, le retour en arrière attend votre publication.

Si l'analyse de contrôle lit encore l'ancienne valeur alors que Webflow a bien enregistré la
nouvelle, la modification n'est sans doute pas encore publiée : le suivi l'indique au lieu
d'annoncer un échec. Publiez votre site dans Webflow, puis relancez le contrôle.

## Ce que la connexion ne fait pas

Les pages de collection, les autres langues, les redirections, les adresses des pages et les
données structurées restent hors de son périmètre : les instructions guidées de chaque anomalie
indiquent où faire la modification.

→ [CMS et plateformes compatibles](/docs/cms-compatibles)

Les membres de l'équipe d'un projet peuvent connecter Webflow et appliquer des corrections.

→ [Comptes d'équipe](/docs/comptes-d-equipe)
