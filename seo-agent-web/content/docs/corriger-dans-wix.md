---
title: "Corriger dans Wix"
meta_title: "Corriger dans Wix : titre SEO et méta-description"
description: "Connecter Wix avec une clé API limitée au SEO, faire rédiger le titre SEO et la méta-description des pages et fiches produit, puis vérifier le résultat."
kind: "Documentation"
section: "Corriger"
order: 29
updated_at: "2026-11-04"
audience: "Tous les plans"
keywords: ["Wix", "clé API Wix", "titre SEO", "méta-description", "fiche produit"]
app_href: "/"
related: ["cms-compatibles", "verifier-une-correction", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Pourquoi Wix refuse-t-il la connexion ?"
    answer: "Le plus souvent : une clé sans la permission SEO, une clé limitée à un autre site, l'identifiant d'un autre site, ou un site publié à une autre adresse que celle du projet (un domaine personnalisé et l'adresse en wixsite.com ne sont pas interchangeables). Le message affiché nomme la cause."
  - question: "Faut-il republier le site après une correction ?"
    answer: "Non. La page est mise à jour à la fois dans l'éditeur Wix et en ligne, sans publier vos autres modifications en attente. Le site doit en revanche avoir été publié au moins une fois."
  - question: "Pourquoi les articles de blog ne sont-ils pas modifiables ?"
    answer: "Parce que Wix refuse pour l'instant toute modification d'un article de blog par une autre application, même de ses seuls réglages SEO. Pour ces pages, les instructions guidées de l'anomalie indiquent où faire la correction dans Wix."
  - question: "{{app_name}} peut-il modifier autre chose que le titre et la description ?"
    answer: "Non. La connexion ne touche que le titre SEO et la méta-description des pages et fiches produit publiées. Les autres réglages SEO de la page sont renvoyés tels qu'ils étaient."
---

Sur un site Wix, {{app_name}} corrige le **titre SEO** et la **méta-description** des **pages**
et des **fiches produit** directement, sans dépôt GitHub : il rédige le texte si vous le
souhaitez, vous le relisez, et rien n'est écrit sur le site avant votre validation. Une nouvelle
analyse confirme ensuite le résultat.

Cette connexion est disponible en pilote.

## Ce qu'il faut

- un site Wix **publié**, à l'adresse que le projet analyse (domaine personnalisé ou adresse
  gratuite en `wixsite.com`) ;
- une **clé API** créée dans votre compte Wix, limitée à ce site, avec la seule permission
  **SEO** ;
- l'**identifiant du site**.

La clé se crée dans votre compte Wix, sur la page **Clés API**
(`manage.wix.com/account/api-keys`), avec ces réglages :

| Réglage | Ce qu'il faut choisir |
|---|---|
| Nom | « {{app_name}} » |
| Sites | **Specific sites**, puis uniquement le site du projet |
| Autorisations | décochez **Toutes les autorisations**, puis cochez seulement **SEO** |

Copiez la clé : Wix ne l'affiche qu'une fois. L'identifiant du site figure dans l'adresse de son
tableau de bord : `manage.wix.com/dashboard/<identifiant>/…`.

## Connecter Wix

1. Dans le projet, ouvrez **CMS et intégrations** et choisissez **Wix** comme plateforme.
2. Saisissez l'identifiant du site et la clé API.
3. Choisissez **Vérifier et connecter Wix**.

{{app_name}} vérifie que la clé ouvre les réglages SEO de ce site, publié à l'adresse du projet,
avant d'enregistrer la connexion. La clé est chiffrée, n'est jamais réaffichée, et l'accès est
revérifié à chaque opération.

## Corriger une page

Depuis **Correction guidée** ou **Corrections**, choisissez **Préparer dans Wix**.

1. **Proposer un texte** : {{app_name}} rédige une valeur à partir de ce que l'analyse a lu sur
   la page (titres, intertitres), dans la langue de la page et dans la bonne longueur. Vous
   pouvez aussi écrire le texte vous-même.
2. **Voir l’avant/après** : l'aperçu montre la valeur actuelle et la valeur proposée. Il reste
   valable 20 minutes.
3. **Appliquer sur mon site** : les réglages SEO de la page sont relus juste avant l'écriture ;
   la page est mise à jour dans l'éditeur Wix et en ligne, sans publier le reste du site, puis
   relue pour confirmer l'enregistrement.
4. **Vérifier le résultat** : une nouvelle analyse, décomptée de votre quota habituel de pages,
   confirme que l'anomalie a disparu.

→ [Vérifier une correction](/docs/verifier-une-correction)

### Les pages modifiables

{{app_name}} modifie les **pages** du site, accueil compris, et les **fiches produit** de la
boutique Wix, dans la langue principale du site. Les **articles de blog** se corrigent dans Wix,
avec les instructions guidées de l'anomalie : Wix ne laisse pas encore une autre application
les modifier. Les autres langues d'un site multilingue restent aussi hors de la connexion.

### Ce que coûte une proposition

Un texte proposé compte pour **une correction IA** de votre forfait. S'il ne trouve pas de texte
fidèle à la page dans la bonne longueur, {{app_name}} n'en propose aucun et rien n'est décompté.
Le texte que vous écrivez vous-même ne consomme rien.

→ [Plans et quotas](/docs/plans-et-quotas)

## Corriger toutes les pages d'une anomalie

Quand une anomalie touche plusieurs pages, sa fiche propose **Corriger ces pages dans Wix**. Les
textes sont rédigés une page après l'autre ; vous les relisez sur un seul écran, décochez les
pages à laisser de côté, puis enregistrez la sélection en une fois. **Une seule analyse de
contrôle** vérifie ensuite toutes les pages enregistrées. Un lot affiche jusqu'à 100 pages à la
fois.

## Ce qui protège votre site

- Rien n'est écrit sans un aperçu validé, et seuls le titre SEO et la méta-description des pages
  et fiches produit publiées peuvent l'être.
- **Les autres réglages SEO de la page sont conservés** : Wix remplace tous les réglages SEO
  propres à une page à chaque écriture ; {{app_name}} les relit juste avant et renvoie à
  l'identique tout ce qui n'est pas le champ corrigé (partage sur les réseaux sociaux, URL
  canonique, données structurées…).
- Les réglages sont relus juste avant l'écriture : s'ils ont changé depuis l'aperçu, l'opération
  s'arrête pour conserver vos modifications.
- Si la réponse de Wix se perd ou signale une erreur, l'opération reste **à confirmer** :
  {{app_name}} relit les valeurs, sans jamais réécrire automatiquement.
- **Rétablir la valeur précédente** annule une modification si les métadonnées n'ont pas changé
  depuis son application ; une page qui n'avait pas de valeur propre retrouve celle du modèle
  SEO de Wix.

Une modification est en général en ligne en moins d'une minute. Si l'analyse de contrôle lit
encore l'ancienne valeur alors que Wix a bien enregistré la nouvelle, le suivi l'indique au lieu
d'annoncer un échec : relancez le contrôle quelques minutes plus tard.

## Ce que la connexion ne fait pas

Les articles de blog, les autres langues d'un site multilingue, les redirections, les adresses
des pages et les données structurées restent hors de son périmètre : les instructions guidées
de chaque anomalie indiquent où faire la modification.

→ [CMS et plateformes compatibles](/docs/cms-compatibles)

Les membres de l'équipe d'un projet peuvent connecter Wix et appliquer des corrections.

→ [Comptes d'équipe](/docs/comptes-d-equipe)
