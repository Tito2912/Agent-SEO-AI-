---
title: "Corriger dans PrestaShop"
meta_title: "Corriger dans PrestaShop : title et méta-description"
description: "Connecter PrestaShop avec une clé Webservice, faire rédiger la balise title et la méta-description des produits et catégories dans chaque langue, puis vérifier."
kind: "Documentation"
section: "Corriger"
order: 28
updated_at: "2026-11-03"
audience: "Tous les plans"
keywords: ["PrestaShop", "clé Webservice", "balise title", "méta-description", "fiche produit"]
app_href: "/"
related: ["cms-compatibles", "verifier-une-correction", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Pourquoi PrestaShop refuse-t-il la connexion ?"
    answer: "Le plus souvent : une boutique en version 1.7, un service web désactivé, une clé sans les droits products, categories et languages, ou une adresse /api qui ne répond pas faute de réécriture d'adresses chez l'hébergeur. Le message affiché nomme la cause."
  - question: "Ma boutique est en plusieurs langues : quelle traduction est modifiée ?"
    answer: "Celle de la page analysée, lue au début de son adresse (/fr/, /en/…). Les autres traductions sont renvoyées telles qu'elles étaient. Une boutique multilingue dont les adresses ne disent pas la langue n'est pas modifiée."
  - question: "Pourquoi les pages CMS ne sont-elles pas modifiables ?"
    answer: "Parce qu'en enregistrant la balise title d'une page CMS, PrestaShop réenregistre aussi son contenu et peut l'altérer. Pour ces pages, les instructions guidées de l'anomalie indiquent où faire la correction dans PrestaShop."
  - question: "{{app_name}} peut-il modifier autre chose que le titre et la description ?"
    answer: "Non. La connexion ne touche que la balise title et la méta-description des produits et catégories publiés. Pour le reste, les instructions guidées et le fix-pack indiquent quoi changer."
---

Sur une boutique PrestaShop, {{app_name}} corrige la **balise title** et la
**méta-description** des **produits** et des **catégories** directement, sans dépôt GitHub, dans
la langue de chaque page : il rédige le texte si vous le souhaitez, vous le relisez, et rien
n'est écrit dans la boutique avant votre validation. Une nouvelle analyse confirme ensuite le
résultat.

Cette connexion est disponible en pilote.

## Ce qu'il faut

- **PrestaShop 8** ou plus récent : la version 1.7 ne permet pas de modifier un seul champ ;
- le **service web** activé, avec une **clé Webservice** dédiée ;
- l'adresse HTTPS de la boutique, sur le même domaine que le projet ;
- dans une boutique en plusieurs langues, des adresses qui commencent par la langue (`/fr/`,
  `/en/`…), comme le fait PrestaShop par défaut.

La clé se crée dans PrestaShop, sous **Paramètres avancés → Webservice** : activez le service
web, puis ajoutez une clé nommée « {{app_name}} » avec ces droits, et aucun autre :

| Ressource | Droits à cocher |
|---|---|
| `products` | lecture (GET) et modification partielle (PATCH) |
| `categories` | lecture (GET) et modification partielle (PATCH) |
| `languages` | lecture (GET) |

Si l'adresse `/api` de la boutique ne répond pas, la réécriture d'adresses doit être activée
chez l'hébergeur ; c'est le cas par défaut avec Apache.

## Connecter PrestaShop

1. Dans le projet, ouvrez **CMS et intégrations** et choisissez **PrestaShop** comme plateforme.
2. Saisissez l'adresse de la boutique (avec son sous-dossier si PrestaShop y est installé) et
   la clé Webservice.
3. Choisissez **Vérifier et connecter PrestaShop**.

{{app_name}} vérifie la version de PrestaShop, les droits de la clé et les langues de la
boutique avant d'enregistrer la connexion. La clé est chiffrée, n'est jamais réaffichée, et
l'accès est revérifié à chaque opération.

## Corriger une page

Depuis **Correction guidée** ou **Corrections**, choisissez **Préparer dans PrestaShop**.

1. **Proposer un texte** : {{app_name}} rédige une valeur à partir de ce que l'analyse a lu sur
   la page (titres, intertitres), dans la langue de la page et dans la bonne longueur. Vous
   pouvez aussi écrire le texte vous-même.
2. **Voir l’avant/après** : l'aperçu montre la valeur actuelle et la valeur proposée. Il reste
   valable 20 minutes.
3. **Appliquer sur mon site** : la fiche est relue juste avant l'écriture ; seule la langue de
   la page change, puis la fiche est relue pour confirmer l'enregistrement.
4. **Vérifier le résultat** : une nouvelle analyse, décomptée de votre quota habituel de pages,
   confirme que l'anomalie a disparu.

→ [Vérifier une correction](/docs/verifier-une-correction)

### Les pages modifiables

{{app_name}} modifie les pages produit et catégorie **publiées**, à leur adresse habituelle,
qui contient leur identifiant : `/fr/12-chemise-en-lin.html` pour un produit, `/fr/3-vetements`
pour une catégorie. Les pages CMS (livraison, mentions légales…), l'accueil, les marques et
les adresses à paramètres (filtres, tri, pagination) se corrigent dans PrestaShop, avec les
instructions guidées de l'anomalie.

### Ce que coûte une proposition

Un texte proposé compte pour **une correction IA** de votre forfait, quelle que soit la langue
de la page. S'il ne trouve pas de texte fidèle à la page dans la bonne longueur, {{app_name}}
n'en propose aucun et rien n'est décompté. Le texte que vous écrivez vous-même ne consomme rien.

→ [Plans et quotas](/docs/plans-et-quotas)

## Corriger toutes les pages d'une anomalie

Quand une anomalie touche plusieurs pages, sa fiche propose **Corriger ces pages dans
PrestaShop**. Les textes sont rédigés une page après l'autre ; vous les relisez sur un seul
écran, décochez les pages à laisser de côté, puis enregistrez la sélection en une fois. **Une
seule analyse de contrôle** vérifie ensuite toutes les pages enregistrées. Un lot affiche
jusqu'à 100 pages à la fois.

## Ce qui protège votre boutique

- Rien n'est écrit sans un aperçu validé, et seuls la balise title et la méta-description des
  produits et catégories publiés peuvent l'être.
- **Les autres langues sont conservées** : chaque écriture renvoie toutes les traductions du
  champ, inchangées sauf celle de la page. Si PrestaShop ne les fournit pas toutes, rien n'est
  écrit.
- La fiche est relue juste avant l'écriture : si elle a changé depuis l'aperçu, l'opération
  s'arrête pour conserver vos modifications.
- Si la réponse de PrestaShop se perd ou signale une erreur, l'opération reste **à confirmer** :
  {{app_name}} relit les valeurs, sans jamais réécrire automatiquement.
- **Rétablir la valeur précédente** annule une modification si les métadonnées n'ont pas changé
  depuis son application.

Si l'analyse de contrôle lit encore l'ancienne valeur alors que PrestaShop a bien enregistré la
nouvelle, un cache sert l'ancienne version de la page : le suivi l'indique au lieu d'annoncer un
échec. Videz le cache de PrestaShop (**Paramètres avancés → Performances**) et celui de votre
CDN, puis relancez le contrôle.

## Ce que la connexion ne fait pas

Les pages CMS, la page d'accueil, les redirections, les URL simplifiées et les données
structurées restent hors de son périmètre : les instructions guidées de chaque anomalie
indiquent où faire la modification.

→ [CMS et plateformes compatibles](/docs/cms-compatibles)

Les membres de l'équipe d'un projet peuvent connecter PrestaShop et appliquer des corrections.

→ [Comptes d'équipe](/docs/comptes-d-equipe)
