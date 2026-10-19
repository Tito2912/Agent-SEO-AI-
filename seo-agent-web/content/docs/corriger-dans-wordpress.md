---
title: "Corriger dans WordPress avec Yoast SEO"
meta_title: "Corriger dans WordPress et Yoast SEO — documentation {{app_name}}"
description: "Connecter WordPress et Yoast SEO, faire rédiger les titres et méta-descriptions, corriger toutes les pages d'une anomalie en un lot, puis vérifier le résultat."
kind: "Documentation"
section: "Corriger"
order: 25
updated_at: "2026-10-19"
audience: "Tous les plans"
keywords: ["WordPress", "Yoast SEO", "titre SEO", "méta-description", "mot de passe d'application"]
app_href: "/"
related: ["verifier-une-correction", "fix-pack-sans-depot", "plans-et-quotas", "comptes-d-equipe"]
faq:
  - question: "Pourquoi WordPress refuse-t-il la connexion ?"
    answer: "Le plus souvent : une version de WordPress antérieure à 6.9 ou de Yoast SEO antérieure à 28.7, un compte sans droit de modifier les métadonnées Yoast, ou un mot de passe d'application erroné. Le message affiché nomme la cause."
  - question: "Un texte proposé est-il décompté si je ne l'applique pas ?"
    answer: "Oui : la proposition est un texte écrit par le modèle, elle compte pour une correction IA dès qu'elle est produite. Si aucun texte ne tient dans la bonne longueur, rien n'est proposé et rien n'est décompté."
  - question: "Puis-je annuler une modification ?"
    answer: "Oui, depuis le suivi de la modification : Rétablir la valeur précédente remet l'ancienne valeur, à condition que les métadonnées de la page n'aient pas changé depuis."
  - question: "{{app_name}} peut-il modifier autre chose que le titre et la description ?"
    answer: "Non. La connexion ne touche que le titre SEO et la méta-description Yoast des pages et articles publiés. Pour le reste, les instructions guidées et le fix-pack indiquent quoi changer."
---

Sur un site WordPress équipé de Yoast SEO, {{app_name}} corrige les **titres SEO** et les
**méta-descriptions** directement, sans dépôt GitHub : il rédige le texte si vous le
souhaitez, vous le relisez, et rien n'est écrit sur votre site avant votre validation. Une
nouvelle analyse confirme ensuite le résultat.

Cette connexion est disponible en pilote.

## Ce qu'il faut

- **WordPress 6.9** ou plus récent, et **Yoast SEO 28.7** ou plus récent ;
- un compte WordPress autorisé à modifier les métadonnées Yoast ;
- un **mot de passe d'application** pour ce compte ;
- l'adresse HTTPS de WordPress, sur le même domaine que le projet et accessible sur Internet.

Le mot de passe d'application se crée dans WordPress, sous **Utilisateurs → Profil → Mots de
passe d'application**. Créez un accès dédié, nommé par exemple « {{app_name}} » : vous pourrez
le révoquer seul, sans toucher à votre mot de passe habituel.

## Connecter WordPress

1. Dans le projet, ouvrez **CMS et intégrations** et choisissez **WordPress** avec **Yoast SEO**.
2. Saisissez l'adresse de WordPress (avec son sous-dossier s'il y est installé), l'identifiant
   et le mot de passe d'application.
3. Choisissez **Vérifier et connecter WordPress**.

{{app_name}} vérifie l'accès et la version de Yoast avant d'enregistrer la connexion. Les
identifiants sont chiffrés, ne sont jamais réaffichés, et l'accès est revérifié à chaque
opération. **Déconnecter WordPress** retire la connexion du projet ; révoquez aussi le mot de
passe d'application dans WordPress si vous ne comptez plus l'utiliser.

## Corriger une page

Depuis **Première correction** ou **Corrections**, choisissez **Préparer dans WordPress**.

1. **Proposer un texte** : {{app_name}} rédige une valeur à partir de ce que l'analyse a lu sur
   la page (titres, intertitres), dans la langue de la page et dans la bonne longueur. Vous
   pouvez aussi écrire le texte vous-même.
2. **Voir l'avant/après** : l'aperçu montre la valeur actuelle et la valeur proposée. Il reste
   valable 20 minutes.
3. **Appliquer sur mon site** : la page est relue dans WordPress juste avant l'écriture, puis
   relue après pour confirmer l'enregistrement.
4. **Vérifier le résultat** : une nouvelle analyse, décomptée de votre quota habituel de pages,
   confirme que l'anomalie a disparu.

→ [Vérifier une correction](/docs/verifier-une-correction)

### Ce que coûte une proposition

Un texte proposé compte pour **une correction IA** de votre forfait, comme un fichier écrit
dans un dépôt. S'il ne trouve pas de texte fidèle à la page dans la bonne longueur,
{{app_name}} n'en propose aucun et rien n'est décompté. Le texte que vous écrivez vous-même
ne consomme rien.

→ [Plans et quotas](/docs/plans-et-quotas)

Les titres et descriptions **en double** ne sont pas rédigés : le bon texte doit se distinguer
de celui des autres pages, et c'est à vous de l'écrire.

## Corriger toutes les pages d'une anomalie

Quand une anomalie touche plusieurs pages, sa fiche propose **Corriger ces pages dans
WordPress**. L'écran du lot regroupe trois étapes :

1. **Faire rédiger les textes** : les propositions sont rédigées une page après l'autre. Le
   lot s'arrête quand votre forfait n'a plus de correction IA ; une page sans proposition
   n'arrête pas les autres.
2. **Relire et enregistrer** : modifiez un texte si besoin, décochez les pages à laisser de
   côté, puis enregistrez la sélection en une fois. Chaque page est relue et écrite
   séparément : une page modifiée dans WordPress entre-temps est arrêtée seule.
3. **Vérifier le résultat** : **une seule analyse de contrôle** vérifie toutes les pages
   enregistrées, et le résultat s'affiche page par page. Les pages résolues restent visibles
   sur l'écran du lot pendant 30 jours.

Un lot affiche jusqu'à 100 pages à la fois.

## Ce qui protège votre site

- Rien n'est écrit sans un aperçu validé, et seuls le titre SEO et la méta-description des
  pages et articles publiés peuvent l'être.
- La page est relue juste avant l'écriture : si elle a changé depuis l'aperçu, l'opération
  s'arrête pour conserver vos modifications.
- Si la réponse de WordPress se perd, l'opération reste **à confirmer** : {{app_name}} relit
  les valeurs, sans jamais réécrire automatiquement.
- **Rétablir la valeur précédente** annule une modification si les métadonnées n'ont pas
  changé depuis son application.

## « Votre site affiche encore l'ancienne version »

La plupart des sites WordPress ont un cache de pages. Si l'analyse de contrôle lit encore
l'ancienne valeur alors que WordPress a bien enregistré la nouvelle, le suivi l'indique au
lieu d'annoncer un échec : videz le cache (extension, hébergeur ou CDN), puis relancez le
contrôle. Inutile de réécrire le texte.

## Ce que la connexion ne fait pas

Les redirections, les canonicals, les données structurées et les sites sous Rank Math restent
hors de son périmètre. Les instructions guidées de chaque anomalie indiquent où faire la
modification, et le fix-pack fournit les données à appliquer vous-même.

→ [Fix-pack : corriger sans dépôt](/docs/fix-pack-sans-depot)

Les membres de l'équipe d'un projet peuvent connecter WordPress et appliquer des corrections,
comme ils peuvent proposer des corrections sur GitHub.

→ [Comptes d'équipe](/docs/comptes-d-equipe)
