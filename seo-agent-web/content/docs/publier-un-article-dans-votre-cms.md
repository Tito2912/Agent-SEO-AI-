---
title: "Publier un article dans votre CMS"
meta_title: "Publier un article dans votre CMS (WordPress, Ghost, Wix)"
description: "Envoyer un article rédigé dans Noyaru vers votre CMS en brouillon, le relire, puis le publier après une confirmation séparée ; ce que fait chaque CMS et comment vérifier le résultat."
kind: "Documentation"
section: "Aller plus loin"
order: 45
updated_at: "2026-11-23"
audience: "Pro et Business"
keywords: ["publier un article", "WordPress", "Ghost", "Webflow", "Wix Blog", "PrestaShop", "brouillon", "CMS"]
app_href: "/"
related: ["rediger-du-contenu", "cms-compatibles", "verifier-une-correction", "comptes-d-equipe"]
faq:
  - question: "{{app_name}} publie-t-il mon article sans me le demander ?"
    answer: "Non. Le premier envoi enregistre un brouillon, après avoir coché que vous avez relu le contenu et sa destination. La publication est une seconde étape, avec sa propre case à cocher."
  - question: "Pourquoi cette connexion est-elle distincte de celle des corrections SEO ?"
    answer: "Parce qu'elle demande d'autres droits : créer et publier un article, pas seulement modifier un titre SEO ou une méta-description. Les droits de modification des métadonnées seuls ne suffisent pas. Elle se connecte et se déconnecte séparément."
  - question: "Que faire si le résultat est « à confirmer » ?"
    answer: "Ouvrez votre CMS et regardez si l'article existe. {{app_name}} ne renvoie jamais seul une création : il pourrait en faire un doublon. Utilisez ensuite « Relire le résultat dans mon CMS », qui lit sans rien écrire."
  - question: "Pourquoi mon CMS n'affiche-t-il pas la mise en page de l'aperçu ?"
    answer: "Parce que le thème de votre site décide de la mise en page finale. L'aperçu sert à relire le texte, les liens et les informations métier, pas l'habillage."
  - question: "Une publication améliore-t-elle mon référencement ?"
    answer: "Elle ne le prouve pas. Une nouvelle analyse doit confirmer que la page est accessible et indexable ; la mesure du résultat se lit ensuite dans la performance."
---

Un article rédigé dans {{app_name}} peut partir directement dans votre CMS, sans copier-coller :
vous relisez son aperçu, il est enregistré **en brouillon**, puis vous le publiez par une
**confirmation séparée**. Rien n'est écrit sur votre site avant vos deux validations.

Cette connexion est disponible en pilote. Elle sert aux articles rédigés dans {{app_name}} ;
la rédaction elle-même est décrite dans
[Rédiger un article et préparer sa publication](/docs/rediger-du-contenu).

## Ce que fait chaque CMS

| CMS | Ce qui est créé | Ce qu'il faut côté CMS |
|---|---|---|
| WordPress | un **article** (statut brouillon, puis publié) | l'adresse HTTPS du site, un identifiant et un **mot de passe d'application** dont le compte peut modifier et publier des articles ; Yoast n'est pas nécessaire |
| Ghost | un **article** (brouillon, puis publié) ; aucune newsletter n'est envoyée | l'adresse HTTPS du site et la **clé Admin API** d'une intégration personnalisée |
| Webflow | un **élément d'une collection** de votre choix, en brouillon puis publié | l'identifiant du site, un jeton de l'intégration, la collection de destination et son champ de **texte riche** |
| Wix | un **brouillon d'article de blog**, puis sa publication | l'identifiant du site, une clé API, et l'identifiant du **membre auteur** dans Wix |
| PrestaShop | une **page CMS** (pas un article de blog), inactive tant qu'elle n'est pas publiée | l'adresse HTTPS de la boutique, une clé du service web, le numéro de langue et de boutique |

Les blogs fournis par une extension PrestaShop demandent une intégration propre à cette
extension : ce parcours crée une page CMS, rien d'autre.

La clé que vous saisissez est chiffrée et n'est jamais réaffichée. Utilisez une clé créée pour
cet usage : les droits de modification des métadonnées seuls ne suffisent pas.

## Publier un article

1. Dans le projet, ouvrez **Contenu**, puis **Publier dans mon CMS**.
2. Choisissez votre plateforme et **connectez-la** : {{app_name}} vérifie que la clé ouvre bien le
   site de ce projet avant d'enregistrer la connexion.
3. Choisissez l'**article** et sa **langue**, puis ce que votre CMS demande (collection et champ
   de texte pour Webflow, auteur pour Wix, langue et boutique pour PrestaShop).
4. Choisissez **Préparer l'aperçu de publication** et relisez le texte, les liens et les
   informations métier.
5. Cochez que vous avez relu le contenu et sa destination, puis choisissez **Enregistrer le
   brouillon dans mon CMS**.
6. Relisez le brouillon dans votre CMS si vous le souhaitez, puis, dans {{app_name}}, cochez
   **Je confirme la publication de cette version** et choisissez **Publier sur mon site**.

Si vous modifiez le brouillon dans le CMS entre les deux étapes, préparez une nouvelle
relecture : {{app_name}} ne publie que la version que vous avez validée.

→ [Vérifier une correction](/docs/verifier-une-correction)

## Les images

Les images d'un article de {{app_name}} sont d'abord privées. Pour **WordPress** et **Ghost**,
cochez l'autorisation de transfert à l'étape 3 : elles sont envoyées dans les médias de votre
CMS, et l'aperçu n'est préparé qu'une fois chacune confirmée. Si le CMS en refuse une, un nouvel
essai demande votre accord exprès, avec une case dédiée.

Pour **Webflow, Wix et PrestaShop**, le transfert des images privées n'est pas encore disponible :
utilisez des images déjà hébergées sur votre site.

## Ce qui protège votre site

- Le contenu est vérifié **avant tout envoi** : un texte ou une image que le CMS ne pourrait pas
  accepter est refusé avant la première écriture, sans rien laisser derrière.
- Brouillon et publication sont deux confirmations distinctes, et chacune garde un reçu de votre
  accord.
- Si la réponse du CMS se perd, le résultat reste **à confirmer** : {{app_name}} ne renvoie jamais
  seul une création, pour ne pas faire de doublon.
- **Relire le résultat dans mon CMS** compare ce que le CMS affiche à ce que vous avez validé, sans
  rien écrire. Si le brouillon a changé côté CMS, la relecture le dit et vous poursuivez
  directement dans votre CMS.
- Sur Webflow, seul l'élément choisi est publié, pas le reste du site.

## Après la publication

Le CMS confirme la publication et {{app_name}} affiche l'adresse de la page. Une nouvelle analyse
doit encore confirmer que la page est accessible et indexable : la publication seule ne prouve
aucune progression du référencement.

→ [Suivre la performance](/docs/suivre-la-performance)

Les membres de l'équipe d'un projet peuvent connecter un CMS et publier.

→ [Comptes d'équipe](/docs/comptes-d-equipe)
