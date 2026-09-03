---
title: "Opportunités de mots-clés"
meta_title: "Opportunités de mots-clés — documentation {{app_name}}"
description: "Les trois types d'opportunités détectées dans vos données Search Console, comment elles sont calculées, et la réécriture de page en pull request."
kind: "Documentation"
section: "Search Console et mots-clés"
order: 31
updated_at: "2026-09-03"
audience: "Tous les plans, Search Console requise"
keywords: ["mots-clés", "Search Console", "CTR", "position moyenne", "réécriture"]
app_href: "/"
featured: true
related: ["connecter-search-console", "suivre-la-performance", "corrections-automatiques"]
faq:
  - question: "D'où viennent ces mots-clés ? D'une base d'outil SEO ?"
    answer: "Non. Ce sont vos propres requêtes Search Console, celles sur lesquelles votre site apparaît déjà. Aucun volume de recherche estimé, aucune base tierce : uniquement des faits de votre compte."
  - question: "Pourquoi les requêtes en position 1 à 3 sont-elles absentes ?"
    answer: "Parce qu'il n'y a rien à promettre dessus. Une requête déjà dans le trio de tête a un taux de clic proche de son plafond : la présenter comme une opportunité serait vendre du vent."
  - question: "Le gain de clics estimé est-il fiable ?"
    answer: "C'est une projection basée sur le taux de clic que votre propre site atteint sur ses positions de tête, pas sur une moyenne de marché. Elle n'est affichée que si ce point de comparaison existe, et reste une estimation."
  - question: "La réécriture change-t-elle le contenu de ma page ?"
    answer: "Non. Elle ne touche que le title et la meta description. Si la page se classe déjà sur la requête, son contenu est pertinent — c'est sa présentation dans les résultats qui ne l'est pas."
---

C'est la fonction qui répond à la question « sur quoi je travaille maintenant ? » avec autre
chose qu'une intuition.

Le point de départ n'est pas une base de mots-clés achetée : ce sont **vos** requêtes Search
Console, celles pour lesquelles Google affiche déjà votre site. Le produit ne cherche pas des
mots-clés à conquérir, il cherche ceux que vous perdez de peu.

Prérequis : [Search Console connectée](/docs/connecter-search-console) au projet.

## Les trois types d'opportunités

{{app_name}} demande à Search Console la requête **et** la page ensemble. C'est ce qui fait la
différence entre une remarque et une action : une opportunité qui ne nomme que la requête vous
laisse chercher la page, une qui nomme les deux peut être corrigée.

### Vue jamais cliquée

Position 3 à 10 — la première page de Google — et **zéro clic** sur la période.

Vous êtes visible et personne n'entre. Ce n'est pas un problème de classement, c'est un
problème de `title` et de description : ce que le chercheur lit ne correspond pas à ce qu'il
cherchait.

C'est le cas le plus rentable du lot : le travail est déjà fait, seule la vitrine manque.

### Cliquée moins que d'habitude

Première page, des clics, mais un taux de clic nettement inférieur à ce que **votre site**
obtient d'ordinaire sur ces positions.

La comparaison est interne, et c'est important : comparer votre taux de clic à une moyenne de
marché ne veut rien dire, parce que le vôtre dépend de votre secteur, de votre marque et du
type de résultats affichés. Comparer votre page à vos autres pages, si.

Le seuil est fixé à la moitié de votre référence.

### Proche de la première page

Position 10 à 20. Vous êtes en deuxième page, donc invisible en pratique — mais l'écart est
franchissable.

Ici, contrairement aux deux autres, la réponse est rarement une balise : c'est du contenu, du
maillage interne vers cette page, ou un lien externe.

## Ce que le produit refuse de faire

**Il ignore les requêtes sous 50 impressions.** En dessous, un taux de clic n'a pas de sens
statistique : deux impressions et un clic, c'est 50 %, et ça ne veut rien dire.

**Il ignore les positions 1 à 3.** Une requête déjà en tête a un taux de clic proche de son
plafond. Promettre un gain dessus serait malhonnête.

**Il n'affiche un gain estimé que s'il a une référence.** Le gain projeté est calculé à partir
du taux de clic que votre site atteint réellement sur ses positions de tête. Si le site n'a
aucune position de tête, aucun chiffre n'est affiché — « pas d'estimation disponible » et
« aucun gain attendu » ne sont pas la même réponse.

**Il trie par impressions, pas par gain estimé.** Les impressions sont un fait de votre compte ;
le gain est un modèle. Trier sur le fait plutôt que sur le modèle évite de mettre en tête de
liste les projections les plus optimistes.

## Suivre une requête

Bouton **Suivre** sur une ligne. La requête et sa page cible entrent dans votre liste
surveillée, ce qui vous permet de les retrouver d'un crawl à l'autre sans refaire l'analyse.

## Réécrire la page en pull request

C'est l'action qui ferme la boucle. Search Console nomme la requête et la page ; {{app_name}}
réécrit le `title` et la `meta description` de cette page pour répondre à cette requête, et
ouvre une pull request sur votre dépôt.

La page **garde son sujet**. Elle se classe déjà, donc le contenu est bon ; seule sa
présentation dans les résultats change.

Cette fonction est volontairement plus prudente que le correcteur d'anomalies, parce qu'aucun
crawl ne peut valider son résultat :

- **Le fichier cible vient uniquement de la carte des routes du dépôt.** Aucun choix de fichier
  par le modèle : s'il n'y a pas de correspondance certaine, la réécriture est refusée.
- **Les valeurs sont écrites par le modèle, et la pull request le dit.** Elle n'est jamais
  fusionnée automatiquement, même en mode application directe.
- **Une page dont le `title` est assemblé dynamiquement est refusée** plutôt que devinée.
- **Une seule pull request ouverte par page**, pas par requête : deux requêtes visant la même
  page réécrivent les deux mêmes lignes, et la seconde PR entrerait en conflit avec la
  première.

Prérequis : un [dépôt connecté](/docs/connecter-github). Une réécriture consomme une unité de
votre quota de corrections.

## Comment savoir si ça a marché

Pas par un crawl. Le crawler constatera que le texte a changé, ce qui ne prouve rien.

Ces tâches sont donc exclues de la
[vérification automatique](/docs/verifier-une-correction). Le verdict se lit dans Search
Console, trois à six semaines plus tard, sur deux chiffres : les clics et la position moyenne
de cette requête.

Gardez en tête le délai de publication de Google — deux à trois jours — et le temps qu'il lui
faut pour réexplorer la page.
