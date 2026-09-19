---
title: "Analyser les concurrents"
meta_title: "Analyse de concurrents — documentation {{app_name}}"
description: "Crawler un site rival, comparer les sujets qu'il traite aux vôtres, et lire les sujets non couverts sans se tromper d'interprétation."
kind: "Documentation"
section: "Aller plus loin"
order: 40
updated_at: "2026-09-03"
audience: "Pro et Business"
keywords: ["concurrents", "analyse concurrentielle", "sujets non couverts", "content gap"]
app_href: "/"
related: ["opportunites-de-mots-cles", "backlinks", "plans-et-quotas"]
faq:
  - question: "Pourquoi seulement 100 pages par concurrent ?"
    answer: "Parce que ce crawl explore un site qui n'est pas le vôtre. Cent pages suffisent à cartographier les sujets d'un site, et la limite garantit que l'analyse reste un échantillonnage poli plutôt qu'une aspiration."
  - question: "{{app_name}} corrige-t-il mon site à partir des concurrents ?"
    answer: "Non. Les sujets non couverts sont rapportés, jamais transformés en correction automatique. Décider d'écrire une page est un choix éditorial, et personne ne devrait le déléguer à une machine."
  - question: "À quelle fréquence les concurrents sont-ils réanalysés ?"
    answer: "Une fois par mois automatiquement — un site rival ne se republie pas toutes les semaines. Vous pouvez déclencher une analyse à la demande depuis la page quand vous savez qu'il a bougé."
---

*Disponible à partir du plan Pro.*

La question à laquelle cette page répond : **de quoi parlent vos concurrents que vous ne
traitez pas ?**

Pas « quel est leur score », ni « combien ont-ils de backlinks ». De quoi ils parlent — parce
que c'est la seule information concurrentielle qui se transforme directement en travail.

## Ajouter un concurrent

Page **Concurrents** d'un projet, champ URL. Jusqu'à **5 concurrents** par projet.

Chaque ajout déclenche un crawl du site rival, plafonné à **100 pages**, à cadence réduite.
Cette limite n'est pas une contrainte technique : c'est la différence entre échantillonner un
site et l'aspirer. Cent pages suffisent à cartographier les sujets d'un site ; au-delà, on
répète.

L'analyse tourne en tâche de fond ; la page affiche l'état de chaque concurrent : en attente,
en cours, prêt, ou en erreur avec la raison.

## Lire la comparaison

Le produit extrait les sujets de chaque page — de son URL, de son `title`, de ses titres — des
deux côtés, puis les rapproche.

Vous obtenez deux familles de résultats, et **les sujets non couverts sont affichés en
premier**.

### Sujets non couverts

Un thème que le concurrent traite et que rien chez vous n'aborde.

C'est la trouvaille intéressante, et c'est aussi celle sur laquelle le produit n'agira pas :
écrire une page est une décision éditoriale. {{app_name}} vous la signale et s'arrête là.

Lisez-la avec du recul. Un concurrent peut publier sur un sujet parce que c'est son métier et
pas le vôtre, ou simplement parce qu'il se trompe. Un vide n'est pas toujours un manque.

### Sujets couverts des deux côtés

Vous avez une page sur le sujet, lui aussi. La comparaison indique la vôtre.

C'est là que le travail est le plus rentable : la page existe, elle est indexée, elle a un
historique. La renforcer coûte infiniment moins cher que d'en créer une.

Croisez avec les [opportunités de mots-clés](/docs/opportunites-de-mots-cles) : si cette page
est en position 12 sur la requête du sujet, vous tenez à la fois le quoi et le pourquoi.

## Les quatre états de la page

La page distingue explicitement quatre situations, plutôt que d'afficher un tableau vide dans
trois d'entre elles :

1. **Pas d'accès** — votre plan ne couvre pas la fonction.
2. **Aucun concurrent ajouté** — il faut commencer par en ajouter un.
3. **Concurrent ajouté, jamais analysé** — le crawl n'a pas encore tourné.
4. **Comparaison disponible** — le seul cas où un tableau a un sens.

Ce n'est pas un détail d'interface : ces quatre états appellent quatre actions différentes, et
un tableau vide les confondrait toutes.

## Rafraîchissement

Les concurrents sont réanalysés automatiquement **une fois par mois**. Un site rival ne se
republie pas toutes les semaines, et un crawl mensuel suffit à voir arriver ses nouvelles
sections.

Vous pouvez relancer une analyse à la demande depuis la page — quand vous savez qu'un
concurrent vient de refondre son site, par exemple.

## Ce que cette page ne fait pas

- **Elle n'estime pas leur trafic.** Aucune donnée réelle de leur Search Console n'est
  accessible, et une estimation présentée comme un fait serait trompeuse.
- **Elle n'analyse pas leurs backlinks.** C'est le rôle de la page [Backlinks](/docs/backlinks),
  avec vos propres sources de données.
- **Elle ne déclenche aucune correction.** Rien de ce qui vient d'un site tiers n'entre dans le
  correcteur automatique.
