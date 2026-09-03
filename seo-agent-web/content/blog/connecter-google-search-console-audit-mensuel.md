---
slug: "connecter-google-search-console-audit-mensuel"
title: "Utiliser Google Search Console dans un audit SEO mensuel"
meta_title: "Google Search Console : audit SEO mensuel"
description: "Croiser les données Search Console avec un crawl technique pour décider quelles pages optimiser, au lieu de corriger dans l'ordre d'un rapport."
kind: "Tutoriel"
updated_at: "2026-09-03"
published_at: "2026-05-14"
audience: "Sites vitrines, blogs et e-commerce"
keywords: ["Google Search Console", "audit mensuel", "performance SEO", "requêtes SEO"]
featured: true
related: ["audit-seo-technique-checklist-priorites", "corriger-title-meta-description-grande-echelle"]
cta: "{{app_name}} fait ce croisement automatiquement : Search Console connectée, les anomalies se trient par impressions réelles et les requêtes mal exploitées deviennent des opportunités actionnables."
faq:
  - question: "Quelle période choisir dans Search Console ?"
    answer: "28 jours pour l'analyse courante : assez pour lisser les variations hebdomadaires, assez court pour voir une évolution récente. Comparez à la même période de l'année précédente plutôt qu'au mois précédent si votre activité est saisonnière."
  - question: "Pourquoi le total des clics par requête ne correspond-il pas au total global ?"
    answer: "Search Console filtre les requêtes rares pour des raisons de confidentialité, et déduplique différemment selon la dimension consultée. L'écart est normal ; ne bâtissez pas un calcul dessus."
  - question: "Search Console suffit-elle pour un audit SEO ?"
    answer: "Non. Elle dit ce que Google fait de vos pages, pas dans quel état elles sont. Elle ne voit ni une canonical incohérente, ni un lien interne cassé, ni une page orpheline. C'est le crawl qui apporte cette moitié."
---

Search Console dit ce que Google fait de votre site. Un crawl dit dans quel état il est. Aucun
des deux ne suffit ; c'est leur croisement qui produit un ordre de travail.

Voici la routine mensuelle, en quatre lectures.

## Lecture 1 : les pages qui perdent des impressions

Rapport Performances, dimension **Pages**, comparaison sur 28 jours contre les 28 précédents,
tri par variation d'impressions.

Une chute d'impressions veut dire que Google affiche moins la page. Trois causes possibles, et
le crawl tranche entre elles :

- la page a un problème d'indexabilité apparu récemment ;
- un concurrent est passé devant sur ses requêtes ;
- la demande a baissé, ce qui n'est pas votre problème.

Regardez les impressions **avant** les clics. Une page qui garde ses impressions et perd ses
clics n'a pas un problème de classement : elle a un problème de vitrine.

## Lecture 2 : les requêtes vues mais jamais cliquées

Dimension **Requêtes**, filtre position entre 3 et 10, tri par impressions décroissantes, et
regardez celles à zéro clic.

Vous êtes en première page et personne n'entre. Ce n'est pas un problème de classement, c'est
un `title` et une description qui ne correspondent pas à ce que le chercheur cherchait.

C'est la catégorie la plus rentable d'un audit : le travail est fait, seule la vitrine manque.

!!! note "Filtrez la longue traîne"
    En dessous de 50 impressions, un taux de clic ne veut rien dire. Deux impressions et un
    clic font 50 %, et cela ne signifie rien.

## Lecture 3 : les requêtes en position 8 à 20

L'écart est franchissable, contrairement à une position 45.

Ici la réponse est rarement une balise. C'est du contenu qui répond mieux à l'intention, du
maillage interne vers cette page, ou un lien externe.

Le piège classique : créer une nouvelle page sur la requête. Neuf fois sur dix, la bonne
décision est de renforcer celle qui se classe déjà — elle a un historique, elle est indexée,
et deux pages sur le même sujet finissent par se concurrencer.

## Lecture 4 : la requête ET la page ensemble

C'est la lecture que presque personne ne fait, et c'est la seule qui débouche sur une action
précise.

Dans le rapport Performances, croisez les deux dimensions : quelle **page** se classe sur
quelle **requête**. Une opportunité qui ne nomme que la requête vous laisse chercher la page ;
une qui nomme les deux se corrige.

Vous découvrirez souvent qu'une requête importante est portée par une page qui n'était pas
prévue pour ça. C'est une information de premier ordre : soit vous adaptez cette page, soit
vous comprenez pourquoi la page prévue ne sort pas.

## Ce que Search Console ne verra jamais

Elle ne voit pas :

- une `canonical` qui pointe au mauvais endroit ;
- un lien interne qui traverse une redirection ;
- une page orpheline que rien ne relie ;
- un `title` dupliqué sur trente pages ;
- une page importante enterrée à six clics de la home.

Toute cette moitié vient du crawl. C'est pourquoi la routine mensuelle utile est **un crawl
plus Search Console**, pas l'un ou l'autre.

## La routine, en pratique

Une fois par mois, dans cet ordre :

1. Lancer un crawl complet.
2. Exporter les pages avec leurs impressions et clics sur 28 jours.
3. Croiser les deux : ne garder que les anomalies sur des pages à impressions.
4. Traiter en priorité les pages « vues jamais cliquées » et les anomalies d'indexabilité.
5. Déployer, puis relancer un crawl pour vérifier.
6. Noter ce qui a été corrigé — c'est la base du rapport du mois suivant.

Et gardez en tête le décalage : Google publie ses données avec deux à trois jours de retard.
Une correction déployée hier ne se verra pas cette semaine, et une chute constatée aujourd'hui
a commencé avant-hier.
