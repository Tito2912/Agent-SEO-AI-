---
title: "Lire le rapport de crawl"
meta_title: "Comprendre le rapport d'audit — documentation {{app_name}}"
description: "Ce que signifient les scores, les compteurs de pages, la comparaison entre deux crawls et les sections de la vue d'ensemble d'un projet."
kind: "Documentation"
section: "Prise en main"
order: 13
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["rapport SEO", "score", "vue d'ensemble", "comparaison de crawls"]
app_href: "/"
related: ["anomalies-et-priorites", "lancer-un-crawl", "exports-et-rapports"]
faq:
  - question: "Mon score a baissé alors que je n'ai rien changé. C'est possible ?"
    answer: "Oui, et c'est presque toujours explicable : de nouvelles pages ont été publiées et portent des anomalies, un crawl précédent était partiel, ou une ressource externe liée est tombée. Comparez les deux crawls plutôt que les deux scores."
  - question: "À quoi sert la comparaison entre deux crawls ?"
    answer: "À voir les mouvements plutôt que les états : anomalies apparues, anomalies résolues, pages ajoutées ou disparues. C'est la seule vue qui prouve qu'une correction a fonctionné."
  - question: "Combien de temps les crawls sont-ils conservés ?"
    answer: "L'historique du projet conserve les crawls successifs, consultables et comparables depuis la page Crawls. Supprimer le projet les supprime."
---

À la fin d'un crawl, la page du projet devient la vue d'ensemble : ce que le site contient, ce
qui cloche, et ce qui a bougé depuis la dernière fois.

## Les scores

Les scores résument l'état du site par famille : indexabilité, balises, contenu, maillage
interne, performance. Ils servent à **une** chose : repérer d'un coup d'œil la famille qui
s'effondre.

Ne les prenez pas pour une note Google. Aucun moteur ne calcule ce chiffre, et un site à 92
peut être invisible pendant qu'un site à 61 se porte très bien. Le score est un thermomètre
interne, utile pour suivre **votre propre** tendance dans le temps.

Ce qui compte vraiment est en dessous : le nombre d'URLs concernées par chaque anomalie.

## Les compteurs de pages

- **Pages explorées** — ce que le crawler a réellement atteint.
- **Pages indexables** — celles qui répondent 200, ne sont pas en `noindex`, et dont la
  `canonical` pointe vers elles-mêmes.
- **Pages du sitemap** — et surtout l'écart avec les précédentes.

Les trois chiffres racontent l'histoire ensemble. 800 pages explorées pour 340 indexables,
c'est un problème d'indexabilité. 800 explorées et 1 400 au sitemap, c'est un sitemap qui
promet des pages que le maillage n'atteint pas.

## Les anomalies

La liste triée par gravité, chaque ligne portant le nombre d'URLs touchées.

{{app_name}} suit un catalogue d'anomalies aligné sur les standards du marché, réparti en
trois niveaux :

| Niveau | Ce que ça veut dire | Exemples |
| --- | --- | --- |
| **Erreur** | Empêche l'indexation ou casse quelque chose | 5xx, page importante en 404, `noindex` sur une page business, boucle de redirection |
| **Avertissement** | Dégrade la compréhension ou l'exploration | `title` dupliqué, `h1` absent, lien interne vers une redirection, image sans `alt` |
| **Remarque** | Bon à savoir, rarement urgent | description manquante sur une archive, URL très longue, page profonde |

Le tri par gravité est un point de départ, pas un ordre de travail. Une remarque sur votre
page tarifs passe avant une erreur sur une page d'archive de 2019.

→ [Anomalies et priorités](/docs/anomalies-et-priorites)

## La comparaison entre deux crawls

C'est la vue la plus utile du produit, et la plus ignorée.

Sélectionnez un crawl de référence : la vue d'ensemble affiche alors les **deltas** —
anomalies apparues, anomalies résolues, pages gagnées ou perdues, évolution par famille.

Deux usages :

1. **Après une correction.** La seule preuve qu'un correctif a fonctionné est que l'anomalie
   a disparu au crawl suivant. C'est aussi ce que {{app_name}} vérifie automatiquement pour
   les corrections qu'il a écrites — voir [Vérifier une correction](/docs/verifier-une-correction).
2. **Après une mise en production.** Un déploiement qui ajoute 40 anomalies est une régression,
   même si personne ne s'en aperçoit dans le navigateur.

## Les données de recherche

Si Search Console est connectée, la vue d'ensemble intègre les impressions, clics et positions
réelles, et les anomalies deviennent priorisables par trafic. Sans elle, {{app_name}} ne peut
trier que par gravité technique, ce qui est nettement moins pertinent.

→ [Connecter Search Console](/docs/connecter-search-console)

## Sortir le rapport de l'écran

Exports CSV et PDF depuis la vue d'ensemble et depuis la page Anomalies : rapport de synthèse,
liste complète des anomalies, liste des URLs pour une anomalie donnée.

→ [Exports et rapports](/docs/exports-et-rapports)
