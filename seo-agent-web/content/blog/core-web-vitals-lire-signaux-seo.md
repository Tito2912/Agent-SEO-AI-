---
slug: "core-web-vitals-lire-signaux-seo"
title: "Core Web Vitals : lire les signaux sans se perdre dans les scores"
meta_title: "Core Web Vitals : LCP, INP et CLS expliqués"
description: "Ce que mesurent vraiment LCP, INP et CLS, pourquoi le score sur 100 ne veut presque rien dire, et comment relier la performance au reste de l'audit."
kind: "Tutoriel"
updated_at: "2026-09-03"
published_at: "2026-05-10"
audience: "Développeurs, responsables produit et marketing"
keywords: ["Core Web Vitals", "LCP", "INP", "CLS", "performance web"]
related: ["audit-seo-technique-checklist-priorites", "regression-seo-apres-mise-en-production"]
cta: "{{app_name}} mesure les Core Web Vitals sur un échantillon de vos gabarits à chaque crawl, et les affiche à côté des impressions Search Console — pour trier les optimisations par le trafic qu'elles concernent."
faq:
  - question: "Un mauvais score PageSpeed fait-il chuter mon classement ?"
    answer: "Pas mécaniquement. Les signaux d'expérience de page pèsent peu face à la pertinence. Une page lente perd surtout des visiteurs qui abandonnent avant l'affichage, ce qui coûte plus cher que la position."
  - question: "Pourquoi mes scores varient-ils d'une mesure à l'autre ?"
    answer: "Parce que ce sont des mesures en laboratoire, sensibles à la charge du serveur et du réseau à l'instant du test. Suivez la tendance sur plusieurs mesures, jamais un point isolé."
  - question: "Faut-il viser 100 sur 100 ?"
    answer: "Non. C'est un objectif coûteux, souvent atteint au prix de compromis fonctionnels, et sans effet mesurable sur le trafic. Visez le seuil « bon » sur les trois métriques et arrêtez-vous là."
---

Les Core Web Vitals sont trois mesures, et le score global sur 100 n'en fait pas partie.
Retenir cette phrase évite l'essentiel des mauvaises décisions sur le sujet.

## Les trois mesures, une question chacune

### LCP — Largest Contentful Paint

**Combien de temps avant que le contenu principal soit visible ?**

Bon en dessous de 2,5 s, mauvais au-delà de 4 s.

Les causes réelles, par ordre de fréquence : une image de bannière non compressée ou non
dimensionnée, un serveur lent à répondre, une police web qui bloque le rendu, un script tiers
en tête de page.

### INP — Interaction to Next Paint

**Combien de temps entre un clic et la réaction visible de la page ?**

Bon en dessous de 200 ms.

Cette mesure a remplacé le FID, qui ne regardait que la première interaction — donc presque
toujours la meilleure. L'INP regarde l'ensemble, ce qui est nettement plus honnête.

La cause est presque toujours la même : du JavaScript qui monopolise le fil d'exécution.
Scripts tiers, analytics, gestionnaires de consentement, chat.

### CLS — Cumulative Layout Shift

**De combien la page bouge-t-elle pendant le chargement ?**

Bon en dessous de 0,1.

C'est la mesure du bouton qui se déplace au moment où on le clique. Trois coupables :
une image sans `width` et `height` déclarés, une police tardive qui change les dimensions du
texte, une bannière injectée en haut de page après le rendu.

C'est aussi le plus facile à corriger des trois, et celui qui améliore le plus l'expérience
réelle.

## Laboratoire contre terrain

Deux sources de données, souvent confondues, qui ne disent pas la même chose.

**Le laboratoire** — un test comme PageSpeed Insights : un appareil simulé, un réseau simulé,
à un instant donné. Reproductible, utile pour diagnostiquer, mais sensible aux conditions du
moment.

**Le terrain** — l'expérience de vos visiteurs réels, agrégée sur 28 jours. C'est cette source
que Google utilise pour ses signaux d'expérience de page.

Conséquence pratique : un test en laboratoire vous dit **quoi corriger**, les données de
terrain vous disent **si ça a marché**, et il faut attendre plusieurs semaines pour la seconde.

## Ce que le score sur 100 ne dit pas

Il agrège des mesures pondérées de façon arbitraire, dont certaines n'ont aucun rapport avec le
classement. Un site à 62 peut très bien se porter mieux qu'un site à 95.

Il varie de dix points d'un test à l'autre sans qu'aucune ligne de code ne change.

Et surtout : **il détourne l'attention**. On optimise le chiffre plutôt que l'expérience,
généralement en repoussant du travail — chargement différé sur des éléments visibles, scripts
retardés qui cassent une fonctionnalité.

Regardez LCP, INP et CLS. Ignorez le reste.

## Relier la performance au reste de l'audit

Une page lente qui n'a aucune impression n'est pas un chantier. Une page lente en position 4
avec 8 000 impressions, si.

C'est la seule priorisation qui tienne : croiser la performance avec les données de recherche,
et traiter les pages qui ont à la fois un problème et du trafic.

Et rappelez-vous l'ordre de grandeur : sur la plupart des sites, une `canonical` incohérente ou
une page importante en `noindex` coûte infiniment plus cher qu'un LCP à 3 secondes. La
performance est un chantier réel — c'est rarement le premier.

## Par où commencer, concrètement

1. Mesurer **une URL par gabarit**, pas vingt pages du même type.
2. Corriger le CLS d'abord : c'est le moins cher et le plus visible pour l'utilisateur.
3. Puis le LCP, en commençant par les images — c'est la cause dans la majorité des cas.
4. Puis l'INP, en auditant les scripts tiers. La question à poser pour chacun : qu'est-ce qu'on
   perd si on le retire ?
5. Remesurer, et regarder la tendance sur plusieurs semaines, pas le prochain test.
