---
title: "Anomalies : les lire, les trier, décider"
meta_title: "Anomalies et priorités — documentation {{app_name}}"
description: "Comment {{app_name}} classe les anomalies détectées, ce que contient la fiche détaillée d'une anomalie, et dans quel ordre les traiter."
kind: "Documentation"
section: "Corriger"
order: 20
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["anomalies SEO", "priorisation", "erreurs de crawl", "gravité"]
app_href: "/"
related: ["corrections-automatiques", "lire-le-rapport", "connecter-search-console"]
faq:
  - question: "Pourquoi une anomalie touche-t-elle 300 URLs alors que je n'ai que 5 pages différentes ?"
    answer: "Parce qu'elle vient d'un gabarit, pas d'une page. C'est une bonne nouvelle : une correction dans le template règle les 300 lignes d'un coup, et c'est exactement ce que la correction automatique cherche à faire."
  - question: "Puis-je ignorer une anomalie définitivement ?"
    answer: "Oui. Une anomalie peut être marquée « ignorée » : elle sort de la liste de travail et de vos statistiques de corrections, sans disparaître du rapport de crawl."
  - question: "Toutes les anomalies sont-elles corrigeables automatiquement ?"
    answer: "Non. Celles qui se règlent dans le code — balises, canonicals, alt, liens internes, redirections en boucle, sitemap — le sont. Celles qui demandent une décision éditoriale ou serveur ne le sont pas, et le bouton de correction ne s'affiche pas."
---

La page **Anomalies** d'un projet est la liste de travail. Chaque ligne est un *type* de
problème, pas une page : « meta description manquante » apparaît une fois, avec le nombre
d'URLs concernées.

C'est délibéré. Un audit qui liste 1 900 lignes est illisible ; un audit qui liste 42 types
dont l'un touche 1 900 URLs est actionnable.

## Ce que contient la fiche d'une anomalie

Cliquez sur une ligne pour ouvrir sa fiche :

- **Ce que c'est**, en français, sans jargon inutile ;
- **Pourquoi ça compte** — l'effet réel sur l'exploration, l'indexation ou le clic ;
- **Les URLs touchées**, avec pour chacune la valeur constatée (le `title` en double, la
  `canonical` qui pointe ailleurs, l'ancre vide) ;
- **La preuve** — ce que le crawler a exactement vu, pour que vous puissiez le vérifier ;
- **Les actions** : générer une suggestion, ouvrir une pull request, exporter, ignorer.

Cette colonne « valeur constatée » est ce qui distingue un audit utilisable d'une liste
d'URLs. Elle vous évite d'ouvrir trente pages pour comprendre le motif.

## Les trois niveaux de gravité

**Erreur** — ça empêche l'indexation ou ça casse quelque chose. Réponses 5xx, page importante
en 404, `noindex` non voulu, boucle de redirection, canonical incohérente.

**Avertissement** — ça dégrade sans bloquer. `title` dupliqué, `h1` absent, lien interne
pointant vers une redirection, image sans `alt`, profondeur excessive.

**Remarque** — c'est noté, rarement urgent. Description manquante sur une page secondaire,
URL très longue, ancre générique.

## L'ordre dans lequel travailler

La gravité technique est un tri par défaut, pas un plan d'action. Le bon ordre croise trois
questions.

### 1. La page rapporte-t-elle quelque chose ?

C'est le filtre le plus discriminant, et il exige Search Console. Une anomalie sur une page à
12 000 impressions et la même sur une archive à zéro impression ne méritent pas la même
journée de travail.

→ [Connecter Search Console](/docs/connecter-search-console)

### 2. Est-ce que ça bloque l'indexation ?

Tant qu'une page ne peut pas être indexée, tout le reste du travail dessus est perdu. Dans
l'ordre : statuts HTTP, `robots.txt`, `noindex`, `canonical`, redirections.

### 3. Combien d'URLs pour une correction ?

Une anomalie à 300 occurrences venant d'un gabarit se corrige en une fois. Trois anomalies à
une occurrence chacune demandent trois interventions. Le rendement n'est pas le même.

!!! note "L'ordre pratique qui marche"
    1. Erreurs d'indexabilité sur les pages business.
    2. Anomalies de gabarit à fort volume.
    3. Signaux SERP (`title`, description, `h1`) sur les pages à impressions.
    4. Maillage interne : liens cassés, liens vers des redirections, pages orphelines.
    5. Le reste, à la relance suivante.

## Suivi : todo, en cours, fait, ignoré

Chaque anomalie peut devenir une **tâche** avec un statut. La page **Corrections** du projet
les regroupe par colonne, et affiche pour chacune la pull request associée quand il y en a
une, ainsi que le résultat de la vérification post-crawl.

C'est ce qui permet de reprendre un audit deux semaines plus tard sans se demander ce qui a
déjà été traité.

## Suggestions et corrections

Deux niveaux d'aide, selon ce que vous avez connecté :

- **Suggestion** — {{app_name}} propose le contenu à écrire : un `title` de la bonne longueur
  qui reprend l'intention de la page, une description, un `alt`. Vous copiez-collez.
- **Correction** — {{app_name}} écrit la modification directement dans votre code et ouvre une
  pull request. Cela demande un dépôt connecté.

→ [Corrections automatiques](/docs/corrections-automatiques)

Sans dépôt Git, le [fix-pack](/docs/fix-pack-sans-depot) rassemble les correctifs dans une
archive prête à appliquer.
