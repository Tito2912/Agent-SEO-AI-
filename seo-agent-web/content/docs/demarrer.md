---
title: "Démarrer avec {{app_name}} en 15 minutes"
meta_title: "Prise en main — documentation {{app_name}}"
description: "Le parcours complet du premier compte au premier correctif : créer un projet, lancer un crawl, lire le rapport, connecter Search Console et GitHub."
kind: "Prise en main"
section: "Prise en main"
order: 10
updated_at: "2026-09-03"
audience: "Nouveau compte"
keywords: ["prise en main", "démarrer", "premier crawl", "onboarding"]
app_href: "/"
related: ["ajouter-un-projet", "lancer-un-crawl", "lire-le-rapport", "corrections-automatiques"]
faq:
  - question: "Faut-il connecter GitHub pour utiliser {{app_name}} ?"
    answer: "Non. L'audit, le rapport, les anomalies et les exports fonctionnent sans aucune connexion. GitHub n'est nécessaire que pour la correction automatique du code en pull request. Sans dépôt, vous pouvez télécharger un fix-pack et appliquer les correctifs à la main."
  - question: "Combien de temps dure un premier crawl ?"
    answer: "De quelques minutes pour un site vitrine à plusieurs heures pour un gros catalogue. Le crawl s'exécute en arrière-plan : vous pouvez fermer l'onglet, le job continue et la page Jobs affiche l'avancement."
  - question: "Le crawl peut-il ralentir mon site ?"
    answer: "Le crawler respecte robots.txt et la directive crawl-delay, et limite le nombre de requêtes simultanées par hôte. Vous pouvez encore réduire la cadence dans les paramètres de crawl du projet si votre hébergement est fragile."
---

Cette page suit l'ordre réel des choses : chaque étape débloque la suivante. Comptez un
quart d'heure de manipulation, plus le temps du crawl qui, lui, tourne sans vous.

La carte de progression affichée sur le tableau de bord suit exactement ces étapes et coche
celles qui sont faites.

## Les trois étapes indispensables

### 1. Ajouter le site à auditer

Depuis le tableau de bord, bouton **Ajouter un site**. Une URL de départ suffit —
`https://example.com`. {{app_name}} en déduit le nom du projet, le domaine autorisé et
l'adresse de départ du crawl.

Un projet = un site. Si vous gérez plusieurs domaines, créez un projet par domaine : les
rapports, les quotas de pages et les corrections sont cloisonnés par projet.

Détails et cas particuliers : [Ajouter un projet](/docs/ajouter-un-projet).

### 2. Lancer le premier crawl

Sur la page du projet, bouton **Lancer un crawl**. Vous n'avez rien à régler pour le premier
passage : les valeurs par défaut conviennent à la quasi-totalité des sites.

Le crawl part en tâche de fond. Suivez-le sur **Jobs**, ou revenez plus tard : vous recevez
le rapport dans tous les cas.

Ce que le crawler collecte, et comment le brider : [Lancer un crawl](/docs/lancer-un-crawl).

### 3. Lire le rapport et traiter les anomalies

À la fin du crawl, la page du projet affiche les scores, les pages analysées et la liste des
anomalies. C'est là que le travail commence vraiment.

N'essayez pas de tout corriger. La page **Anomalies** trie par gravité et par nombre d'URLs
touchées : quelques corrections de gabarit règlent souvent des centaines de lignes.

Comment prioriser : [Lire le rapport](/docs/lire-le-rapport) et
[Anomalies et priorités](/docs/anomalies-et-priorites).

## Les deux connexions qui changent tout

Elles sont facultatives. Elles transforment aussi le produit d'un outil d'audit en un outil
qui corrige.

### Search Console

Sans elle, {{app_name}} sait ce que votre site *contient*. Avec elle, il sait ce que Google
en *fait* : impressions, clics, position moyenne, requêtes, page par page.

C'est ce qui rend les priorités crédibles. Une meta description manquante sur une page à
12 000 impressions et une sur une archive à zéro impression cessent d'être le même problème.

→ [Connecter Search Console](/docs/connecter-search-console)

### GitHub

C'est la fonction centrale du produit. Une fois le dépôt du site connecté, {{app_name}} ne se
contente plus de signaler une anomalie : il écrit le correctif dans votre code, ouvre une
**pull request**, et vérifie au crawl suivant que le problème a réellement disparu.

Vous gardez la main : rien n'est fusionné sans vous.

→ [Connecter GitHub](/docs/connecter-github) puis
[Corrections automatiques](/docs/corrections-automatiques)

!!! note "Pas de dépôt Git ?"
    Site sous WordPress, Wix, Shopify, ou dépôt inaccessible : le
    [fix-pack](/docs/fix-pack-sans-depot) exporte les correctifs prêts à coller, avec le
    mode d'emploi. Vous perdez l'automatisation, pas les corrections.

## Ce que vous pouvez faire ensuite

| Vous voulez… | Allez voir |
| --- | --- |
| Savoir quelles pages retravailler en priorité | [Opportunités de mots-clés](/docs/opportunites-de-mots-cles) |
| Comprendre vos Core Web Vitals | [Suivre la performance](/docs/suivre-la-performance) |
| Voir les sujets que vos concurrents traitent et pas vous | [Analyser les concurrents](/docs/analyser-les-concurrents) |
| Travailler vos backlinks | [Backlinks](/docs/backlinks) |
| Ne plus lancer les crawls à la main | [Automatiser les audits](/docs/automatiser-les-audits) |
| Sortir un rapport pour un client ou un dirigeant | [Exports et rapports](/docs/exports-et-rapports) |
| Connaître vos limites de plan | [Plans et quotas](/docs/plans-et-quotas) |

## L'assistant, si vous êtes bloqué

Le bouton **Assistant IA** en bas à droite de l'application connaît le contexte de la page
que vous consultez. Posez-lui la question dans vos mots — « pourquoi cette page est en
noindex ? », « qu'est-ce qu'une canonical ? » — plutôt que de chercher dans la
documentation.

Le nombre de messages est compté dans votre quota mensuel, variable selon le plan.
