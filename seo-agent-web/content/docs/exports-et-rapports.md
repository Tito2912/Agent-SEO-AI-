---
title: "Exports et rapports"
meta_title: "Exports CSV et PDF — documentation {{app_name}}"
description: "Tous les exports disponibles dans {{app_name}} : rapport de synthèse, anomalies, URLs par anomalie, fix-pack, et lequel choisir selon le destinataire."
kind: "Documentation"
section: "Aller plus loin"
order: 43
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["export CSV", "rapport PDF", "audit client", "reporting SEO"]
app_href: "/"
related: ["lire-le-rapport", "fix-pack-sans-depot", "anomalies-et-priorites"]
faq:
  - question: "Les exports consomment-ils un quota ?"
    answer: "Non. Ils sont construits à partir de données déjà collectées par le crawl. Seules les corrections écrites par le modèle consomment un quota."
  - question: "Puis-je exporter plusieurs crawls pour les comparer ?"
    answer: "Chaque export porte sur un crawl. Pour comparer, exportez les deux et rapprochez-les dans un tableur — ou utilisez la comparaison intégrée à la vue d'ensemble, qui fait le travail à l'écran."
  - question: "Le PDF est-il présentable à un client ?"
    answer: "Oui, c'est sa raison d'être : structuré, lisible, sans jargon inutile. Le CSV, lui, est fait pour être trié et filtré, pas pour être envoyé."
---

Deux formats, deux usages qu'il ne faut pas confondre : le **CSV** se trie et se filtre, le
**PDF** se transmet.

Aucun export ne consomme de quota : tout est construit à partir de données déjà collectées.

## Les exports d'un projet

### Rapport de synthèse — CSV et PDF

L'état du site sur le crawl choisi : scores par famille, compteurs de pages, anomalies
principales avec leur volume.

C'est le document à envoyer à quelqu'un qui ne va pas ouvrir l'application — un client, un
dirigeant, un prestataire. Prenez le PDF.

### Liste des anomalies — CSV et PDF

Toutes les anomalies détectées, avec leur type, leur gravité, leur catégorie, le nombre d'URLs
touchées, et l'estimation de priorité et d'effort.

Le CSV est le bon choix ici : dans un tableur, vous triez par volume, vous filtrez par gravité,
et vous obtenez votre plan de travail en trente secondes.

### URLs par anomalie — CSV et PDF

Pour **une** anomalie donnée, la liste complète des URLs concernées avec la valeur constatée
sur chacune — le `title` en double, la `canonical` qui pointe ailleurs, l'`alt` manquant.

C'est l'export à donner à la personne qui va corriger à la main. Il contient exactement ce
qu'il faut et rien d'autre.

### Toutes les URLs, toutes les anomalies — CSV

L'export exhaustif : chaque couple URL / anomalie sur une ligne.

Volumineux par nature, et fait pour être traité par un outil, pas lu. Utile pour un
rapprochement avec vos propres données — un export de CMS, une liste de pages prioritaires.

### Fix-pack — ZIP

L'archive des correctifs prêts à appliquer, avec le mode d'emploi par plateforme. C'est la
voie principale quand le site n'a pas de dépôt Git connecté.

→ [Fix-pack sans dépôt](/docs/fix-pack-sans-depot)

## Choisir selon le destinataire

| Destinataire | Format | Ce qu'il en fera |
| --- | --- | --- |
| Client, dirigeant | Rapport de synthèse **PDF** | Le lire, comprendre l'état, décider d'un budget |
| Développeur | URLs par anomalie **CSV** | Corriger, ligne par ligne |
| Vous, pour planifier | Liste des anomalies **CSV** | Trier, filtrer, prioriser |
| Un site sans dépôt | **Fix-pack ZIP** | Appliquer les correctifs à la main |
| Une analyse externe | Toutes les URLs **CSV** | Croiser avec d'autres données |

## Ce qu'un rapport transmis devrait contenir

Un export brut n'est pas un rapport. Si vous facturez cet audit, ajoutez trois choses que
{{app_name}} ne peut pas deviner :

1. **Un résumé en quelques lignes** — l'état général et le seul chiffre qui compte.
2. **Ce que vous recommandez de faire, dans l'ordre**, avec l'effort estimé. Le produit vous
   donne le volume et la gravité ; l'arbitrage business est le vôtre.
3. **Ce qui a été corrigé depuis le rapport précédent**, avec la preuve — c'est-à-dire la
   comparaison entre deux crawls.

Cette troisième partie est celle qui justifie un abonnement récurrent auprès d'un client.
C'est aussi la seule que les outils concurrents ne produisent pas toute seule.

→ [Lire le rapport](/docs/lire-le-rapport)
