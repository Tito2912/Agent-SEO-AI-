---
title: "Plans et quotas"
meta_title: "Plans, limites et quotas — documentation {{app_name}}"
description: "Ce que chaque plan {{app_name}} inclut : projets, pages crawlées, corrections IA, Core Web Vitals, et comment chaque quota se compte réellement."
kind: "Référence"
section: "Compte et facturation"
order: 50
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["tarifs", "quotas", "limites", "abonnement"]
app_href: "/billing"
related: ["corrections-automatiques", "lancer-un-crawl", "analyser-les-concurrents"]
faq:
  - question: "Comment se compte une correction IA ?"
    answer: "Un fichier écrit par le modèle égale une unité. Une pull request qui modifie un seul gabarit et règle 300 URLs coûte donc une unité — le quota mesure le travail du modèle, pas le nombre de lignes du rapport."
  - question: "Que se passe-t-il quand j'atteins un quota ?"
    answer: "L'action concernée est refusée avec un message qui nomme le quota atteint, et un lien vers la page Abonnement. Rien d'autre ne s'arrête : un quota de corrections épuisé ne bloque ni les crawls, ni les rapports, ni les exports."
  - question: "Les quotas se reportent-ils d'un mois sur l'autre ?"
    answer: "Non. Ils sont mensuels et repartent de zéro à chaque période de facturation."
  - question: "Puis-je changer de plan en cours de mois ?"
    answer: "Oui, depuis la page Abonnement. Une montée de plan prend effet immédiatement ; une descente est programmée pour la fin de la période en cours et reste visible d'ici là."
---

Les chiffres de cette page sont ceux réellement appliqués par le produit, et non une copie du
tarif : ils sont lus depuis la configuration en vigueur au moment où vous consultez cette
page.

## Le tableau

| | Free | Solo | Pro | Business |
| --- | --- | --- | --- | --- |
| **Prix** | {{price_free}} | {{price_solo}} | {{price_pro}} | {{price_business}} |
| **Projets** | {{projects_free}} | {{projects_solo}} | {{projects_pro}} | {{projects_business}} |
| **Pages crawlées / mois** | {{pages_free}} | {{pages_solo}} | {{pages_pro}} | {{pages_business}} |
| **Pages max / crawl** | {{maxpages_free}} | {{maxpages_solo}} | {{maxpages_pro}} | {{maxpages_business}} |
| **Corrections IA / mois** | {{corrections_free}} | {{corrections_solo}} | {{corrections_pro}} | {{corrections_business}} |
| **Fichiers max / correction** | {{files_free}} | {{files_solo}} | {{files_pro}} | {{files_business}} |
| **URLs Core Web Vitals / crawl** | {{pagespeed_free}} | {{pagespeed_solo}} | {{pagespeed_pro}} | {{pagespeed_business}} |
| **Messages assistant / mois** | {{assistant_free}} | {{assistant_solo}} | {{assistant_pro}} | {{assistant_business}} |
| **Opportunités backlinks** | — | ✓ | ✓ | ✓ |
| **Concurrents** | — | — | ✓ | ✓ |

## Comment chaque quota se compte

### Corrections IA — la règle qui compte

**Une unité = un fichier écrit par le modèle.**

C'est la seule règle, et elle a une conséquence qu'il vaut la peine de comprendre : la
correction qui règle 300 URLs en modifiant un seul gabarit coûte **une** unité. Le quota
mesure le travail du modèle, pas la taille du problème.

En pratique, un site de taille moyenne consomme quelques dizaines d'unités le premier mois —
celui où l'on rattrape le retard — puis nettement moins ensuite, une fois les anomalies de
gabarit réglées.

Le plan Free n'inclut pas de correction IA. Il inclut l'audit complet, les suggestions, les
exports et le fix-pack : de quoi corriger à la main.

### Fichiers par correction

Le plafond de fichiers qu'une seule correction peut toucher. Il protège contre la pull request
de quarante fichiers que personne ne relira.

Ce plafond est aussi ramené au quota restant : s'il vous reste 3 unités, une correction qui
voudrait toucher 12 fichiers s'arrête à 3.

### Pages crawlées

Le total mensuel de pages HTML explorées, tous projets confondus. Les ressources — images, CSS,
scripts — n'y entrent pas, sauf si vous activez leur vérification, et elles sont alors
plafonnées séparément.

Le plafond réel d'un crawl est le plus petit de trois nombres : la limite du projet, la limite
du plan par crawl, et ce qu'il reste du quota mensuel.

### URLs Core Web Vitals

Le nombre d'URLs mesurées par PageSpeed à chaque crawl. La limite vient de l'API de Google,
qui est lente et contingentée par jour, et cette limite est partagée par toute la plateforme.

Choisissez cet échantillon : une URL par gabarit vaut mieux que vingt pages du même type.

→ [Suivre la performance](/docs/suivre-la-performance)

### Concurrents

À partir du plan Pro. Jusqu'à {{competitors_max}} concurrents par projet,
{{competitor_pages}} pages explorées par concurrent, et un rafraîchissement automatique tous
les {{competitor_refresh_days}} jours.

Le seuil est plus haut que pour les autres modules parce qu'un crawl de concurrent dépense du
temps de traitement sur un site qui n'est pas le vôtre.

→ [Analyser les concurrents](/docs/analyser-les-concurrents)

### Opportunités de backlinks

À partir du plan Solo, avec deux quotas mensuels distincts : les recherches et les brouillons
de messages. Ils sont volontairement bas — le netlinking utile se compte en quelques contacts
pertinents, pas en centaines d'envois.

→ [Backlinks](/docs/backlinks)

## Ce qui n'est jamais limité

- La lecture des rapports et de l'historique.
- Tous les exports CSV et PDF, et le fix-pack.
- Les suggestions de contenu que vous copiez-collez vous-même.
- La consultation des anomalies et de leurs URLs.

## Atteindre un quota

L'action concernée est refusée avec un message qui nomme le quota atteint et pointe vers la
page **Abonnement**. Le reste continue de fonctionner : un quota de corrections épuisé
n'empêche ni les crawls, ni les rapports, ni les exports.

Les quotas sont mensuels et ne se reportent pas.

## Changer de plan

Page **Abonnement**. Une montée de plan prend effet immédiatement. Une descente est programmée
pour la fin de la période en cours et reste affichée jusque-là — vous gardez donc ce que vous
avez payé jusqu'au bout.
