---
title: "Ajouter un projet"
meta_title: "Ajouter un site à auditer — documentation {{app_name}}"
description: "Créer un projet dans {{app_name}} : URL de départ, sous-domaines, sites multilingues, environnements de préproduction et suppression."
kind: "Documentation"
section: "Prise en main"
order: 11
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["projet", "ajouter un site", "domaine", "sous-domaine"]
app_href: "/"
related: ["demarrer", "lancer-un-crawl", "plans-et-quotas"]
faq:
  - question: "Puis-je auditer un site en préproduction protégé par mot de passe ?"
    answer: "Pas via une authentification HTTP interactive. Une préproduction accessible par une URL secrète non protégée fonctionne, à condition qu'elle réponde en 200 et ne bloque pas le crawler dans robots.txt."
  - question: "Un sous-domaine compte-t-il comme un projet séparé ?"
    answer: "Par défaut oui : le crawl reste sur l'hôte de l'URL de départ. Vous pouvez activer « Autoriser les sous-domaines » dans les paramètres de crawl pour couvrir blog.example.com depuis le projet example.com."
  - question: "Que se passe-t-il si je supprime un projet ?"
    answer: "Le projet, ses crawls, ses anomalies et ses tâches de correction sont supprimés. Les pull requests déjà ouvertes sur votre dépôt GitHub ne le sont pas : elles vous appartiennent."
---

Un projet représente **un site**. Il porte l'URL de départ, les réglages de crawl, l'historique
des audits, les anomalies, les tâches de correction et les connexions (Search Console, dépôt
Git) qui lui sont propres.

## Créer le projet

Depuis le tableau de bord, **Ajouter un site**. Un seul champ obligatoire : l'URL de départ.

Quelques règles qui évitent les mauvaises surprises :

- **Donnez l'URL canonique, avec le bon protocole et le bon préfixe.** `https://example.com`
  et `https://www.example.com` ne sont pas le même hôte pour le crawler. Si votre site
  redirige l'un vers l'autre, partez de la destination, pas de l'origine — sinon le premier
  saut de chaque crawl est une redirection.
- **Partez de la home**, sauf raison précise. Le crawler suit les liens : une URL de départ
  profonde ne verra jamais les sections qui ne sont liées que depuis l'accueil.
- **Un projet par domaine.** Les quotas de pages, les scores et l'historique n'ont de sens que
  cloisonnés.

Le nombre de projets simultanés dépend de votre plan — voir [Plans et quotas](/docs/plans-et-quotas).

## Sites multilingues

Un site en `example.com/fr/` et `example.com/en/` reste **un seul projet** : les deux versions
partagent le même hôte, et les contrôles `hreflang` n'ont de sens que si le crawler voit les
deux côtés de la relation. Séparer les langues en deux projets casse la détection des balises
`hreflang` réciproques manquantes.

En revanche, `example.fr` et `example.com` sont deux hôtes, donc deux projets.

## Sous-domaines

Par défaut, le crawl ne sort pas de l'hôte de départ : un lien vers `blog.example.com` depuis
`example.com` est traité comme un lien externe (vérifié, mais pas exploré).

Pour couvrir les sous-domaines dans le même audit, activez **Autoriser les sous-domaines**
dans les [paramètres de crawl](/docs/lancer-un-crawl) du projet. Attention au volume : un blog
sur sous-domaine peut doubler le nombre de pages, et donc la consommation de votre quota
mensuel.

## Préproduction et environnements de test

{{app_name}} refuse par défaut les hôtes privés (`localhost`, `127.0.0.1`, adresses de réseau
local). C'est une protection contre les requêtes vers l'infrastructure interne, pas une
limitation cosmétique.

Une préproduction publique — une URL Netlify de prévisualisation, un sous-domaine `staging.` —
s'audite normalement. Vérifiez simplement deux choses :

1. Elle ne renvoie pas un `X-Robots-Tag: noindex` global, sans quoi le rapport vous signalera
   à juste titre que toutes les pages sont non indexables.
2. Son `robots.txt` n'interdit pas tout. Si c'est le cas et que vous voulez quand même
   l'auditer, l'option **Ignorer robots.txt** existe dans les paramètres de crawl — à
   n'utiliser que sur un site dont vous êtes responsable.

## Supprimer un projet

Le nom du projet se choisit à la création. La suppression, elle, est définitive : elle emporte
l'historique des crawls, les rapports, les anomalies et les tâches de correction du projet.

Ce qu'elle **ne** supprime pas : les pull requests déjà ouvertes sur votre dépôt, les
correctifs déjà fusionnés, et les exports que vous avez téléchargés.

## Après la création

Le projet apparaît dans la barre latérale avec ses propres pages : vue d'ensemble, paramètres
de crawl, performance, mots-clés, concurrents, backlinks, anomalies, corrections, automatisation
et historique. Toutes sont vides tant que le premier crawl n'a pas tourné.

→ [Lancer un crawl](/docs/lancer-un-crawl)
