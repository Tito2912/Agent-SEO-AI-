---
title: "Connecter Google Search Console"
meta_title: "Connecter Search Console — documentation {{app_name}}"
description: "Relier Search Console à un projet {{app_name}} : autorisation Google, choix de la propriété, période, inspection d'URL et connexion Bing."
kind: "Documentation"
section: "Search Console et mots-clés"
order: 30
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["Google Search Console", "GSC", "impressions", "inspection d'URL"]
app_href: "/settings/accounts#gsc-oauth-card"
related: ["opportunites-de-mots-cles", "suivre-la-performance", "anomalies-et-priorites"]
faq:
  - question: "Quelle propriété choisir : domaine ou préfixe d'URL ?"
    answer: "La propriété de type Domaine, quand elle existe : elle couvre tous les sous-domaines et les deux protocoles. Une propriété par préfixe d'URL doit correspondre exactement à l'URL de départ du projet, protocole et www compris."
  - question: "Pourquoi ma page performance est-elle vide alors que la connexion est verte ?"
    answer: "Presque toujours un décalage entre la propriété Search Console et l'URL du projet — https contre http, avec www contre sans. La page indique la raison exacte plutôt que d'afficher un tableau vide."
  - question: "{{app_name}} peut-il modifier quelque chose dans mon Search Console ?"
    answer: "Non. L'accès demandé est en lecture. Le produit lit les performances et, si vous l'activez, interroge l'API d'inspection d'URL — qui est elle aussi en lecture seule."
  - question: "Les données Search Console sont-elles en temps réel ?"
    answer: "Non, Google publie avec deux à trois jours de retard. Une correction déployée hier ne se verra pas aujourd'hui, et une chute constatée aujourd'hui a commencé avant-hier."
---

Sans Search Console, {{app_name}} connaît votre site. Avec elle, il connaît aussi ce que Google
en fait — et c'est cette seconde information qui rend les priorités crédibles.

Une `meta description` manquante sur une page à 12 000 impressions et la même sur une archive
à zéro impression cessent d'être le même problème.

## Autoriser Google

**Paramètres → Comptes → Google Search Console → Connecter**. Vous êtes redirigé vers Google,
vous choisissez le compte qui a accès à la propriété, vous revenez.

L'autorisation est en **lecture seule**. {{app_name}} ne peut rien modifier, ni soumettre, ni
supprimer dans votre Search Console.

## Choisir la propriété

Le rattachement se fait projet par projet, dans les paramètres de crawl, champ **Propriété
Search Console**.

C'est l'endroit où presque toutes les connexions échouent, pour une raison unique : la
propriété ne correspond pas à l'URL du projet.

| Type de propriété | Forme | Couvre |
| --- | --- | --- |
| Domaine | `sc-domain:example.com` | Tous les sous-domaines, http et https |
| Préfixe d'URL | `https://www.example.com/` | Exactement ce préfixe, rien d'autre |

Prenez la propriété **Domaine** si elle existe : elle évite tout le problème. Sinon, vérifiez
caractère par caractère que le préfixe correspond à l'URL de départ de votre projet — `https`
contre `http`, avec `www` contre sans, barre finale comprise.

Si aucune donnée ne remonte, la page vous dit *pourquoi* plutôt que d'afficher un tableau
vide. Lisez ce message : il nomme la cause dans la quasi-totalité des cas.

## Les réglages

Dans les paramètres de crawl du projet, section Search Console :

- **Activer** — joint les données de recherche au crawl.
- **Période** — le nombre de jours d'historique à récupérer. 28 jours par défaut, ce qui lisse
  les variations hebdomadaires sans noyer une évolution récente.
- **Type de recherche** — web, image, vidéo, actualités. Laissez « web » sauf si votre trafic
  vient réellement d'ailleurs.
- **Impressions minimum** — le seuil sous lequel une requête est ignorée. Il élimine la longue
  traîne à deux impressions, où le taux de clic ne veut rien dire statistiquement.

## L'inspection d'URL

Option distincte, plus lente, et parfois décisive : {{app_name}} interroge l'API d'inspection
d'URL de Google pour un échantillon de pages.

Ce qu'elle apporte que le crawl ne peut pas donner : **le verdict de Google lui-même**. Page
indexée ou non, canonical retenue par Google — qui n'est pas toujours celle que vous déclarez —
date du dernier passage, présence dans un sitemap connu de Google.

Trois réglages : activation, nombre maximum d'URLs à inspecter, et délai d'attente. L'API est
contingentée par Google, d'où l'échantillon.

!!! note "Quand ça vaut le coup"
    Quand le crawl dit qu'une page est parfaitement indexable et qu'elle ne reçoit pourtant
    aucune impression. L'inspection tranche : soit Google ne la connaît pas, soit il a retenu
    une autre canonical, soit il l'a vue et écartée.

## Bing Webmaster Tools

Même principe, connexion séparée, et les mêmes réglages : période, impressions minimum,
requêtes et pages. Bing expose en plus ses propres problèmes de crawl, ses sitemaps connus et
ses URLs bloquées.

Le volume est plus faible que Google presque partout, mais les données sont gratuites et
parfois plus explicites sur les blocages d'exploration.

## Ce que ça débloque

Une fois connectée, Search Console alimente trois choses :

1. **La priorisation des anomalies** — par impressions réelles, plus seulement par gravité.
2. **La page Performance** — clics, impressions, position, page par page et requête par
   requête.
3. **Les opportunités de mots-clés**, qui est la fonction pour laquelle cette connexion vaut
   surtout la peine.

→ [Opportunités de mots-clés](/docs/opportunites-de-mots-cles)
