---
title: "Lancer un crawl et régler ses paramètres"
meta_title: "Crawl SEO : lancer et paramétrer — documentation {{app_name}}"
description: "Comment {{app_name}} explore un site : profondeur, nombre de pages, cadence, filtres d'URL, JavaScript, robots.txt et options avancées."
kind: "Documentation"
section: "Prise en main"
order: 12
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["crawl SEO", "paramètres de crawl", "robots.txt", "profondeur"]
app_href: "/"
related: ["lire-le-rapport", "anomalies-et-priorites", "automatiser-les-audits", "plans-et-quotas"]
faq:
  - question: "Mon crawl s'est arrêté avant la fin, pourquoi ?"
    answer: "Trois causes possibles : la limite de pages du plan ou du projet a été atteinte, le délai maximal du job a expiré, ou l'hôte a commencé à refuser les requêtes. La page du job indique laquelle."
  - question: "{{app_name}} exécute-t-il le JavaScript ?"
    answer: "Oui, le crawler dispose d'un moteur de rendu Chromium. Le rendu est plus lent et plus coûteux que la lecture du HTML brut, il est donc réservé aux cas où il change le résultat."
  - question: "Le crawl compte-t-il les images et les fichiers CSS dans mon quota de pages ?"
    answer: "Non. Le quota compte les pages HTML explorées. Les ressources ne sont comptées que si vous activez leur vérification, et elles sont plafonnées séparément."
  - question: "Puis-je crawler un site dont je ne suis pas propriétaire ?"
    answer: "Pour un audit de votre propre site, oui bien sûr. Pour un site tiers, la fonction Concurrents existe et se limite volontairement à un échantillon de pages, à cadence réduite."
---

Sur la page du projet, **Lancer un crawl**. Pour un premier passage, ne touchez à rien : les
valeurs par défaut couvrent la quasi-totalité des sites.

Le crawl s'exécute sur un worker séparé de l'interface. Fermer l'onglet ne l'interrompt pas.

## Ce que le crawler collecte

Pour chaque page atteinte :

- le **statut HTTP** et la chaîne de redirections qui y mène ;
- l'**indexabilité** : `robots.txt`, `meta robots`, en-tête `X-Robots-Tag`, `canonical` ;
- les **balises** : `title`, `meta description`, `h1` et hiérarchie des titres, `hreflang`,
  `lang`, Open Graph, données structurées ;
- les **liens** internes et externes, leur ancre, leur attribut `rel`, et la profondeur de
  clic depuis la page de départ ;
- les **images** : `alt`, poids, format ;
- la présence de la page dans le **sitemap XML**, et l'inverse — les URLs du sitemap que le
  maillage interne n'atteint jamais.

À l'arrivée : un rapport, des scores, et une liste d'anomalies typées.

## Les réglages qui comptent vraiment

Tout se règle dans **Paramètres de crawl**, page par projet. Les options sont nombreuses ;
en pratique, cinq suffisent.

### Nombre de pages maximum

La limite de sécurité. Elle évite qu'un calendrier d'événements ou un moteur à facettes
génère 400 000 URLs et consomme votre quota mensuel en une nuit.

Le plafond réel est le plus petit de trois valeurs : celle que vous fixez ici, celle de votre
plan, et ce qu'il reste de votre quota mensuel de pages.

### Profondeur maximale

Le nombre de clics depuis l'URL de départ. Une profondeur de 3 ou 4 suffit à cartographier un
site vitrine. Un e-commerce a besoin de plus, sinon les fiches produit restent invisibles.

Si des pages importantes n'apparaissent pas dans le rapport, c'est souvent la première chose
à regarder — et le fait qu'elles soient si profondes est en soi un problème SEO.

### Cadence : workers, délai minimum, crawl-delay

Trois réglages liés :

- **Workers** — le nombre de requêtes en parallèle.
- **Délai minimum entre deux requêtes** — le frein.
- **Respecter le crawl-delay de robots.txt** — activé par défaut, et à laisser activé.

!!! warning "Un site qui répond 429 ou 403 n'est pas un site cassé"
    Si l'hôte se met à refuser le crawler, {{app_name}} ralentit puis s'arrête plutôt que
    d'insister, et ces réponses ne sont **pas** comptées comme des erreurs de votre site dans
    le rapport. Elles apparaissent comme un incident de crawl. Baissez les workers et
    augmentez le délai avant de relancer.

### Filtres d'URL : inclure, exclure, paramètres

Trois leviers pour empêcher le crawler de se perdre :

- **Inclure / exclure** (expressions régulières) — pour restreindre à une section, ou écarter
  `/panier/`, `/mon-compte/`, un calendrier infini.
- **Supprimer des paramètres** — `utm_source`, `gclid`, `sessionid` : le même contenu vu vingt
  fois sous vingt URLs pollue le rapport et gaspille le quota.
- **Nombre maximum de paramètres** — un garde-fou contre les facettes combinatoires.

Ces filtres décrivent ce que **le crawl** explore. Ils ne changent rien à ce que Google
explore : une URL exclue ici reste indexable en réalité.

### Vérification des ressources

Désactivée par défaut. Activée, le crawler contrôle aussi les images, CSS et scripts liés :
c'est ce qui remonte les images cassées et les ressources bloquées. Le coût est un volume de
requêtes nettement supérieur, plafonné par un réglage dédié.

## Les options avancées

- **Ignorer robots.txt** — sur votre propre site, pour auditer une préproduction verrouillée.
  Jamais ailleurs.
- **Suivre les liens en nofollow** — utile pour cartographier un site qui abuse du `nofollow`
  interne.
- **User-agent** — personnalisable si vous devez autoriser le crawler dans un pare-feu.
- **Délais d'attente** — à augmenter sur un hébergement lent, plutôt que de conclure à des
  erreurs 5xx qui n'en sont pas.

## PageSpeed et Core Web Vitals

Case **PageSpeed** dans les paramètres, avec un nombre d'URLs à analyser et une stratégie
mobile ou desktop.

Cette mesure passe par l'API Google PageSpeed Insights, qui est lente et contingentée. C'est
pourquoi elle ne s'applique qu'à un échantillon d'URLs, dont la taille dépend de votre plan.
Choisissez cet échantillon : la home et vos gabarits principaux valent mieux que les vingt
premières URLs par ordre alphabétique.

→ [Suivre la performance](/docs/suivre-la-performance)

## Search Console et Bing dans le même crawl

Si vous avez connecté Search Console au projet, le crawl y joint les données de recherche
réelles : impressions, clics, position, requêtes. Même principe pour Bing Webmaster Tools.

C'est ce croisement qui fait passer le rapport de « voici les anomalies » à « voici les
anomalies sur les pages qui vous rapportent du trafic ».

→ [Connecter Search Console](/docs/connecter-search-console)

## Suivre et arrêter un crawl

La page **Jobs** liste tous les traitements en cours et passés : crawls, corrections,
analyses de concurrents. Chaque job affiche son avancement, sa durée, ses journaux, et deux
boutons — **Annuler** et **Relancer**.

Un crawl annulé conserve ce qu'il a déjà collecté, mais son rapport est partiel : traitez-le
comme un sondage, pas comme un audit.
