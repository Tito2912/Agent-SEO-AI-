---
title: "Lancer une analyse et choisir son périmètre"
meta_title: "Crawl SEO : lancer et paramétrer — documentation {{app_name}}"
description: "Choisir les pages et sous-domaines à analyser, adapter les paramètres disponibles et suivre la préparation de son bilan."
kind: "Documentation"
section: "Prise en main"
order: 12
updated_at: "2026-10-21"
audience: "Tous les plans"
keywords: ["crawl SEO", "paramètres de crawl", "robots.txt", "profondeur"]
app_href: "/"
related: ["lire-le-rapport", "anomalies-et-priorites", "automatiser-les-audits", "plans-et-quotas"]
faq:
  - question: "Mon crawl s'est arrêté avant la fin, pourquoi ?"
    answer: "Trois causes possibles : la limite de pages du plan ou du projet a été atteinte, le délai maximal de l’analyse a expiré, ou l'hôte a commencé à refuser les requêtes. Consultez le suivi dans Activité pour lire les informations disponibles."
  - question: "{{app_name}} exécute-t-il le JavaScript ?"
    answer: "Oui, le crawler dispose d'un moteur de rendu Chromium. Le rendu est plus lent et plus coûteux que la lecture du HTML brut, il est donc réservé aux cas où il change le résultat."
  - question: "Le crawl compte-t-il les images et les fichiers CSS dans mon quota de pages ?"
    answer: "Non. Le quota compte les pages HTML explorées. Les ressources ne sont comptées que si vous activez leur vérification, et elles sont plafonnées séparément."
  - question: "Puis-je crawler un site dont je ne suis pas propriétaire ?"
    answer: "Pour un audit de votre propre site, oui bien sûr. Pour un site tiers, la fonction Concurrents existe et se limite volontairement à un échantillon de pages, à cadence réduite."
---

Sur la page du projet, lancez une **nouvelle analyse**. Pour un premier passage, ne touchez à rien : les
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

Dans le menu du site, ouvrez **Réglages de l’analyse**. Le nombre de pages et les sous-domaines sont accessibles en premier. Les autres choix sont regroupés dans des sections repliables ; les réglages d’analyse d’un site partagé sont consultables par les collaborateurs et enregistrables par son propriétaire.

### Nombre de pages et sous-domaines

Choisissez un nombre maximal de pages. Le moteur applique les plafonds de votre formule et le solde mensuel du compte qui gère le site. L’option des sous-domaines étend le périmètre, par exemple à `blog.exemple.fr`.

**Enregistrer les paramètres ne lance pas d’analyse** et ne modifie aucune page du site. Vos choix seront repris lors des prochaines demandes.

### Adapter l’exploration

Dans **Adapter l’exploration (avancé)** :

- **Connexions simultanées** : nombre de pages demandées en parallèle. Réduisez-le si votre hébergement refuse les requêtes.
- **Attente maximale par page** : délai laissé à une page pour répondre.
- **Inclure / exclure des adresses** : expressions régulières pour limiter l’analyse à une rubrique ou écarter certaines adresses. Ces filtres ne changent pas ce que Google peut explorer.
- **Ignorer robots.txt** : option à réserver à un site que vous gérez, avec une compréhension des restrictions qu’il définit.

Le groupe **Options avancées** contient l’identifiant du robot (User-Agent).

### Données complémentaires

Les ressources (images, styles et scripts), PageSpeed, Google Search Console et Bing sont regroupés dans **Données en plus**. Consultez les choix enregistrés avant de les modifier ; les autorisations Google et Bing se gèrent aussi depuis les connexions du site. Ces services sont facultatifs pour obtenir une première analyse.

## PageSpeed et Core Web Vitals

Module **Core Web Vitals**, dans **Données en plus** : l’appareil testé (mobile ou desktop), le
nombre de pages à tester, l’attente maximale de PageSpeed et le nombre de requêtes simultanées.

Cette mesure passe par l'API Google PageSpeed Insights, qui est lente et contingentée. C'est
pourquoi elle ne s'applique qu'à un échantillon d'URLs, dont la taille dépend de votre plan.
Les réglages permettent de choisir le nombre maximal de pages et le type d’appareil ; ils ne proposent pas de sélectionner manuellement chaque adresse testée.

→ [Suivre la performance](/docs/suivre-la-performance)

## Search Console et Bing dans le même crawl

Si vous avez connecté Search Console au projet, le crawl y joint les données de recherche
réelles : impressions, clics, position, requêtes. Même principe pour Bing Webmaster Tools.

C'est ce croisement qui fait passer le rapport de « voici les anomalies » à « voici les
anomalies sur les pages qui vous rapportent du trafic ».

→ [Connecter Search Console](/docs/connecter-search-console)

## Suivre une analyse et retrouver son bilan

Ouvrez **Activité**. La liste rassemble les traitements récents et ceux encore actifs accessibles à votre compte. Les filtres Site, Type et État s’appliquent à cette liste ; utilisez **Recharger la liste** pour récupérer d’autres demandes.

**Voir le suivi** distingue attente, exécution, préparation du bilan, échec et annulation. Un traitement « Terminé » ne suffit pas à garantir que son rapport est lisible : le bouton **Consulter le bilan de cette analyse** apparaît lorsque ce rapport précis est disponible.

**Demander l’arrêt** agit sur un traitement actif. Une fois arrêté ou en échec, **Demander une nouvelle analyse** crée une nouvelle demande, soumise aux limites et au quota du compte propriétaire. Un arrêt ne garantit pas qu’un rapport partiel soit disponible ; les bilans précédents restent accessibles depuis le site.

Si la mise à jour est interrompue, **Actualiser l’état** relit le suivi sans relancer d’analyse. Une session expirée propose de se reconnecter puis de revenir au même écran. Les journaux sont accessibles dans **Détails techniques et journaux**.
