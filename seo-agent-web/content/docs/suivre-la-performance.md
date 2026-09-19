---
title: "Suivre la performance et les Core Web Vitals"
meta_title: "Performance et Core Web Vitals — documentation {{app_name}}"
description: "La page Performance d'un projet : données Search Console, mesures PageSpeed, LCP, INP et CLS, et pourquoi l'échantillon d'URLs est limité."
kind: "Documentation"
section: "Search Console et mots-clés"
order: 32
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["Core Web Vitals", "LCP", "INP", "CLS", "PageSpeed"]
app_href: "/"
related: ["connecter-search-console", "lancer-un-crawl", "plans-et-quotas"]
faq:
  - question: "Pourquoi seules quelques URLs ont-elles une mesure PageSpeed ?"
    answer: "La mesure passe par l'API Google PageSpeed Insights, lente et contingentée par jour. L'échantillon est donc plafonné, et sa taille dépend de votre plan. Choisissez les URLs représentatives de vos gabarits plutôt que de vouloir tout mesurer."
  - question: "Les scores PageSpeed varient d'une mesure à l'autre, est-ce normal ?"
    answer: "Oui. Ce sont des mesures en laboratoire, sensibles à la charge du serveur et du réseau au moment du test. Suivez la tendance sur plusieurs mesures, jamais un point isolé."
  - question: "Un mauvais score PageSpeed fait-il chuter mon classement ?"
    answer: "Pas mécaniquement. Les signaux d'expérience de page pèsent peu face à la pertinence du contenu. Une page lente perd surtout des visiteurs qui abandonnent avant l'affichage — ce qui coûte plus cher que le classement."
---

La page **Performance** d'un projet réunit deux choses que l'on confond souvent : ce que Google
constate sur vos résultats de recherche, et ce que vos pages mettent à s'afficher.

## Les données de recherche

Si [Search Console est connectée](/docs/connecter-search-console) : clics, impressions,
position moyenne et taux de clic, page par page et requête par requête, sur la période
configurée.

Deux réflexes utiles :

- **Regardez les impressions avant les clics.** Une page qui perd des impressions perd de la
  visibilité ; une page qui garde ses impressions et perd ses clics a un problème de vitrine,
  pas de classement.
- **Souvenez-vous du décalage.** Google publie avec deux à trois jours de retard. Une
  correction déployée hier ne se voit pas aujourd'hui.

Pour transformer ces chiffres en actions, la page voisine est plus directe :
[Opportunités de mots-clés](/docs/opportunites-de-mots-cles).

## Les Core Web Vitals

Trois mesures, et une seule question chacune.

**LCP — Largest Contentful Paint.** Combien de temps avant que le contenu principal soit
visible ? En dessous de 2,5 s, c'est bon. Au-delà de 4 s, le visiteur voit une page blanche
assez longtemps pour partir.

**INP — Interaction to Next Paint.** Combien de temps entre un clic et la réaction visible de
la page ? En dessous de 200 ms, c'est bon. Cette mesure a remplacé le FID, qui ne regardait que
la toute première interaction.

**CLS — Cumulative Layout Shift.** De combien la page bouge-t-elle pendant le chargement ? En
dessous de 0,1, c'est bon. C'est la mesure du bouton qui se déplace au moment où on le clique —
presque toujours une image sans dimensions déclarées, une police tardive, ou une bannière
injectée en haut de page.

!!! note "Ces trois-là, et pas les autres"
    Le score global sur 100 de PageSpeed n'est pas un critère de classement, et un site à 62
    peut très bien se porter mieux qu'un site à 95. Travaillez LCP, INP et CLS ; ignorez la
    course au score.

## Pourquoi un échantillon d'URLs

La mesure passe par l'API Google PageSpeed Insights, qui est lente — plusieurs secondes par
URL — et limitée en nombre d'appels par jour, une limite partagée par toute la plateforme.

D'où un plafond d'URLs mesurées par crawl, dont la taille dépend de votre plan.

Cet échantillon se choisit. Mesurer les vingt premières URLs par ordre alphabétique
n'apprend rien ; mesurer la home, une page de catégorie, une fiche produit, un article et une
page de contact vous donne l'état de chacun de vos gabarits.

Réglage dans les [paramètres de crawl](/docs/lancer-un-crawl), avec le choix de la stratégie
mobile ou desktop. Prenez mobile : c'est l'index que Google utilise.

## Ce qu'une mesure ne dit pas

PageSpeed mesure **en laboratoire** : un appareil simulé, un réseau simulé, à un instant donné.
Deux mesures consécutives sur la même URL peuvent différer sensiblement selon la charge de
votre serveur.

Ce qui se lit dans les données de terrain de Google — l'expérience de vos visiteurs réels sur
28 jours — n'est pas la même chose, et c'est cette seconde source que Google utilise pour ses
signaux d'expérience de page.

Conclusion pratique : **suivez la tendance sur plusieurs crawls**, ne réagissez jamais à un
point isolé.

## Relier la performance au reste de l'audit

Une page lente qui n'a aucune impression n'est pas un chantier prioritaire. Une page lente sur
laquelle vous êtes en position 4 avec 8 000 impressions, si.

C'est le principal intérêt d'avoir la performance et les données de recherche sur le même
écran : trier les optimisations par le trafic qu'elles concernent, plutôt que par la couleur
du score.

## Corriger

Les causes les plus fréquentes se règlent dans le code ou la configuration d'hébergement :
images non dimensionnées, images non compressées, polices bloquantes, scripts tiers en tête de
page, absence de cache.

Certaines relèvent des fichiers que {{app_name}} sait modifier — configuration d'en-têtes et de
cache, dimensions d'images dans les gabarits. D'autres relèvent de votre hébergement, et le
produit ne s'en mêle pas : il les signale, vous décidez.

→ [Corrections automatiques](/docs/corrections-automatiques)
