---
slug: "frequence-crawl-seo-site-vitrine-ecommerce-blog"
title: "À quelle fréquence crawler un site vitrine, un e-commerce ou un blog ?"
meta_title: "Fréquence de crawl SEO selon le type de site"
description: "Le bon rythme d'audit dépend de la vitesse à laquelle un site peut casser, pas de sa taille. Repères par type de site et déclencheurs à ne pas rater."
kind: "Guide"
updated_at: "2026-09-03"
published_at: "2026-05-12"
audience: "Freelances, agences et équipes internes"
keywords: ["fréquence de crawl", "monitoring SEO", "audit récurrent"]
related: ["audit-seo-technique-checklist-priorites", "regression-seo-apres-mise-en-production"]
cta: "{{app_name}} enchaîne crawl, comparaison et vérification des corrections : vous ne relancez rien à la main, et une régression se voit dans les jours qui suivent le déploiement qui l'a créée."
faq:
  - question: "Un crawl trop fréquent peut-il gêner mon site ?"
    answer: "Un crawl poli — robots.txt respecté, crawl-delay honoré, requêtes simultanées limitées — est indolore. Ce qui gêne un serveur, c'est un crawl agressif, pas un crawl fréquent."
  - question: "Faut-il crawler tout le site à chaque fois ?"
    answer: "Pour la comparaison entre deux crawls, oui : comparer un crawl complet à un crawl partiel produit des faux mouvements. Un crawl restreint à une section est utile pour vérifier une correction précise, pas pour suivre une tendance."
---

La question est mal posée. Ce n'est pas la taille du site qui détermine le rythme, c'est **la
vitesse à laquelle il peut casser**.

Un catalogue de 50 000 pages qui ne bouge jamais a besoin de moins de surveillance qu'un site
vitrine de 30 pages en pleine refonte.

## Les repères, par type de site

| Type | Rythme | Ce qui casse, et à quelle vitesse |
| --- | --- | --- |
| **Vitrine stable** | Mensuel | Presque rien entre deux crawls. Le mensuel sert à attraper ce qui vient de l'extérieur : liens sortants morts, certificat, redirections d'hébergeur. |
| **Blog actif** | Toutes les 2 semaines | Chaque publication passe par le même gabarit. Une modification de template introduit l'anomalie sur tout l'historique d'un coup. |
| **E-commerce** | Hebdomadaire | Produits retirés qui deviennent des 404 liées, facettes qui génèrent des URLs, stocks qui font disparaître des pages du maillage. |
| **Site en refonte** | Après chaque mise en production | C'est là que 90 % des régressions SEO naissent. |
| **Site multilingue** | Mensuel, complet | Les relations `hreflang` ne se vérifient que si le crawl voit les deux côtés en même temps. |

## Les déclencheurs qui priment sur le calendrier

Quel que soit votre rythme, ces cinq événements méritent un crawl immédiat :

1. **Une mise en production.** Surtout si elle touche les gabarits, le routage ou la
   configuration d'hébergement.
2. **Un changement de CMS, de thème ou d'hébergeur.** Les `robots.txt` de préproduction qui
   partent en production sont un classique du genre.
3. **Une migration d'URLs.** Le crawl de contrôle fait partie de la migration, pas de l'après.
4. **Une chute d'impressions dans Search Console.** Le crawl dit si la cause est technique.
5. **Un lot de corrections déployé.** Sans crawl postérieur, vous n'avez aucune preuve que ça a
   marché.

Le cinquième est le plus négligé, et c'est celui qui coûte le plus cher : une correction non
vérifiée est une correction dont vous ne savez rien.

## Ce qu'un rythme régulier vous donne

Un crawl isolé donne une photo. Une série donne trois choses qu'aucune photo ne contient :

**Le sens de la marche.** Le nombre d'anomalies baisse-t-il ou monte-t-il ? Sur un site vivant,
stable veut dire que vous corrigez au rythme où vous cassez.

**La date d'apparition d'un problème.** Une anomalie apparue entre le crawl du 3 et celui du 17
se relie à un déploiement précis. Sans historique, vous cherchez dans six mois de commits.

**La preuve du travail fait.** Si vous facturez cet audit, la comparaison entre deux crawls est
votre livrable le plus convaincant — bien plus qu'un score.

## Le piège du crawl partiel

Pour que la comparaison ait un sens, les crawls doivent être comparables. Un crawl arrêté à
mi-chemin — limite de pages atteinte, annulation, timeout — produit des mouvements qui
n'existent pas : 200 pages « disparues » qui n'ont simplement pas été visitées.

Deux précautions :

- réglez la limite de pages **au-dessus** du volume réel du site, avec de la marge pour la
  croissance ;
- vérifiez que le crawl s'est terminé normalement avant de tirer une conclusion d'une
  comparaison.

## Un rythme tenable vaut mieux qu'un rythme idéal

Un crawl mensuel qui tourne tous les mois vaut infiniment mieux qu'un crawl hebdomadaire
abandonné au bout de trois semaines.

Commencez au rythme que vous tiendrez, ajoutez les déclencheurs événementiels, et augmentez
seulement si les crawls trouvent régulièrement des choses. Si trois mois de crawls
hebdomadaires ne remontent rien de neuf, vous crawlez trop souvent.
