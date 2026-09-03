---
slug: "regression-seo-apres-mise-en-production"
title: "Les régressions SEO arrivent au déploiement, pas au fil de l'eau"
meta_title: "Détecter une régression SEO après un déploiement"
description: "Les pannes SEO naissent presque toutes d'une mise en production. Ce qui casse le plus souvent, et comment l'attraper en jours plutôt qu'en mois."
kind: "Guide"
updated_at: "2026-09-03"
published_at: "2026-09-02"
audience: "Équipes techniques et responsables SEO"
keywords: ["régression SEO", "déploiement", "noindex", "migration", "monitoring"]
related: ["frequence-crawl-seo-site-vitrine-ecommerce-blog", "corriger-le-seo-dans-le-code-pas-dans-un-rapport"]
cta: "{{app_name}} compare chaque crawl au précédent et nomme les anomalies apparues — c'est la vue qui relie une régression au déploiement qui l'a créée, pendant qu'on peut encore le retrouver."
faq:
  - question: "Combien de temps avant qu'une régression se voie dans le trafic ?"
    answer: "De quelques jours à plusieurs semaines, selon la fréquence de passage de Google sur les pages touchées. C'est précisément le problème : quand la courbe bouge, le déploiement responsable est loin derrière."
  - question: "Un noindex accidentel se rattrape-t-il ?"
    answer: "Oui, mais pas instantanément. Il faut que Google réexplore les pages, ce qui prend de quelques jours à plusieurs semaines. Le coût réel est le trafic perdu entre-temps, et il n'est pas récupérable."
  - question: "Faut-il crawler avant ou après le déploiement ?"
    answer: "Les deux. Le crawl d'avant sert de référence, celui d'après révèle l'écart. Sans référence, un crawl post-déploiement ne dit pas ce qui a changé — seulement ce qui existe."
---

Les sites ne se dégradent pas progressivement. Ils cassent d'un coup, un mardi après-midi, au
moment d'une mise en production — et personne ne s'en aperçoit avant six semaines.

C'est la nature même du problème : rien n'est visible dans le navigateur, et les courbes ne
bougent qu'après que Google a repassé sur les pages touchées.

## Ce qui casse, dans l'ordre de fréquence

**1. Le `noindex` de préproduction qui part en production.** Le grand classique. La
préproduction est protégée par un `noindex` global ou un `X-Robots-Tag`, et la configuration
part telle quelle. Le site entier disparaît de l'index en quelques semaines.

**2. Le `robots.txt` remplacé.** Même mécanisme, autre fichier. Un `Disallow: /` hérité d'un
environnement de test, ou un blocage de `/assets/` qui empêche Google de rendre les pages.

**3. Les URLs qui changent sans redirection.** Une refonte modifie la structure des chemins et
les anciennes URLs répondent 404. Tout l'historique de ces pages est perdu, et les liens
externes pointent dans le vide.

**4. Une `canonical` généralisée à tort.** Un composant de layout qui écrit la même `canonical`
sur toutes les pages — souvent la home. Le site entier se déclare comme une seule page.

**5. Le maillage interne qui disparaît.** Un menu réécrit en JavaScript sans vraies balises
`<a>`. Les pages existent toujours, plus rien ne les relie.

**6. Les balises perdues dans un changement de gabarit.** `title` générique sur tout le site,
`h1` qui devient un `div`, `hreflang` supprimé lors d'un refactoring.

Aucune de ces six n'est visible en regardant le site dans un navigateur. C'est pour cela
qu'elles passent.

## Pourquoi on s'en aperçoit si tard

La chaîne est longue : le déploiement casse quelque chose, Google doit repasser sur les pages,
l'index se met à jour, les positions bougent, les impressions baissent, et enfin quelqu'un
regarde la courbe.

Comptez trois à six semaines entre la cause et le symptôme visible. À ce moment-là, il y a eu
quinze autres déploiements, et retrouver le responsable devient une enquête.

Et le trafic perdu entre-temps ne se rattrape pas.

## La méthode : un crawl avant, un crawl après

Le principe tient en une phrase : **comparer deux états, pas regarder un état**.

Le crawl d'avant est la référence. Le crawl d'après révèle l'écart. Sans référence, un crawl
post-déploiement ne vous dit pas ce qui a changé — seulement ce qui existe, ce qui ne prouve
rien.

Ce qu'il faut regarder dans la comparaison, dans cet ordre :

1. **Le nombre de pages indexables.** Une chute est une urgence, pas une remarque.
2. **Les anomalies apparues.** Pas le total — les nouvelles. C'est le chiffre qui nomme la
   régression.
3. **Les pages disparues du crawl.** Soit elles ne sont plus liées, soit elles ne répondent
   plus.
4. **Les statuts HTTP.** Des 404 qui n'existaient pas, des redirections qui s'allongent.

## Les cinq minutes après chaque mise en production

À faire à la main, tout de suite, avant même de lancer un crawl :

- `votresite.com/robots.txt` — le lire en entier, pas le survoler.
- Le code source de la home : y a-t-il un `noindex` ? La `canonical` pointe-t-elle sur
  elle-même ?
- Le même contrôle sur **une** page de chaque gabarit — une catégorie, une fiche, un article.
- `votresite.com/sitemap.xml` — répond-il, et contient-il un nombre d'URLs plausible ?
- Trois anciennes URLs importantes — répondent-elles encore, ou en 301 vers la bonne page ?

Ces cinq minutes attrapent la majorité des régressions graves. Le crawl complet attrape le
reste.

## Automatiser, parce que personne ne le fera à la main

La vérité pratique : une checklist manuelle après chaque déploiement tient trois semaines, puis
un jour où tout le monde est pressé.

Ce qui tient dans la durée, c'est un crawl déclenché automatiquement après chaque mise en
production, comparé au précédent, avec une alerte sur deux chiffres : pages indexables et
anomalies apparues.

C'est le même raisonnement que les tests automatisés. Personne ne relance les tests à la main
après chaque commit ; on ne devrait pas non plus vérifier le SEO à la main après chaque
déploiement.

## Le cas particulier des migrations

Une migration d'URLs mérite son propre protocole, parce que le risque y est maximal :

1. Crawl complet **avant**, exporté et conservé.
2. Table de correspondance ancienne URL → nouvelle URL, exhaustive.
3. Redirections en 301, jamais en 302 — et jamais toutes vers la home.
4. Crawl **après**, et vérification que chaque ancienne URL importante redirige vers son
   équivalent.
5. Contrôle des liens internes : ils doivent pointer vers les nouvelles URLs directement, pas
   vers une redirection.
6. Surveillance des impressions pendant six semaines.

L'étape 5 est celle qu'on oublie. Un site où tous les liens internes traversent une
redirection fonctionne parfaitement pour l'utilisateur, et gaspille son budget d'exploration.
