---
title: "Vérifier qu'une correction a vraiment fonctionné"
meta_title: "Vérification post-crawl des corrections — documentation {{app_name}}"
description: "Comment {{app_name}} confirme au crawl suivant qu'une anomalie corrigée a disparu, détecte les régressions collatérales, et ce que chaque statut signifie."
kind: "Documentation"
section: "Corriger"
order: 23
updated_at: "2026-09-03"
audience: "Solo, Pro et Business"
keywords: ["vérification", "régression SEO", "suivi des corrections", "crawl de contrôle"]
app_href: "/"
related: ["corrections-automatiques", "lire-le-rapport", "automatiser-les-audits"]
faq:
  - question: "Pourquoi une correction reste-t-elle « en cours » après la fusion de la PR ?"
    answer: "Parce que la vérification a besoin d'un crawl postérieur au déploiement. Tant que le site en ligne n'a pas été réexploré, le produit n'a aucune preuve et refuse d'en inventer une."
  - question: "Ma correction est marquée « toujours présente », qu'est-ce que ça veut dire ?"
    answer: "Le correctif est dans le dépôt mais l'anomalie subsiste sur le site exploré. Les trois causes habituelles : la PR n'est pas fusionnée, le déploiement n'a pas eu lieu, ou un cache sert encore l'ancienne version."
  - question: "Une correction peut-elle en casser une autre ?"
    answer: "Oui, et c'est précisément ce que la vérification cherche. Elle compare le rapport au rapport de référence et signale les anomalies apparues entre les deux, sans les attribuer à une correction précise quand plusieurs partagent la même fenêtre."
---

Une pull request fusionnée n'est pas une anomalie corrigée. Entre les deux il y a un
déploiement, un cache, parfois un CDN, et l'hypothèse que le correctif était le bon.

{{app_name}} ne coche donc rien à la fusion. Il attend le crawl suivant et regarde.

## Comment ça marche

À la fin de chaque crawl, le produit reprend les tâches de correction du projet et confronte
chacune au rapport tout juste produit :

- les URL du correctif ont été revisitées avec succès dans un crawl plus récent et
  l'anomalie a disparu → **résolue** ;
- l'anomalie subsiste, y compris sur une autre page pour une correction globale ou sous
  une variante d'indexabilité → **toujours présente** ;
- des URL n'ont pas été explorées, le crawl est partiel ou le périmètre du correctif est
  inconnu → **non vérifiable**. L'absence d'une URL dans les exemples ne suffit pas à conclure.

Une tâche confirmée résolue passe automatiquement en **Fait**. Vous n'avez rien à cocher.

!!! note "Le cas « PR ouverte, anomalie encore là »"
    Il est attendu, pas signalé. Tant que la pull request n'est pas fusionnée, le site en
    ligne n'a évidemment pas changé — badger une régression ici serait du bruit.

## Les statuts, et ce qu'ils veulent dire

| Statut | Lecture |
| --- | --- |
| **Résolue** | Le crawl postérieur ne voit plus l'anomalie sur cette URL. C'est la seule preuve qui compte. |
| **Toujours présente** | Le correctif est dans le dépôt, l'anomalie est encore en ligne. Voir ci-dessous. |
| **Pas encore vérifiée** | Aucun crawl n'a eu lieu depuis la correction. Lancez-en un. |
| **Non vérifiable** | Le crawl ne couvre pas assez le correctif pour conclure. Consultez le motif sur la tâche et relancez un crawl complet. |

La page **Corrections** du projet affiche ces compteurs en tête : combien de corrections
vérifiées résolues, combien encore présentes, combien portent une pull request.

## « Toujours présente » : les trois causes

Dans cet ordre de fréquence :

1. **La PR n'a pas été fusionnée.** Vérifiez le lien sur la tâche.
2. **Le déploiement n'a pas eu lieu**, ou a échoué. Le dépôt est à jour, le site en ligne
   non.
3. **Un cache sert encore l'ancienne version.** CDN, cache de page, service worker. Ouvrez
   l'URL en navigation privée et regardez le code source : si le correctif y est, c'est un
   cache ; s'il n'y est pas, c'est un déploiement.

Si aucune des trois n'explique le résultat, le correctif ne visait pas le bon endroit. La
fiche d'anomalie affiche ce que le crawler voit exactement — c'est le point de départ pour
comprendre.

## Les régressions collatérales

La vérification ne se contente pas de la question posée. Elle compare le rapport au **rapport
de référence** — celui du crawl à partir duquel la correction a été décidée — et relève les
anomalies **apparues** entre les deux.

C'est ce qui attrape le cas désagréable : une correction de `canonical` qui règle 40 URLs et
en met 12 autres en `noindex`.

Deux précautions dans la lecture, et elles sont volontaires :

- **Quand plusieurs corrections partagent la même fenêtre**, le produit ne désigne pas de
  coupable. Il indique le nombre de corrections concernées et vous laisse le diff. Accuser
  une PR au hasard serait pire que ne rien dire.
- **Quand le rapport de référence est illisible ou absent**, aucune comparaison n'est
  affichée. Diffuser un diff contre une base vide ferait apparaître chaque anomalie survivante
  comme une nouveauté — un faux positif spectaculaire.

## Le cas particulier des réécritures de mots-clés

Une page réécrite pour mieux répondre à une requête ne peut pas être validée par un crawl. Le
crawler constate que le texte a changé ; il ne peut pas dire si la position a bougé.

Ces tâches sont donc **exclues** de la vérification automatique. Leur verdict se lit dans
Search Console, quelques semaines plus tard, sur les clics et la position moyenne.

→ [Opportunités de mots-clés](/docs/opportunites-de-mots-cles)

## Fermer la boucle sans y penser

La vérification demande un crawl postérieur à la correction. Le plus simple est de ne plus
avoir à y penser : un crawl planifié fait le travail, et vous retrouvez les tâches passées en
« Fait » toutes seules.

→ [Automatiser les audits](/docs/automatiser-les-audits)
