---
title: "Backlinks : inventaire, opportunités et suivi"
meta_title: "Backlinks et netlinking — documentation {{app_name}}"
description: "Importer son profil de liens, faire remonter des opportunités de backlinks, préparer les prises de contact et détecter les liens perdus."
kind: "Documentation"
section: "Aller plus loin"
order: 41
updated_at: "2026-10-19"
audience: "Solo, Pro et Business pour les opportunités"
keywords: ["backlinks", "netlinking", "liens entrants", "Ahrefs"]
app_href: "/"
related: ["analyser-les-concurrents", "automatiser-les-audits", "plans-et-quotas"]
faq:
  - question: "{{app_name}} crée-t-il des backlinks à ma place ?"
    answer: "Non, et personne ne le peut honnêtement. Le produit trouve des endroits où votre site aurait sa place, prépare un message, et vous laisse l'envoyer. La décision de publier appartient toujours à quelqu'un d'autre."
  - question: "Faut-il un abonnement Ahrefs ?"
    answer: "Non. La synchronisation Ahrefs est une option pour ceux qui en ont un. Sans elle, vous importez un CSV depuis n'importe quel outil, ou vous travaillez uniquement sur les opportunités."
  - question: "Que se passe-t-il si un lien obtenu disparaît ?"
    answer: "Un contrôle périodique visite les pages des liens marqués « obtenu » ou « contacté » et signale ceux qui ne pointent plus vers vous. C'est la partie du netlinking que tout le monde oublie."
---

Le netlinking est le domaine où les outils promettent le plus et tiennent le moins. Ce que
{{app_name}} fait ici est volontairement borné : il inventorie, il repère, il prépare, il
surveille. Il n'obtient pas de liens à votre place, parce que personne ne le peut.

## Inventorier vos liens entrants

Page **Liens entrants** d’un projet. Le bloc « Liens vers votre site » présente les sources importées ; le bloc « Liens dans les pages analysées » présente les liens internes et sortants de l’analyse sélectionnée. Deux façons de la remplir :

![Bloc Netlinking du crawl : pages analysées, pages orphelines et domaines externes](/static/captures/netlinking.webp "Ce que le crawl dit déjà de vos liens : pages analysées, pages orphelines, domaines externes.")

**Import CSV** — depuis n'importe quel outil qui exporte un profil de liens. C'est la voie
universelle, et elle ne demande aucun abonnement.

**Synchronisation Ahrefs** — si vous disposez d'un accès à l'API Ahrefs. Quatre portées
possibles : domaine, sous-domaines, préfixe d'URL, ou URL exacte. Le volume récupéré est
plafonné pour éviter d'aspirer un profil entier d'un coup.

Les imports montrent les domaines référents, les pages cibles et les ancres recensés par chaque source ; ils ne constituent pas un inventaire exhaustif. Les **pages orphelines** du bloc d’analyse sont celles sans lien interne entrant repéré dans les pages analysées. Ce chiffre ne mesure pas l’absence de liens depuis d’autres sites.

## Les opportunités

*Disponible à partir du plan Solo.*

Page **Faire connaître mon site**. {{app_name}} cherche des endroits où votre site aurait légitimement sa
place : discussions où votre sujet est abordé, pages de ressources, listes, articles qui citent
des outils comparables.

La recherche s'appuie sur des mots-clés que vous définissez, et sur des sources que vous
choisissez.

Chaque opportunité porte un statut que vous faites évoluer à la main :

| Statut | Sens |
| --- | --- |
| **Nouvelle** | Détectée, pas encore traitée |
| **Contactée** | Vous avez envoyé un message |
| **Obtenue** | Le lien existe |
| **Perdue** | Refus, ou lien retiré |

Ce n'est pas décoratif : c'est ce qui vous évite de recontacter trois fois le même site, et ce
qui alimente la surveillance décrite plus bas.

## La préparation des messages

Pour une opportunité donnée, {{app_name}} peut rédiger un brouillon de prise de contact :
contexte de la page visée, raison pour laquelle votre contenu y a sa place, ton adapté à la
plateforme.

Trois choses à savoir, et elles sont volontaires :

- **C'est un brouillon.** Il est fait pour être relu et modifié. Un message générique envoyé
  tel quel se repère à dix mètres et se supprime aussi vite.
- **Vous l’envoyez vous-même.** « Copier et ouvrir » prépare le texte et ouvre la discussion. Après l’envoi sur le site concerné, revenez cliquer sur « J’ai envoyé la réponse ». Cette déclaration ne prouve pas encore la présence du lien ; elle met à jour votre suivi.
- **Les brouillons sont comptés** dans un quota mensuel, comme les recherches. Les deux
  dépendent de votre plan.

!!! warning "La ligne à ne pas franchir"
    Le netlinking automatisé au kilomètre — commentaires, forums, profils — abîme un site plus
    qu'il ne l'aide, et les moteurs savent le reconnaître depuis longtemps. Les quotas de ce
    module sont bas exprès : ils poussent vers quelques contacts pertinents plutôt que vers
    trois cents envois.

## L'automatisation, si vous la voulez

Une recherche périodique d'opportunités peut être activée par projet : mots-clés à surveiller,
sources, fréquence quotidienne ou hebdomadaire, nombre maximum de trouvailles par passage, et
rédaction automatique des brouillons ou non. Cette automatisation ne publie pas de messages sur les sites tiers ; l’envoi reste manuel.

Elle est **désactivée par défaut**, et c'est le bon réglage tant que vous n'avez pas vérifié à
la main que les opportunités remontées sont pertinentes pour votre site.

## La surveillance des liens obtenus

C'est la partie que la plupart des outils oublient. Un lien obtenu n'est pas acquis : la page
est refondue, l'article est dépublié, le lien passe en `nofollow`, ou disparaît sans
explication.

{{app_name}} revisite périodiquement les pages des opportunités marquées **obtenue** ou
**contactée**, et signale celles qui ne pointent plus vers vous.

Un lien perdu se récupère souvent d'un message poli — beaucoup plus facilement qu'on n'en
obtient un nouveau.

## Ce que ça vaut par rapport au reste

Le netlinking est lent et incertain. Avant d'y consacrer un budget, vérifiez que les pages
que vous voulez pousser sont **indexables**, correctement titrées et reliées entre elles :
un lien externe vers une page en `noindex` est de l'argent brûlé.

→ [Anomalies et priorités](/docs/anomalies-et-priorites)
