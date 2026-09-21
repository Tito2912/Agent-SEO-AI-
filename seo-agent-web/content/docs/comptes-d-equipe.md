---
title: "Comptes d'équipe : inviter quelqu'un sur vos projets"
meta_title: "Comptes d'équipe — documentation {{app_name}}"
description: "Inviter un collaborateur, ce qu'il voit et ce qu'il peut faire, qui paie ses actions, et les deux connexions qui ne se partagent pas de la même façon."
kind: "Documentation"
section: "Compte et facturation"
order: 51
updated_at: "2026-09-21"
audience: "Pro et Business"
keywords: ["comptes d'équipe", "collaborateur", "agence", "invitation", "places"]
app_href: "/settings/team"
related: ["plans-et-quotas", "connecter-github", "connecter-search-console", "depannage"]
faq:
  - question: "Qui paie ce qu'un collaborateur consomme ?"
    answer: "Le propriétaire du projet. Un consultant au forfait Gratuit qui travaille sur un projet Business obtient le moteur Business, débité sur le compte de l'agence. C'est le PROJET qui décide du plan et du quota, pas la personne connectée."
  - question: "Mon collaborateur garde-t-il son propre abonnement ?"
    answer: "Oui. Hors projet — son assistant, sa facturation, ses propres sites — il reste sur son compte et son plan. Tout rattacher à l'hôte aurait effacé l'abonnement d'un consultant qui est lui-même client."
  - question: "Combien de places ai-je ?"
    answer: "{{members_pro}} sur le plan {{label_pro}} et {{members_business}} sur {{label_business}}. Les places sont forfaitaires : inviter quelqu'un ne déclenche aucun paiement supplémentaire."
  - question: "Peut-on appartenir à deux équipes ?"
    answer: "Non, une seule adhésion par personne. Deux adhésions rendraient une adresse comme /projects/mon-site ambiguë si les deux comptes possédaient un projet de ce nom."
---

Un compte d'équipe permet d'inviter quelqu'un — un consultant, un collègue, un client — à
travailler sur **vos** projets, sans lui donner vos identifiants.

## Inviter quelqu'un

Depuis **Paramètres → Équipe**. Vous saisissez une adresse e-mail, la personne reçoit une
invitation, et elle rejoint votre compte qu'elle ait déjà un compte {{app_name}} ou non.

| plan | places incluses |
|---|---|
| {{label_free}} | {{members_free}} |
| {{label_solo}} | {{members_solo}} |
| {{label_pro}} | {{members_pro}} |
| {{label_business}} | {{members_business}} |

Les places sont **forfaitaires** : aucune facturation supplémentaire, aucun paiement au siège.
Une invitation en attente peut être révoquée, un membre peut être retiré, et un membre peut
quitter votre compte de lui-même.

## Ce qu'un membre voit

Il ouvre **vos projets** comme vous : rapports de crawl, anomalies, corrections, mots-clés,
concurrents, backlinks. Il voit aussi **les travaux en cours** sur ces projets — un crawl lancé
par un autre membre apparaît dans sa liste, et il peut le suivre, l'annuler ou le relancer.

Il continue par ailleurs de voir **ses propres projets**, s'il en a. Les deux listes coexistent.

## Qui paie, et avec quel moteur

**C'est le projet qui décide.** Sur un projet qui vous appartient, un collaborateur obtient
*votre* plan — votre moteur de correction, vos plafonds de fichiers, vos quotas — et chaque
action est débitée sur **votre** compte.

Hors projet, il reste sur le sien : son assistant, sa facturation, ses propres sites suivent son
abonnement à lui. C'est délibéré : un consultant peut être client de {{app_name}} par ailleurs,
et rejoindre une agence ne doit pas effacer son abonnement.

Chaque débit garde la trace de **qui a cliqué**, en plus du compte débité. Voir
[Plans et quotas](/docs/plans-et-quotas).

## Les connexions externes : deux règles, pas une

C'est le point le plus important de cette page, et il n'est pas symétrique.

**GitHub est un identifiant d'agence.** Un membre qui lance une correction pousse sur le dépôt
avec le **jeton GitHub du propriétaire du compte**. Les pull requests portent donc le compte de
l'agence, pas celui du collaborateur. C'est une conséquence acceptée en connaissance de cause :
la carte GitHub de l'écran le dit explicitement, pour que personne ne le découvre dans
l'historique d'un dépôt client. Voir [Connecter GitHub à un projet](/docs/connecter-github).

**Une propriété Search Console ou Bing appartient au propriétaire du site.** Ces connexions sont
donc **par projet** : chaque projet porte la sienne, et un membre peut connecter le compte Search
Console du client sur le projet de ce client. À défaut, la connexion de compte sert de repli, ce
qui évite à une agence qui n'a qu'un seul compte de tout reconnecter. Voir
[Connecter Google Search Console](/docs/connecter-search-console).

## Une seule adhésion par personne

Quelqu'un ne peut rejoindre qu'un seul compte à la fois. La raison est concrète : si vous et
l'agence possédiez chacun un projet nommé `mon-site`, l'adresse `/projects/mon-site` deviendrait
ambiguë. Dans ce cas précis, c'est **votre** projet qui s'ouvre — le seul choix qui ne surprend
personne.

## Ce que cette page ne fait pas

- Il n'y a **pas de rôles**. Un membre a les mêmes droits que vous sur les projets du compte,
  corrections et pull requests comprises. N'invitez que des personnes à qui vous confieriez
  votre dépôt.
- Les places ne s'achètent pas à l'unité. Pour en avoir plus, il faut changer de plan.
- Retirer un membre lui retire l'accès aux projets du compte, mais **ne supprime pas** les
  travaux qu'il a lancés ni les pull requests qu'il a ouvertes.
