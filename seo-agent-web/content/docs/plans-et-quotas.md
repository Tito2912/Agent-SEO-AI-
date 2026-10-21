---
title: "Plans et quotas"
meta_title: "Plans, limites et quotas — documentation {{app_name}}"
description: "Ce que chaque plan {{app_name}} inclut : projets, pages crawlées, corrections IA, Core Web Vitals, et comment chaque quota se compte réellement."
kind: "Référence"
section: "Compte et facturation"
order: 50
updated_at: "2026-10-21"
audience: "Tous les plans"
keywords: ["tarifs", "quotas", "limites", "abonnement"]
app_href: "/billing"
related: ["corrections-automatiques", "lancer-un-crawl", "rediger-du-contenu", "comptes-d-equipe"]
faq:
  - question: "Comment se compte une correction IA ?"
    answer: "Un fichier écrit par le modèle égale une unité — sur WordPress, un titre ou une méta-description rédigé pour une page. Une pull request qui modifie un seul gabarit et règle 300 URLs coûte donc une unité : le quota mesure le travail du modèle, pas le nombre de lignes du rapport."
  - question: "Que se passe-t-il quand j'atteins un quota ?"
    answer: "L'action concernée est refusée avec un message qui nomme le quota atteint, et un lien vers la page Abonnement. Rien d'autre ne s'arrête : un quota de corrections épuisé ne bloque ni les crawls, ni les rapports, ni les exports."
  - question: "Les quotas se reportent-ils d'un mois sur l'autre ?"
    answer: "Non. Les compteurs mensuels repartent à zéro le 1er de chaque mois à 00:00 UTC, indépendamment de la date de renouvellement de votre abonnement."
  - question: "Puis-je changer de plan en cours de mois ?"
    answer: "Oui, depuis la page Abonnement. Une montée de plan prend effet immédiatement ; une descente est programmée pour la fin de la période en cours et reste visible d'ici là."
---

Les chiffres de cette page sont ceux réellement appliqués par le produit, et non une copie du
tarif : ils sont lus depuis la configuration en vigueur au moment où vous consultez cette
page.

## Le tableau

| | Free | Solo | Pro | Business |
| --- | --- | --- | --- | --- |
| **Prix** | {{price_free}} | {{price_solo}} | {{price_pro}} | {{price_business}} |
| **Projets** | {{projects_free}} | {{projects_solo}} | {{projects_pro}} | {{projects_business}} |
| **Pages crawlées / mois** | {{pages_free}} | {{pages_solo}} | {{pages_pro}} | {{pages_business}} |
| **Pages max / crawl** | {{maxpages_free}} | {{maxpages_solo}} | {{maxpages_pro}} | {{maxpages_business}} |
| **Corrections IA / mois** | {{corrections_free}} | {{corrections_solo}} | {{corrections_pro}} | {{corrections_business}} |
| **Fichiers max / correction** | {{files_free}} | {{files_solo}} | {{files_pro}} | {{files_business}} |
| **URLs Core Web Vitals / crawl** | {{pagespeed_free}} | {{pagespeed_solo}} | {{pagespeed_pro}} | {{pagespeed_business}} |
| **Articles rédigés / mois** | {{articles_free}} | {{articles_solo}} | {{articles_pro}} | {{articles_business}} |
| **Messages assistant / mois** | {{assistant_free}} | {{assistant_solo}} | {{assistant_pro}} | {{assistant_business}} |
| **Places d'équipe** | {{members_free}} | {{members_solo}} | {{members_pro}} | {{members_business}} |
| **Opportunités backlinks** | — | ✓ | ✓ | ✓ |
| **Concurrents** | — | — | ✓ | ✓ |

## Comment chaque quota se compte

### Corrections IA — la règle qui compte

**Dans GitHub : une unité = un fichier écrit par le modèle.**

Une correction qui règle 300 URLs en modifiant un seul gabarit coûte **une** unité. Le quota
mesure le travail du modèle, pas la taille du problème.

Sur WordPress, la même règle s'applique page par page : chaque titre ou méta-description que
{{app_name}} propose compte pour une unité dès la préparation, avant son enregistrement sur
votre site. Vous relisez et confirmez toujours cet enregistrement. Un texte que vous écrivez
vous-même et les réécritures sans IA ne consomment pas ce quota.
→ [Corriger dans WordPress](/docs/corriger-dans-wordpress)

En pratique, un site de taille moyenne consomme quelques dizaines d'unités le premier mois —
celui où l'on rattrape le retard — puis nettement moins ensuite, une fois les anomalies de
gabarit réglées.

Le plan Free inclut {{corrections_free}} corrections IA par mois, sur un dépôt GitHub compatible
ou avec WordPress et Yoast SEO,
ainsi que l'audit et les exports PDF et CSV. Le fix-pack sans dépôt est disponible dès le plan
Solo. Les limites réellement disponibles figurent dans le tableau ci-dessus.

### Articles rédigés — un compteur à part

**Une unité = une page écrite**, quelle que soit sa taille et quel que soit le nombre de
fichiers touchés : l'index de section modifié au passage pour que la page ne soit pas orpheline
n'est pas un second article.

Ce compteur est **séparé de celui des corrections**, délibérément. Un article n'a pas de commune
mesure avec une réécriture de titre, et les mélanger rendrait les deux quotas illisibles :
publier deux pages ne doit pas vous priver de vos corrections du mois.

Les plafonds sont volontairement bas. La politique anti-spam de Google vise le contenu produit
en masse pour le classement, quelle que soit la façon dont il est produit — un plafond généreux
ferait de cette fonction un moyen de nuire au site qu'elle doit servir. Voir
[Rédiger une page et la proposer en pull request](/docs/rediger-du-contenu).

### Places d'équipe

Une place permet d'inviter une personne sur vos projets. Elles sont **forfaitaires** : aucune
facturation supplémentaire, aucun paiement au siège. Ce que consomme un collaborateur sur un de
vos projets est débité sur **votre** compte, avec la trace de qui a cliqué. Voir
[Comptes d'équipe](/docs/comptes-d-equipe).

### Fichiers par correction

Le plafond de fichiers qu'une seule correction peut toucher. Il protège contre la pull request
de quarante fichiers que personne ne relira.

Ce plafond est aussi ramené au quota restant : s'il vous reste 3 unités, une correction qui
voudrait toucher 12 fichiers s'arrête à 3.

### Pages crawlées

Le total mensuel de pages HTML explorées, tous projets confondus. Les ressources — images, CSS,
scripts — n'y entrent pas, sauf si vous activez leur vérification, et elles sont alors
plafonnées séparément.

Le plafond réel d'un crawl est le plus petit de trois nombres : la limite du projet, la limite
du plan par crawl, et ce qu'il reste du quota mensuel.

### URLs Core Web Vitals

Le nombre d'URLs mesurées par PageSpeed à chaque crawl. La limite vient de l'API de Google,
qui est lente et contingentée par jour, et cette limite est partagée par toute la plateforme.

Le produit mesure les premières pages HTML valides par ordre alphabétique. Vous réglez
leur nombre dans les réglages de l’analyse, dans la limite de votre formule.

→ [Suivre la performance](/docs/suivre-la-performance)

### Concurrents

À partir du plan Pro. Jusqu'à {{competitors_max}} concurrents par projet,
{{competitor_pages}} pages explorées par concurrent, et un rafraîchissement automatique tous
les {{competitor_refresh_days}} jours.

Le seuil est plus haut que pour les autres modules parce qu'un crawl de concurrent dépense du
temps de traitement sur un site qui n'est pas le vôtre.

→ [Analyser les concurrents](/docs/analyser-les-concurrents)

### Opportunités de backlinks

À partir du plan Solo, avec deux quotas mensuels distincts : les recherches et les brouillons
de messages. Ils sont volontairement bas — le netlinking utile se compte en quelques contacts
pertinents, pas en centaines d'envois.

→ [Backlinks](/docs/backlinks)

## Ce qui n'est jamais limité

- La lecture des rapports et de l'historique.
- Tous les exports CSV et PDF. Le fix-pack est disponible dès Solo, sans quota mensuel propre.
- Les suggestions de contenu que vous copiez-collez vous-même.
- La consultation des anomalies et de leurs URLs.

## Atteindre un quota

L'action concernée est refusée avec un message qui nomme le quota atteint et pointe vers la
page **Abonnement**. Le reste continue de fonctionner : un quota de corrections épuisé
n'empêche ni les crawls, ni les rapports, ni les exports.

Les compteurs mensuels utilisent le mois civil en UTC : ils repartent à zéro le 1er de chaque
mois à 00:00 UTC et ne se reportent pas. Cette date est distincte du renouvellement de votre
abonnement. La page **Abonnement** indique le mois suivi et les six consommations mensuelles :
pages, messages de l’assistant, corrections, articles, recherches de liens entrants et brouillons
de messages pour ces liens. Les sites sont une capacité active, pas une consommation mensuelle.

## Quel compte est concerné ?

La page **Abonnement** présente la formule, les compteurs et les factures de votre compte personnel. Si vous venez d’un site partagé, le nom du propriétaire et sa formule sont indiqués séparément : acheter une formule pour vous ne change pas les limites de son site. Contactez ce propriétaire pour faire évoluer son abonnement ; le bouton de retour conserve le site concerné.

Vos préférences, votre pays, votre mot de passe et votre choix concernant l’origine de l’inscription se trouvent dans **Mon compte**. Les connexions Google, Bing, GitHub et Netlify restent accessibles plus bas, par les raccourcis de cette page. Pour les réglages d’un site précis, ouvrez son menu.

## Changer de plan

Page **Abonnement**. Une montée de plan prend effet immédiatement. Une descente est programmée
pour la fin de la période en cours et reste affichée jusque-là — vous gardez donc ce que vous
avez payé jusqu'au bout.
