---
title: "Automatiser les audits et les corrections"
meta_title: "Automatisation des audits — documentation {{app_name}}"
description: "Faire tourner les crawls sans y penser, corriger toutes les erreurs en une pull request, et suivre les tâches de correction d'un projet."
kind: "Documentation"
section: "Aller plus loin"
order: 42
updated_at: "2026-09-03"
audience: "Tous les plans"
keywords: ["automatisation", "monitoring SEO", "crawl planifié", "jobs"]
app_href: "/jobs"
related: ["corrections-automatiques", "verifier-une-correction", "lancer-un-crawl"]
faq:
  - question: "À quelle fréquence faut-il crawler un site ?"
    answer: "Un site vitrine stable : une fois par mois. Un blog qui publie chaque semaine : toutes les deux semaines. Un e-commerce à catalogue mouvant : chaque semaine. Et systématiquement après une mise en production importante, quel que soit le rythme."
  - question: "Un crawl automatique consomme-t-il mon quota de pages ?"
    answer: "Oui, exactement comme un crawl lancé à la main. C'est la raison pour laquelle la limite de pages par crawl mérite d'être réglée avant d'automatiser quoi que ce soit."
  - question: "Puis-je annuler un traitement en cours ?"
    answer: "Oui, depuis la page Jobs. Un crawl annulé conserve ce qu'il a déjà collecté, mais son rapport est partiel : traitez-le comme un sondage, pas comme un audit."
---

Un audit ponctuel donne une photo. Une routine montre les tendances — et surtout, elle attrape
les régressions dans les jours qui suivent le déploiement qui les a introduites, plutôt que six
mois plus tard.

## La page Jobs

Tous les traitements du compte, en cours et passés : crawls, corrections, analyses de
concurrents, recherches d'opportunités.

Chaque job affiche son état, sa durée, ses journaux, et deux boutons :

- **Annuler** — arrête un traitement en cours. Ce qui a été collecté est conservé.
- **Relancer** — rejoue le même traitement avec les mêmes paramètres. Utile après une panne
  réseau ou un site momentanément indisponible.

C'est la page à ouvrir quand quelque chose semble bloqué. Un crawl « en cours » depuis trois
heures sur un site de 40 pages a un problème, et ses journaux le nomment.

## Faire tourner les crawls régulièrement

Un crawl périodique est ce qui rend le reste du produit utile :

- la [vérification des corrections](/docs/verifier-une-correction) a besoin d'un crawl
  postérieur au déploiement pour confirmer quoi que ce soit ;
- la comparaison entre deux crawls n'existe que s'il y a deux crawls ;
- une régression introduite par une mise en production ne se voit que si quelqu'un regarde.

Les rythmes qui marchent, par type de site :

| Type de site | Rythme | Pourquoi |
| --- | --- | --- |
| Vitrine stable | Mensuel | Peu de changements entre deux crawls |
| Blog actif | Toutes les 2 semaines | Chaque publication peut introduire une anomalie de gabarit |
| E-commerce | Hebdomadaire | Catalogue mouvant, produits retirés, facettes |
| Refonte en cours | Après chaque mise en production | C'est là que les régressions arrivent |

!!! note "Le crawl qui compte le plus"
    Celui qui suit une mise en production. Une refonte qui ajoute 40 anomalies est une
    régression, même si le site est parfait dans le navigateur.

## Corriger en lot

Page **Automatisation** d'un projet : le bouton **Tout corriger en une PR**.

{{app_name}} reprend le dernier rapport, génère les correctifs pour chaque anomalie
corrigeable, et pousse l'ensemble dans une pull request unique.

En mode Vérification, la PR vous attend. En mode Full Access, les corrections déterministes
sont fusionnées automatiquement, les autres restent ouvertes — voir
[Connecter GitHub](/docs/connecter-github).

La même page affiche le tableau des tâches de correction du projet, réparties en quatre
colonnes : à faire, en cours, fait, ignoré. Avec pour chacune la pull request associée et le
résultat de la vérification post-crawl.

Deux limites à connaître : la correction globale est plafonnée à cinq déclenchements par
heure, et le nombre de fichiers modifiables par correction dépend de votre plan.

## Automatiser la recherche de backlinks

Réglable par projet, désactivée par défaut : mots-clés à surveiller, sources, fréquence
quotidienne ou hebdomadaire, nombre maximum de trouvailles par passage, rédaction automatique
des brouillons ou non.

Laissez-la désactivée jusqu'à ce que vous ayez vérifié à la main que les opportunités
remontées correspondent à votre site.

→ [Backlinks](/docs/backlinks)

## Ce que l'automatisation ne remplace pas

Elle fait tourner les crawls, prépare les corrections, vérifie les résultats et signale les
régressions.

Elle ne décide pas quelles pages méritent votre temps, n'écrit pas votre stratégie éditoriale,
et ne fusionne pas une pull request dont le contenu vient d'un modèle. Ces trois-là restent
des décisions.
