---
title: "Automatiser les audits et les corrections"
meta_title: "Automatisation des audits — documentation {{app_name}}"
description: "Faire tourner les crawls sans y penser, corriger toutes les erreurs en une pull request, et suivre les tâches de correction d'un projet."
kind: "Documentation"
section: "Aller plus loin"
order: 42
updated_at: "2026-10-21"
audience: "Tous les plans"
keywords: ["automatisation", "monitoring SEO", "crawl planifié", "activité"]
app_href: "/jobs"
related: ["corrections-automatiques", "verifier-une-correction", "lancer-un-crawl"]
faq:
  - question: "À quelle fréquence faut-il crawler un site ?"
    answer: "Un site vitrine stable : une fois par mois. Un blog qui publie chaque semaine : toutes les deux semaines. Un e-commerce à catalogue mouvant : chaque semaine. Et systématiquement après une mise en production importante, quel que soit le rythme."
  - question: "Un crawl automatique consomme-t-il mon quota de pages ?"
    answer: "Oui, exactement comme un crawl lancé à la main. C'est la raison pour laquelle la limite de pages par crawl mérite d'être réglée avant d'automatiser quoi que ce soit."
  - question: "Puis-je annuler un traitement en cours ?"
    answer: "Oui : ouvrez son suivi dans Activité, puis « Demander l’arrêt ». Le traitement termine son étape en cours avant de s’arrêter. Un arrêt ne garantit pas qu’un rapport partiel soit disponible ; les bilans précédents restent accessibles depuis le site."
---

Un audit ponctuel donne une photo. Une routine montre les tendances — et surtout, elle attrape
les régressions dans les jours qui suivent le déploiement qui les a introduites, plutôt que six
mois plus tard.

## L’écran Activité

**Activité**, dans la barre latérale, rassemble les traitements récents et ceux encore actifs
de vos sites : analyses d’un site, audits de plusieurs sites et autres traitements. Ce n’est pas
un historique complet. Les filtres Site, Type et État s’appliquent à cette liste.

**Voir le suivi** ouvre le détail d’un traitement : son état, son avancement et, dans
**Détails techniques et journaux**, ses journaux. Les actions se trouvent sur ce suivi :

- **Demander l’arrêt** — arrête un traitement actif, une fois son étape en cours terminée.
- **Demander une nouvelle analyse** — après un échec ou un arrêt, crée une nouvelle demande,
  soumise au quota du compte qui gère le site. Utile après une panne réseau ou un site
  momentanément indisponible.

C'est l'écran à ouvrir quand quelque chose semble bloqué. Une analyse « En cours » depuis trois
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

Page **Automatisation** d'un projet, bloc **Correction globale** : le bouton **Préparer un brouillon GitHub**.

![Écran Automatisation : dépôt GitHub connecté, correction globale en une pull request, corrections passées](/static/captures/automatisation.webp "La correction globale : toutes les anomalies corrigeables en une seule pull request.")

{{app_name}} reprend le dernier rapport, génère les correctifs pour chaque anomalie
corrigeable, et pousse l'ensemble dans une pull request unique.

En mode Vérification, la PR vous attend. En mode Full Access, les corrections déterministes
sont fusionnées automatiquement, les autres restent ouvertes — voir
[Connecter GitHub](/docs/connecter-github).

La même page affiche le tableau des tâches de correction du projet, réparties en quatre
groupes : en cours, à corriger, déclarées modifiées, ignorées. Avec pour chacune la pull request associée et le
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
