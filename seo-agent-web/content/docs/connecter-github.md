---
title: "Connecter GitHub à un projet"
meta_title: "Connecter un dépôt GitHub — documentation {{app_name}}"
description: "Autoriser {{app_name}} sur GitHub, rattacher un dépôt et une branche à un projet, et choisir entre le mode Vérification et le mode Full Access."
kind: "Documentation"
section: "Corriger"
order: 21
updated_at: "2026-09-03"
audience: "Solo, Pro et Business"
keywords: ["GitHub", "pull request", "dépôt", "OAuth"]
app_href: "/settings/accounts#github-connect-card"
related: ["corrections-automatiques", "verifier-une-correction", "fix-pack-sans-depot", "plans-et-quotas"]
faq:
  - question: "Quelles permissions {{app_name}} demande-t-il sur GitHub ?"
    answer: "La lecture du profil et de l'email, et l'accès aux dépôts. L'accès en écriture est nécessaire pour créer une branche et ouvrir une pull request ; sans lui, le produit ne peut que suggérer du texte."
  - question: "Puis-je limiter l'accès à un seul dépôt ?"
    answer: "Le jeton OAuth GitHub couvre les dépôts auxquels votre compte a accès. Si vous voulez cloisonner, la pratique habituelle est de créer un compte de service GitHub, de l'inviter uniquement sur le dépôt concerné, et de connecter celui-là."
  - question: "Que se passe-t-il si je déconnecte GitHub ?"
    answer: "Le jeton est supprimé et les boutons de correction disparaissent. Les pull requests déjà ouvertes restent sur votre dépôt : elles vous appartiennent, {{app_name}} n'y touche plus."
  - question: "Le mode « Full Access » peut-il casser mon site ?"
    answer: "Il fusionne automatiquement les corrections déterministes — jamais celles écrites par le modèle, jamais un changement de redirection. Le risque est donc borné, mais il reste : sur une branche déployée automatiquement, du code non relu part en ligne. Restez en mode Vérification si vous n'êtes pas sûr."
---

C'est la connexion qui transforme {{app_name}} d'un outil qui signale en un outil qui corrige.
Elle est facultative — et c'est la fonction pour laquelle le produit existe.

## Autoriser GitHub sur votre compte

**Paramètres → Comptes → GitHub → Connecter**. Vous partez sur GitHub, vous autorisez, vous
revenez. La carte affiche alors le compte connecté et la liste des dépôts accessibles.

Les autorisations demandées : lecture du profil et de l'e-mail, et accès aux dépôts. Cette
dernière est indispensable — créer une branche et ouvrir une pull request est une écriture.

!!! note "Un compte de service, si vous voulez cloisonner"
    Le jeton porte les droits de **votre** compte GitHub. Sur une organisation avec beaucoup
    de dépôts, la façon propre de restreindre est de créer un compte GitHub dédié, de
    l'inviter sur le seul dépôt du site, et de connecter celui-ci.

Un mode « jeton manuel » existe aussi, pour les cas où l'OAuth de plateforme n'est pas
disponible. Il fait la même chose et se configure sur la même page.

## Rattacher un dépôt à un projet

La connexion GitHub est au niveau du **compte**. Le rattachement est au niveau du **projet** :
un projet, un dépôt, une branche.

Trois réglages :

- **Dépôt** — au format `organisation/depot`. Choisissez-le dans la liste des dépôts
  accessibles.
- **Branche** — `main` par défaut. C'est la branche depuis laquelle {{app_name}} part, et la
  cible de ses pull requests.
- **Mode** — `Vérification` ou `Full Access`.

## Les deux modes, et lequel choisir

### Vérification (par défaut)

{{app_name}} crée une branche, y écrit le correctif, ouvre une pull request et vous en donne
le lien. **Rien n'est fusionné sans vous.**

Vous obtenez tout ce qu'une PR apporte : le diff ligne à ligne, vos checks d'intégration
continue, la prévisualisation de déploiement si votre hébergeur en génère une, et l'historique.

C'est le mode à garder, y compris quand vous faites confiance au résultat. La relecture d'un
diff de trois lignes coûte trente secondes.

### Full Access

La pull request est créée de la même façon, puis **fusionnée automatiquement** (en squash) —
mais seulement quand les trois conditions suivantes sont réunies :

- **aucun fichier n'a été écrit par le modèle.** Dès qu'une valeur est rédigée par l'IA plutôt
  que déduite du crawl, la PR reste ouverte et attend votre relecture ;
- **aucune règle de redirection ni fichier de configuration d'hébergement n'a été touché.** Un
  changement de routage passe toujours par un humain ;
- **la correction ne repose sur aucune hypothèse à valider.**

Autrement dit, la fusion automatique est réservée aux corrections déterministes, dont la
valeur écrite vient du crawl et non du modèle. Tout le reste vous revient, quel que soit le
mode.

Même ainsi, réservez Full Access à un site dont vous maîtrisez la chaîne de déploiement — ou à
une branche qui ne part pas directement en production.

## Ce que {{app_name}} s'interdit

Les garde-fous ne sont pas cosmétiques, parce que le produit écrit dans votre code :

- **Les noms de branche sont validés** avant tout appel à l'API — pas de `..`, pas de
  chemin absolu, pas de suffixe `.lock`, aucune des formes que Git refuse ou interprète.
- **Les extensions modifiables sont limitées** aux fichiers de gabarit et de configuration :
  HTML, JSX/TSX, Vue, Svelte, Astro, PHP, Markdown, JSON, YAML, TOML, et les fichiers de
  configuration d'hébergement. Pas de binaire, pas de fichier hors de cette liste.
- **Le nombre de fichiers touchés par correction est plafonné**, et ce plafond dépend de votre
  plan. Une correction qui voudrait réécrire quarante fichiers s'arrête à la limite plutôt que
  de produire une PR que personne ne relira.
- **Les appels sont limités en fréquence** : correction unitaire et correction d'anomalie
  complète sont plafonnées à 20 par heure, la correction globale à 5 par heure.

## Netlify

Une connexion Netlify existe également, dans la même page. Elle sert à lister les sites de
votre compte Netlify pour les rattacher à des projets — pas à déployer. Le déploiement reste
piloté par votre chaîne habituelle, déclenchée par la fusion de la pull request.

## Vérifier que tout est en place

Sur la page **Corrections** du projet, la bannière du haut indique le dépôt, la branche et le
mode. Si elle affiche « aucun dépôt connecté », les boutons de correction resteront absents
des fiches d'anomalie.

→ [Corrections automatiques](/docs/corrections-automatiques)
