---
title: "Rédiger une page et la proposer en pull request"
meta_title: "Rédaction de contenu SEO — documentation {{app_name}}"
description: "Faire écrire une page neuve par l'agent, dans la forme de votre site, liée à sa section, et la recevoir en pull request brouillon sur votre dépôt."
kind: "Documentation"
section: "Aller plus loin"
order: 44
updated_at: "2026-09-21"
audience: "Pro et Business"
keywords: ["rédaction IA", "contenu SEO", "page neuve", "pull request", "mode automatique"]
app_href: "/"
related: ["analyser-les-concurrents", "connecter-github", "verifier-une-correction", "plans-et-quotas"]
faq:
  - question: "Pourquoi n'y a-t-il pas d'aperçu avant la pull request ?"
    answer: "Parce que la pull request EST l'aperçu. Elle s'ouvre en brouillon : vous lisez le diff complet, le build de votre dépôt se prononce, et rien n'est publié tant que vous ne fusionnez pas. Un aperçu séparé ferait payer un brouillon jeté, ou laisserait engendrer des pages gratuitement en boucle."
  - question: "L'agent peut-il écrire sur un sujet que mon site traite déjà ?"
    answer: "En mode automatique, non : il ne prend que des sujets qu'un concurrent traite et qu'aucune de vos pages ne couvre. Deux pages qui se disputent la même requête se cannibalisent. En saisie libre, vous restez maître du sujet."
  - question: "Combien de pages l'agent écrit-il tout seul ?"
    answer: "Une par semaine et par projet au maximum, et jamais au-delà de votre quota mensuel. Ces plafonds sont bas volontairement : la politique anti-spam de Google vise le contenu produit en masse pour le classement, quelle qu'en soit la fabrication."
  - question: "Que se passe-t-il si l'agent ne sait pas où poser la page ?"
    answer: "Il refuse et vous le dit. Créer une arborescence chez vous sur une supposition est pire que de dire qu'on ne sait pas. Il lui faut au moins une page existante dans la même section pour en transposer la convention."
---

L'agent écrit une page neuve **dans la forme de votre site**, l'ajoute à l'index de sa section
pour qu'elle ne soit pas orpheline, et vous l'envoie en **pull request brouillon**. Rien n'est
publié sans votre fusion.

## Où trouver l'écran

**Contenu**, dans le menu de gauche d'un projet. Il demande un dépôt GitHub connecté : une page
neuve est un fichier ajouté à votre dépôt, et sans dépôt il n'y a nulle part où l'écrire. Voir
[Connecter GitHub à un projet](/docs/connecter-github).

## Écrire une page à la main

Deux champs : un **sujet** et une **adresse**. L'adresse est suggérée à partir du sujet, et vous
la corrigez — la section (`/blog`, `/guides`, `/ressources`) est une décision éditoriale que
personne ne peut deviner à votre place.

Ce qui se passe ensuite, dans l'ordre :

1. L'agent lit l'arborescence de votre dépôt et **transpose le chemin d'une page sœur** — une
   page existante de la même section. Il n'essaie pas de deviner la convention de votre
   générateur : il recopie celle que vous utilisez déjà.
2. Il lit cette page sœur et fait écrire la nouvelle **dans sa forme** : mêmes clés de tête,
   même langue, mêmes balises. Une page à qui il manquerait une clé que ses sœurs portent est
   refusée, pas publiée.
3. Il **ajoute l'entrée** dans l'index de la section, en clonant l'entrée d'une sœur.
4. Il ouvre la pull request **en brouillon**.

### Ce que l'agent vérifie avant d'écrire

| contrôle | ce qu'il refuse |
|---|---|
| Placement | une section sans aucune page existante, ou une adresse déjà servie par votre site |
| Clés de tête | une page qui n'a pas les clés que ses sœurs portent — elle casserait votre build |
| Relecture du fichier | un fichier déséquilibré : accolades, balises, front matter |
| Adresses | un `canonical` ou un `og:url` qui ne désigne pas la page elle-même |
| Échappements | un antislash posé dans du texte de balisage, où il s'afficherait tel quel |

Les deux derniers existent parce qu'ils se sont produits : un modèle recopie volontiers le
`canonical` de la page qu'on lui montre, et un build vert ne voit pas une adresse qui ment.

## Le lien depuis la section

Une page que rien ne pointe est orpheline, et votre prochain crawl la signalerait. L'agent
distingue trois cas, et il vous dit lequel dans le corps de la pull request :

- **l'index cite ses pages à la main** → l'entrée est ajoutée, clonée sur celle d'une sœur ;
- **la liste est engendrée** (un gabarit qui parcourt le dossier) → il n'y a rien à ajouter ;
- **il ne sait pas** (index ambigu, illisible, absent) → la pull request s'ouvre quand même, en
  disant en toutes lettres que la page n'est liée depuis nulle part.

Le journal des pages proposées, en bas de l'écran, garde cette information : une ligne
**« à lier à la main »** signale une page dont l'entrée reste à poser.

## Le mode automatique

Éteint par défaut. Pour l'allumer il faut **nommer la section** où les pages iront : personne ne
relira l'adresse avant la pull request, et `/blog` contre `/guides` ne se devine pas.

Une fois allumé, l'agent écrit **au plus une page par semaine et par projet**, et seulement :

- sur un sujet qu'un de vos concurrents traite et qu'**aucune de vos pages ne couvre** — voir
  [Analyser les concurrents](/docs/analyser-les-concurrents) ;
- si un crawl de votre propre site existe, sans quoi tout sujet paraîtrait non couvert ;
- sur un sujet **jamais proposé auparavant**, même si vous aviez fermé sa pull request ;
- et **seulement s'il sait lier la page**. C'est la seule différence avec le mode manuel : ici
  personne ne lira l'avertissement, donc une page qu'il ne sait pas relier n'est pas écrite du
  tout.

Un compte dont le forfait a changé peut toujours **éteindre** le mode, quel que soit son plan.

## Ce que ça coûte

Un article vaut **une unité**, quelle que soit la taille de la page et le nombre de fichiers
touchés — l'index modifié au passage n'est pas un second article. Le compteur est **séparé de
celui des corrections** : publier deux pages ne vous prive pas de vos corrections du mois.

| plan | articles par mois |
|---|---|
| {{label_free}} | {{articles_free}} |
| {{label_solo}} | {{articles_solo}} |
| {{label_pro}} | {{articles_pro}} |
| {{label_business}} | {{articles_business}} |

Ces plafonds sont volontairement bas. La politique anti-spam de Google vise le **contenu produit
en masse** dont le but premier est le classement, quelle que soit la façon dont il est produit.
Un plafond généreux ferait de cette fonction un moyen de nuire au site qu'elle doit servir.

## Ce que cette page ne fait pas

- Elle ne **publie** rien. La pull request reste en brouillon et n'est jamais fusionnée
  automatiquement, quel que soit le mode du projet — tout le texte vient d'un modèle.
- Elle ne **vérifie pas les faits**. Chiffres, noms, dates, prix et promesses commerciales sont
  à contrôler avant de fusionner. Le texte est plausible, il n'est pas vérifié.
- Elle n'écrit pas la **première** page d'une section. Sans page sœur, la convention de votre
  site pour cette section n'existe pas encore, et l'agent s'abstient plutôt que d'en inventer une.

Une fois la page fusionnée, [vérifier qu'une correction a vraiment fonctionné](/docs/verifier-une-correction)
s'applique à elle comme au reste : c'est le crawl suivant qui dit si elle est bien en ligne,
indexable et liée.
