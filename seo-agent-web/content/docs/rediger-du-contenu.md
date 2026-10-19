---
title: "Rédiger un article et préparer sa publication"
meta_title: "Rédaction de contenu SEO — documentation {{app_name}}"
description: "Rédiger un article à relire en HTML et Markdown ou préparer une proposition GitHub ; comprendre les quotas par langue et les étapes de publication."
kind: "Documentation"
section: "Aller plus loin"
order: 44
updated_at: "2026-10-19"
audience: "Pro et Business"
keywords: ["rédaction IA", "contenu SEO", "page neuve", "pull request", "mode automatique"]
app_href: "/"
related: ["analyser-les-concurrents", "connecter-github", "verifier-une-correction", "plans-et-quotas"]
faq:
  - question: "Pourquoi n'y a-t-il pas d'aperçu avant la pull request ?"
    answer: "Parce que la pull request EST l'aperçu. Elle s'ouvre en brouillon : vous lisez le diff complet, le build de votre dépôt se prononce, et rien n'est publié tant que vous ne fusionnez pas. Un aperçu séparé ferait payer un brouillon jeté, ou laisserait engendrer des pages gratuitement en boucle."
  - question: "L'agent peut-il écrire sur un sujet que mon site traite déjà ?"
    answer: "Le mode automatique retient les sujets non repérés dans vos pages analysées. Vérifiez le périmètre de l’analyse : une page peut exister ailleurs sur votre site. En saisie libre, vous choisissez le sujet."
  - question: "Combien de pages l'agent écrit-il tout seul ?"
    answer: "Selon la fréquence choisie : une page par mois, toutes les deux semaines, chaque semaine ou deux fois par semaine. Chaque version linguistique compte dans le quota mensuel du compte qui gère le projet."
  - question: "Que se passe-t-il si l'agent ne sait pas où poser la page ?"
    answer: "Il refuse et vous le dit. Créer une arborescence chez vous sur une supposition est pire que de dire qu'on ne sait pas. Il lui faut au moins une page existante dans la même section pour en transposer la convention."
---

{{app_name}} prépare un texte à relire à partir d’un sujet et d’une rubrique existante. Choisissez **Contenu seul** pour obtenir du Markdown et du HTML à publier vous-même, ou une **proposition GitHub en brouillon** si le code du site est connecté.

## Où trouver l'écran

**Contenu**, dans le menu d’un projet, à partir du plan Pro. Le mode **Contenu seul** demande une analyse du site pour connaître ses rubriques et ses pages. Il fonctionne avec votre CMS sans dépôt GitHub. Le mode **Proposition GitHub** et la rédaction automatique demandent un [dépôt connecté](/docs/connecter-github).

## Écrire une page à la main

Choisissez la destination, le **sujet**, sa **rubrique**, puis la longueur, le ton et les langues si nécessaire. Le récapitulatif indique le nombre de versions et leur coût.

En mode **Contenu seul**, relisez l’aperçu isolé ou le texte en Markdown, puis copiez ou téléchargez la version choisie. Aucune copie ne publie l’article : insérez-le ensuite dans votre CMS et contrôlez la page en ligne.

En mode **Proposition GitHub**, renseignez aussi une **adresse**. L'adresse est suggérée à partir du sujet, et vous
la corrigez — la section (`/blog`, `/guides`, `/ressources`) est une décision éditoriale que
personne ne peut deviner à votre place.

![Formulaire de rédaction : destination, sujet, taille, ton, section du site, adresse et langues](/static/captures/rediger-une-page.webp "Le formulaire de rédaction d'une page.")

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

![Réglages du mode automatique : section, fréquence, jour, heure, fuseau, taille, ton, traductions et publication](/static/captures/mode-automatique.webp "Les réglages du mode automatique.")

La rédaction suit votre fréquence, votre jour, votre heure et votre fuseau, dans les quotas disponibles. Elle ne prépare une page que :

- sur un sujet qu'un de vos concurrents traite et qui n’a pas été repéré dans les pages analysées de votre site — voir
  [Analyser les concurrents](/docs/analyser-les-concurrents) ;
- si un crawl de votre propre site existe, sans quoi tout sujet paraîtrait non couvert ;
- sur un sujet **jamais proposé auparavant**, même si vous aviez fermé sa pull request ;
- et **seulement s'il sait lier la page**. C'est la seule différence avec le mode manuel : ici
  personne ne lira l'avertissement, donc une page qu'il ne sait pas relier n'est pas écrite du
  tout.

La publication se règle séparément : **avec votre validation**, la proposition attend votre relecture puis votre autorisation de fusion ; **fusion automatique après contrôles**, elle peut être fusionnée lorsque les vérifications du dépôt passent. Sans contrôle exploitable ou en cas d’échec, une validation est demandée.

Une proposition validée peut donc rester en attente. Une proposition fusionnée n’est pas une preuve de mise en ligne : celle-ci dépend du déploiement de votre site.

Un compte dont le forfait a changé peut toujours **éteindre** le mode, quel que soit son plan.

## Ce que ça coûte

Chaque **version linguistique vaut une unité**, quelle que soit la taille de la page et le nombre de fichiers
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

- La rédaction **manuelle** ne publie rien : vous copiez le contenu dans votre CMS, ou relisez la proposition GitHub en brouillon avant sa fusion. La rédaction **automatique** suit le réglage de publication explicite décrit ci-dessus.
- Elle ne **vérifie pas les faits**. Chiffres, noms, dates, prix et promesses commerciales sont
  à contrôler avant de fusionner. Le texte est plausible, il n'est pas vérifié.
- Elle n'écrit pas la **première** page d'une section. Sans page sœur, la convention de votre
  site pour cette section n'existe pas encore, et l'agent s'abstient plutôt que d'en inventer une.

Une fois la page fusionnée, [vérifier qu'une correction a vraiment fonctionné](/docs/verifier-une-correction)
s'applique à elle comme au reste : c'est le crawl suivant qui dit si elle est bien en ligne,
indexable et liée.
