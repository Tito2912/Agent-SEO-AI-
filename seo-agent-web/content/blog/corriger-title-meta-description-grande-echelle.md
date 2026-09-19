---
slug: "corriger-title-meta-description-grande-echelle"
title: "Corriger les title et meta descriptions à grande échelle"
meta_title: "Corriger title et meta description en masse"
description: "Traiter 400 balises manquantes ou dupliquées sans y passer trois semaines : distinguer le gabarit de la page, et corriger à la source."
kind: "Tutoriel"
updated_at: "2026-09-03"
published_at: "2026-05-13"
audience: "Sites de services, e-commerce et blogs"
keywords: ["balise title", "meta description", "duplicate title", "SEO on-page"]
related: ["audit-seo-technique-checklist-priorites", "connecter-google-search-console-audit-mensuel"]
cta: "{{app_name}} identifie le gabarit responsable, écrit la correction dans votre code et ouvre une pull request — une modification pour 300 URLs, relue par vous avant fusion."
faq:
  - question: "Quelle longueur pour un title ?"
    answer: "Google affiche environ 580 pixels, soit 55 à 60 caractères selon les lettres utilisées. Compter en caractères est une approximation commode ; l'important est que l'information décisive soit au début, pas à la fin."
  - question: "La meta description influence-t-elle le classement ?"
    answer: "Non, pas directement. Elle influence le taux de clic, qui décide si votre position se transforme en visite. Sur une page en position 4, c'est la différence entre 3 % et 8 % de clics."
  - question: "Google réécrit mes titles, à quoi bon les soigner ?"
    answer: "Il les réécrit surtout quand ils sont mauvais : trop courts, trop répétitifs, ou sans rapport avec la requête. Un title précis et pertinent est conservé dans la grande majorité des cas."
---

Un audit remonte 400 `title` dupliqués et 250 `meta description` manquantes. Traité page par
page, c'est trois semaines. Traité correctement, c'est une journée.

La différence tient à une seule question, posée avant d'écrire la moindre balise.

## La question : gabarit ou page ?

**Si toutes les pages d'un même type sont touchées**, la cause est dans le gabarit. Une fiche
produit sans description, ça arrive ; 1 200 fiches produit sans description, c'est le template
qui n'émet pas la balise.

**Si quelques pages éparses sont touchées**, c'est du contenu, et il faut les traiter une par
une.

Faites ce tri avant toute chose. Il détermine si votre chantier est d'une heure ou de trois
semaines, et la plupart des audits qui « prennent trois semaines » n'ont simplement pas posé la
question.

Un test rapide : triez votre export par type d'URL. Si les anomalies se concentrent sur un
motif — `/produit/…`, `/blog/…`, `/categorie/…` — vous tenez le gabarit.

## Corriger un gabarit : le patron générique

Sur un gabarit, on n'écrit pas une balise : on écrit une **formule**, avec un repli quand une
donnée manque.

```
Title      : {nom de la page} - {marque}
Fiche      : {produit} - {catégorie} | {marque}
Catégorie  : {catégorie} : {promesse} | {marque}
Article    : {titre de l'article} - {blog}
```

Trois règles qui font la différence :

1. **L'information décisive en premier.** « Marque | Catégorie | Nom du produit » gaspille les
   trente premiers caractères, ceux qui sont toujours affichés.
2. **Un repli explicite.** Si la variable est vide, le gabarit doit produire quelque chose de
   sensé, pas « - | ».
3. **Une variable qui garantit l'unicité.** Sans elle, la formule produit 200 titres
   identiques — vous avez déplacé le problème, pas résolu.

## L'ordre de traitement

Toutes les balises manquantes ne se valent pas.

**D'abord les pages qui ont des impressions.** Search Console vous les donne. Une description
manquante sur une page à 12 000 impressions coûte des clics tous les jours ; sur une archive à
zéro impression, elle ne coûte rien.

**Ensuite les pages business.** Services, catégories, comparatifs, pages locales. Elles ont
peut-être peu d'impressions justement parce qu'elles sont mal titrées.

**Ensuite les doublons visibles.** Deux pages avec le même `title` se concurrencent dans
l'index, et Google en choisit une — rarement celle que vous auriez choisie.

**Le reste attend.** Une description manquante sur une page de tag sans potentiel n'est pas un
chantier.

## Écrire les balises restantes

Pour les pages qui échappent au gabarit, quatre critères :

- **Reprendre l'intention, pas le mot-clé.** Une page qui répond à « combien coûte X » doit
  contenir un prix ou une fourchette dans son `title`.
- **Rester dans la longueur affichée.** Environ 55-60 caractères pour le `title`, 150-160 pour
  la description. Au-delà, c'est tronqué.
- **Ne pas répéter le `h1` mot pour mot.** Ce sont deux surfaces différentes : le `title` est
  lu dans une liste de dix résultats concurrents, le `h1` est lu par quelqu'un déjà arrivé.
- **Donner une raison de cliquer** dans la description : ce que la page apporte, pas ce qu'elle
  contient.

## Vérifier que ça a marché

Deux vérifications, à deux échelles de temps.

**Immédiatement** : relancez un crawl. Les anomalies visées doivent avoir disparu, et le
compteur d'anomalies **nouvelles** doit rester à zéro. Une formule de gabarit mal écrite peut
créer 400 titres identiques là où il y avait 400 titres absents.

**Trois à six semaines plus tard** : Search Console, sur les pages concernées. Le taux de clic
est le seul juge. Attention au décalage — Google publie avec deux à trois jours de retard, et
il lui faut réexplorer la page avant même ça.

## Le piège de l'automatisation intégrale

Générer 1 200 descriptions par un modèle et les publier sans relecture produit 1 200 phrases
correctes et interchangeables. Ce n'est pas mieux que 1 200 descriptions absentes — Google
réécrira les deux.

La bonne répartition : **la formule pour le volume, la main pour les pages qui comptent.** Vos
vingt pages business méritent vingt minutes chacune ; vos 1 200 fiches produit méritent une
bonne formule.
