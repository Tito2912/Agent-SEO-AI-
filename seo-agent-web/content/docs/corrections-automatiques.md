---
title: "Corrections automatiques : de l'anomalie à la pull request"
meta_title: "Corriger le code automatiquement — documentation {{app_name}}"
description: "Les trois portées de correction, ce que {{app_name}} écrit réellement dans votre dépôt, ce qu'il refuse de toucher, et comment relire une pull request."
kind: "Documentation"
section: "Corriger"
order: 22
updated_at: "2026-09-03"
audience: "Solo, Pro et Business"
keywords: ["correction automatique", "pull request", "SEO technique", "IA"]
app_href: "/"
featured: true
related: ["connecter-github", "verifier-une-correction", "anomalies-et-priorites", "plans-et-quotas"]
faq:
  - question: "Comment une correction est-elle facturée ?"
    answer: "Un fichier écrit par le modèle compte pour une unité de votre quota mensuel de corrections. Une pull request qui modifie un seul gabarit et règle 300 URLs coûte donc une unité, pas trois cents."
  - question: "Que se passe-t-il si {{app_name}} ne trouve pas le bon fichier ?"
    answer: "Il ne modifie rien et vous le dit. Le produit préfère refuser une correction plutôt que de patcher un fichier au hasard : un correctif appliqué au mauvais endroit coûte plus cher à défaire qu'à ne jamais faire."
  - question: "Le modèle peut-il inventer du contenu ?"
    answer: "Il rédige des balises — title, description, alt — à partir du contenu réel de la page, jamais d'un sujet imaginé. Pour les corrections structurelles (canonical, lien interne, redirection), la valeur écrite provient du crawl, pas du modèle."
  - question: "Puis-je annuler une correction ?"
    answer: "En mode pull request, il suffit de la fermer sans fusionner : rien n'a touché votre branche principale. En mode application directe, c'est un revert Git ordinaire."
---

C'est le cœur du produit. Un audit qui liste 230 anomalies vous laisse 230 tâches ; ici,
{{app_name}} ouvre une pull request qui les traite, et vérifie ensuite qu'elles ont disparu.

Prérequis : un [dépôt GitHub connecté](/docs/connecter-github) au projet.

## Les trois portées

Le bouton dépend de ce que vous voulez corriger. Les trois existent parce que les situations
ne se ressemblent pas.

### Une occurrence

Depuis la fiche d'une anomalie, sur une URL précise. {{app_name}} localise le fichier source
de cette page, écrit la correction, ouvre une PR.

Utile pour tester le mécanisme sur un cas que vous connaissez avant de lui faire confiance à
plus grande échelle. C'est ce que je recommande pour la toute première correction.

### Toute une anomalie

Depuis la fiche d'anomalie, bouton de correction complète. {{app_name}} prend **toutes** les
URLs touchées, remonte aux fichiers sources, et détermine s'il s'agit d'un fichier partagé ou
de fichiers page par page.

C'est ici que le produit gagne son temps. Si 300 pages n'ont pas de `meta description` parce
que le gabarit ne l'émet pas, la correction est **une** modification dans **un** fichier, et
les 300 lignes du rapport disparaissent ensemble.

Le tout arrive dans une seule pull request.

### Toutes les erreurs du crawl

Depuis la page Corrections, correction globale. {{app_name}} traite l'ensemble des erreurs du
dernier crawl et les regroupe dans une pull request unique.

Puissant, et à manier avec les yeux ouverts : la PR est plus grosse, donc plus longue à
relire. Le plafond de fichiers de votre plan s'applique, et la fréquence est limitée à cinq
par heure.

## Comment {{app_name}} trouve le fichier à modifier

C'est la partie difficile, et celle qui distingue une correction utile d'un patch au hasard.

Le produit indexe votre dépôt et reconnaît la structure des générateurs de sites courants —
Next.js, Astro, Nuxt, Jekyll, Hugo, Eleventy, Svelte, Vue, WordPress en thème, ainsi que les
fichiers de configuration d'hébergement (`netlify.toml`, `vercel.json`, `_redirects`,
`_headers`, `.htaccess`).

Puis il fait la distinction qui compte : **l'anomalie vient-elle de la page ou du gabarit ?**

- Une seule page en cause → le fichier de cette page.
- Toutes les pages en cause → le layout, le composant `<head>`, ou le fichier de
  configuration.

Quand rien ne correspond de façon certaine, {{app_name}} s'arrête et vous le signale. Une
correction non faite est un désagrément ; une correction faite dans le mauvais fichier est un
incident.

## Ce qu'il écrit, concrètement

**Les balises SERP.** `title`, `meta description`, `h1`, attributs `alt`. Rédigés à partir du
contenu réel de la page — son sujet, son intention, ses termes — et calibrés à la longueur que
les moteurs affichent réellement.

**Les signaux structurels.** `canonical` incohérente, `hreflang` réciproque manquant,
`meta robots` en `noindex` non voulu, liens internes pointant vers une redirection, boucles de
redirection sur une même URL, entrées de sitemap.

Sur cette seconde famille, le modèle n'invente rien : la valeur à écrire est celle que le
crawl a déterminée. Le rôle du modèle est de l'insérer correctement dans votre code, pas de la
choisir.

## Ce qu'il refuse de toucher

- **Les décisions éditoriales.** Réécrire un article pour le rendre meilleur n'est pas une
  correction SEO, et ce n'est pas ce que fait ce bouton.
- **Ce qui relève du serveur.** Erreurs 5xx, lenteurs d'hébergement, certificats : le rapport
  les signale, le correcteur ne s'en mêle pas.
- **Les fichiers hors liste.** Seules les extensions de gabarit et de configuration sont
  modifiables. Ni binaire, ni dépendance, ni secret.
- **Les corrections qui ne tiennent pas.** Un patch dont le produit n'arrive pas à vérifier
  qu'il s'applique proprement au fichier réel est abandonné, pas forcé.

## Relire la pull request

Le lien de la PR apparaît sur la tâche de correction, page **Corrections** du projet.

Ce qu'il faut regarder, dans l'ordre :

1. **Le diff.** Il est petit dans l'immense majorité des cas — quelques lignes. S'il est
   énorme, c'est un signal, pas une réussite.
2. **Le bon fichier.** Un layout modifié pour une anomalie de gabarit : normal. Un layout
   modifié pour corriger une seule page : à regarder de près.
3. **Vos checks.** L'intégration continue et la prévisualisation de déploiement de votre
   hébergeur, si vous en avez une, s'exécutent sur cette PR comme sur les autres. Une
   prévisualisation coûte trente secondes et confirme que la page rend correctement.

Puis vous fusionnez, ou vous fermez. Fermer une PR ne consomme rien de plus et ne laisse
aucune trace dans votre branche principale.

## Le quota, et comment il se compte

**Une unité = un fichier écrit par le modèle.**

C'est la seule règle, et elle a une conséquence agréable : la correction qui règle 300 URLs
en touchant un gabarit coûte une unité. Le quota mesure le travail du modèle, pas le nombre de
lignes du rapport.

Les quotas mensuels et le plafond de fichiers par correction dépendent du plan.

→ [Plans et quotas](/docs/plans-et-quotas)

## Et après la fusion ?

{{app_name}} ne considère pas une correction comme faite parce qu'une PR a été fusionnée. Il
attend le crawl suivant et regarde si l'anomalie est réellement partie.

→ [Vérifier une correction](/docs/verifier-une-correction)
