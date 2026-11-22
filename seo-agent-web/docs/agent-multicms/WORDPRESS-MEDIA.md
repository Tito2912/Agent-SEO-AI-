# C085 — article réel et images WordPress

Le 22 novembre 2026, la recette locale éprouve WordPress 6.9.12 / PHP 8.3 et
Yoast 28.7, avec comptes, projets, article et image fictifs. Le HTML vient de
`_versions_servies`, pas d’un paragraphe simplifié. Le rapport
`WORDPRESS-MEDIA-LOCAL-20261122.json` décrit les sources exactes et les contrôles.
Aucune publication sur un CMS client ni liaison HTTPS externe n’est réalisée.

## Corrections après la revue Claude du 20 novembre

- Validation pure du document complet et de tous les fichiers images avant le
  premier transfert : un format refusé n’abandonne plus une image déjà envoyée.
- Conservation des intertitres et identifiants, tableaux, alignements et classes
  de langage autorisées. Seul `style="text-align: left|center|right"` devient
  l’attribut `align` équivalent ; les attributs de présentation générés pour les
  images (`loading`, `decoding`) sont retirés. Le reste reste strictement validé,
  sans autoriser JavaScript, événements, styles arbitraires ni références privées.
- Le premier H1 devient le titre de l’article CMS, sans suffixe SEO de marque.
  Le thème fournit son H1 ; les autres H1 éventuels deviennent H2. Le contenu
  éditorial, les textes alternatifs et les dimensions d’image sont conservés.
- Wix accepte les intertitres simples sans leurs identifiants générés ; un lien
  vers ces identifiants ou un tableau est refusé avant tout transfert. La
  conversion Ricos ne perd aucun de ces éléments silencieusement.
- Si WordPress confirme un identifiant de média mais renvoie une URL publique
  inutilisable, cet identifiant reste dans le transfert et le reçu. L’URL refusée
  n’est pas conservée ; la tentative reste bloquée sans second envoi aveugle.
- Les corrections manuelles de titre/méta, seules ou par lot, demandent une case
  explicite. L’acteur réel, la destination et l’empreinte sont persistés dans le
  registre cinq ans avant écriture. Une panne d’enregistrement empêche l’envoi.
- Le correctif Markdown de `main` (`ea5d794`) est intégré avec ses 35 tests :
  aucun `attr_list`, événement HTML ni protocole actif dans les articles.

L’avertissement long de rédaction planifiée demandé par Tony est rétabli en
français/anglais (version du 22/11). Les textes publics approuvés de Claude
`66c20a6` sont intégrés sans réécriture ; la date de mise à jour sera celle de
leur déploiement effectif. Ils ne sont pas mis en ligne par cette livraison.

## Preuve native locale

Le formulaire sans accord média n’envoie rien. Avec cet accord, les deux
occurrences de l’image déclenchent un seul transfert ; rouvrir l’aperçu le
réutilise. Deux autres accords distincts créent le brouillon puis publient.
Le post distant a le titre attendu, les intertitres, le tableau aligné et les
deux images. La page publique porte un seul H1. Un GET sans Authorization ni
cookie reçoit exactement les 92 octets de la WebP source, dimensions 128×96,
SHA-256 `c00293c5914eee6bab0ef78d3af8764ea829c53bbc02f99851bcf9a6dd0bf286`.

Le laboratoire conserve les contrôles de droits, conflit, non-doublon, accord et
rétablissement. Sept écritures de contenu comprennent la mutation externe de
conflit, le média, les brouillons et publications. Seule la frontière DNS/TLS
du processus de test est adaptée à la boucle locale. Le runtime et ses données
jetables sont supprimés à la sortie du lanceur.

## Points encore ouverts

- Il manque une reprise explicitement consentie des transferts dont l’échec est
  certain. La tentative reste bloquée ; ne pas contourner en répétant un upload.
- Une interruption de processus en état `creating`/`publishing`, même avec un
  identifiant connu, nécessite encore une récupération contrôlée. La relecture
  actuelle ne déverrouille pas ces états : le faire pendant une écriture encore
  active pourrait enlever son verrou. Sans identifiant, aucune nouvelle création
  n’est tentée à l’aveugle.
- L’image réellement servie par un hébergeur HTTPS externe et les recettes
  Wix/Webflow/PrestaShop 9 restent à éprouver. Les autres CMS que WordPress/Ghost
  exigent toujours des images déjà hébergées publiquement.

Ces limites ne sont ni une validation générale de compatibilité, ni une
transaction atomique entre Noyaru et les CMS. La PR 95 reste en brouillon.
