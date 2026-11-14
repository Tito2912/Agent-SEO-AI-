# Publication du blog propre à Noyaru

## Parcours courant depuis le 14 novembre 2026

Les nouvelles propositions utilisent **deux décisions Telegram au total** :

1. Valider le sujet et autoriser une rédaction IA bornée avec son coût habituel.
   Le brief est relu dans cette même transaction. Le texte reste privé ; une
   proposition de publication se prépare ensuite en lecture seule.
2. Relire l’article et ses sources, puis autoriser sa préparation GitHub et sa
   publication exacte après réussite des contrôles. Cette décision inclut la
   relecture enregistrée, la création de PR en brouillon, la CI, la fusion et
   l’observation du déploiement. Aucun nouvel appel IA.

Un changement de base, de contenu, de sources ou d’accès arrête la progression.
Une nouvelle décision est nécessaire après actualisation de la proposition. Les
refus et expirations restent effectifs ; un résultat incertain est réconcilié sans
répéter une écriture. Les limites de lecture/écriture et la protection de branche
restent applicables. Le suivi des nouvelles opérations est relevé environ chaque
minute ; consulter une page ne déclenche ni appel IA ni écriture GitHub.

**Compatibilité :** les opérations antérieures sans `proposal.flow=two_decisions`
conservent leurs accords distincts de préparation et de publication. Aucun mandat
ancien n’est élargi. Les anciennes cartes de rédaction en attente dont la version
ne correspond plus deviennent caduques ; les opérations déjà autorisées restent
bornées à leur effet d’origine. Aucune migration de base ni action en production
n’est nécessaire pour activer ce comportement sur les prochaines propositions.

Le bilan écran par écran et les preuves locales sont dans
[ux-own-blog-journey](ux-own-blog-journey/README.md).

## Historique : bilan avant le lot initial

| Écran / étape | Existant examiné dans le code | Conséquence | Évolution (P1) |
| --- | --- | --- | --- |
| Rédaction sourcée | Brouillon privé, relecture et export Markdown | L'accord de relecture n'autorise pas GitHub | Accès à une proposition de publication distincte |
| Blog public FR / EN | Fichiers Markdown ; un article EN exige son original FR au même slug | Un export privé ne peut pas être publié tel quel | Fichiers déterministes compatibles avec le catalogue, FR obligatoire, EN facultatif explicitement relu |
| Validations Telegram | Sources, briefs, appels IA, relecture | Aucune capacité de publication | Deux accords séparés : PR en brouillon, puis fusion du contenu exact après CI |
| Suivi de publication | Aucun suivi propre au blog | Une réponse réseau perdue pourrait laisser un résultat ambigu | Intentions durables avant écriture, relecture GitHub, arrêt sans répétition aveugle |

Ce bilan provient du code, pas d'une nouvelle recette en production. Les écrans système
exigent le compte propriétaire. Aucun article réel n'est créé par les tests de ce lot.

## Historique : parcours initial et règles

1. Relire les affirmations et les sources du brouillon privé.
2. Choisir le français et, facultativement, une version anglaise relue du même sujet.
3. Examiner les fichiers exacts, le dépôt, la base et les URL proposées.
4. Approuver dans Telegram la création d'une PR en brouillon. Aucun nouvel appel IA.
5. Examiner le diff et les contrôles du même commit. Si `main` avance, un nouvel accord
   de préparation permet uniquement de synchroniser la branche de cette opération.
6. Approuver dans une seconde carte la fusion. GitHub conserve ses règles de protection.
7. Observer les pages effectivement servies (langue, canonique, titre, description, corps
   d'article, empreinte source et version déployée) ; distinguer publication technique, indexation
   et visibilité. Aucun gain de classement ou de citation n'est déduit de la publication.

L'interface reprend les cartes, focus visibles, cibles de 44 px, détails et états textuels
de l'espace SEO. FR / EN, petits écrans, accès refusé et erreurs ont les mêmes règles.

## Périmètre fermé

- Dépôt `Tito2912/Agent-SEO-AI-`, identifiant 1178767307, branche `main`.
- Ajouts exclusivement sous `seo-agent-web/content/blog/`, un fichier FR et au plus
  sa traduction dans `en/`. Aucun écrasement d'article, changement de code ou de workflow.
- Mandats de 24 h, empreinte du contenu et de la connexion, bornes de lectures/écritures,
  journal d'audit, verrou et bail pour les exécutions concurrentes.
- Après un résultat incertain, lecture de réconciliation, jamais répétition automatique
  d'une écriture déjà tentée. Les refus ne suppriment ni branche ni brouillon.
- SAV, messagerie personnelle, réseaux sociaux et sites clients restent des capacités
  séparées. La migration ne crée aucun mandat ni publication.

Références API : [contenus GitHub](https://docs.github.com/en/rest/repos/contents),
[fusion d'une PR](https://docs.github.com/en/rest/pulls/pulls),
[fusion vers une branche](https://docs.github.com/en/rest/branches/branches).

## Vérifications de ce lot

- Chromium local : 80 combinaisons FR/EN, 360 à 1440 px, états de décision, d'attente,
  d'incertitude, de refus et de publication ; formulaires natifs de préparation et d'arrêt.
  Aucun débordement, erreur JavaScript ou appel externe réel. Les états publiés y sont simulés.
- 213 tests combinés de non-régression, puis 39 tests publication/migration après le
  durcissement de la lecture des connexions. 34 contrôles du démarrage des services.
- Réponses GitHub perdues avant/après effet, synchronisation, reprise sous bail, expiration,
  double décision, refus, preuves CI absentes/échouées/ignorées, protections de branche,
  source ou connexion modifiée, ajout non autorisé et faux succès HTTP sont couverts.
  Des en-têtes corrects ne suffisent pas si le HTML servi est vide ou ne correspond pas.
- La lecture d'un ancien secret ou d'un secret chiffré ne peut ni le migrer ni valider
  implicitement une transaction de décision. Les données du compte restent exportables.
- La CI et les contrôles en production sont consignés dans la PR. Aucune publication
  réelle d'article n'est incluse dans la recette locale ; elle exige ses propres accords.
