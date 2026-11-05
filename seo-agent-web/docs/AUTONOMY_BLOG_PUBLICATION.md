# Publication du blog propre à Noyaru

## Bilan avant ce lot

| Écran / étape | Existant examiné dans le code | Conséquence | Évolution (P1) |
| --- | --- | --- | --- |
| Rédaction sourcée | Brouillon privé, relecture et export Markdown | L'accord de relecture n'autorise pas GitHub | Accès à une proposition de publication distincte |
| Blog public FR / EN | Fichiers Markdown ; un article EN exige son original FR au même slug | Un export privé ne peut pas être publié tel quel | Fichiers déterministes compatibles avec le catalogue, FR obligatoire, EN facultatif explicitement relu |
| Validations Telegram | Sources, briefs, appels IA, relecture | Aucune capacité de publication | Deux accords séparés : PR en brouillon, puis fusion du contenu exact après CI |
| Suivi de publication | Aucun suivi propre au blog | Une réponse réseau perdue pourrait laisser un résultat ambigu | Intentions durables avant écriture, relecture GitHub, arrêt sans répétition aveugle |

Ce bilan provient du code, pas d'une nouvelle recette en production. Les écrans système
exigent le compte propriétaire. Aucun article réel n'est créé par les tests de ce lot.

## Parcours cible et règles

1. Relire les affirmations et les sources du brouillon privé.
2. Choisir le français et, facultativement, une version anglaise relue du même sujet.
3. Examiner les fichiers exacts, le dépôt, la base et les URL proposées.
4. Approuver dans Telegram la création d'une PR en brouillon. Aucun nouvel appel IA.
5. Examiner le diff et les contrôles du même commit. Si `main` avance, un nouvel accord
   de préparation permet uniquement de synchroniser la branche de cette opération.
6. Approuver dans une seconde carte la fusion. GitHub conserve ses règles de protection.
7. Observer les pages effectivement servies ; distinguer publication technique, indexation
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
