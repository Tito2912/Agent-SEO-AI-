# Blog Noyaru : deux décisions et un suivi continu

Base examinée : `main 0a8410cb99e8ee1a8dd05499af0bcd75f21613b7`, le 14 novembre 2026.
Le premier article réel a été publié ; Tony a confirmé « Article publié et observé ».
Il a ensuite signalé cinq validations Telegram et des liens qui font perdre le contexte.
Cette évolution porte sur les prochaines propositions, sans modifier l’article publié.

## Bilan et parcours cible

| Écran / étape | Problème | Conséquence | Correction P1 |
| --- | --- | --- | --- |
| Sujet / brief | Relecture du plan et autorisation IA séparées | Deux cartes pour une seule intention | Une décision « sujet et rédaction », avec sources et coût |
| Rédaction | Une carte pour marquer le texte relu, puis un écran distinct pour choisir le même texte | Répétition et recherche de l’étape suivante | Proposition préparée en lecture seule après rédaction ; la relecture appartient à l’accord final |
| Publication | Accord PR, attente CI, puis nouvel accord de fusion | Trois validations après rédaction | Une décision « relire et publier » sur les fichiers exacts, puis enchaînement contrôlé PR → CI → fusion → observation |
| Liens | Navigation générique et détour vers Contenu | Article courant perdu, retour arrière | Liens avec brief, brouillon et publication exacts, titres lisibles et étape courante |
| Attente | État affiché inchangé jusqu’au clic manuel | Impression que rien ne se passe | Relevés serveur environ chaque minute pour les nouvelles opérations ; consultation GET actualisée sans action métier |
| Petits écrans | Limites techniques et choix secondaires occupent le haut de page | Prochaine action enfouie | Étapes visibles, action de suivi en haut, détails secondaires repliés |

Parcours normal : choisir un sujet → décision Telegram 1/2 (sujet + rédaction) →
article privé et proposition préparés → décision Telegram 2/2 (relecture + publication)
→ PR, CI, fusion et observation automatiques. Chaque décision conserve le refus et
sa confirmation explicite. Une correction du sujet, des preuves, du texte, de la base
ou des accès peut nécessiter une nouvelle décision : l’accord n’est pas transféré.

## Garde-fous et compatibilité

- `proposal.flow=two_decisions` identifie uniquement les nouvelles propositions.
  Sans ce marqueur, les accords PR puis publication restent distincts. Aucun mandat
  existant n’est élargi, aucun ancien refus ou accord expiré n’est ressuscité.
- Le premier accord relit le brief et réserve une seule demande IA dans la même
  transaction. Les limites existantes restent en place : deux demandes par 24 h
  glissantes, aucune relance aveugle ni crédit SEO client utilisé.
- La proposition initiale n’autorise que des lectures GitHub. La décision finale
  marque les versions exactes relues et lie le mandat à l’identifiant, à la génération
  et à toute la proposition (base, fichiers et empreintes). Chaque nouvelle écriture
  revérifie ce mandat et les sources, droits, contenus et délais.
- La CI doit réussir sur la tête exacte ; branche, diff et protections restent
  vérifiés. Un changement de base bloque la fusion. Une actualisation invalide le
  mandat précédent. La préparation automatique ne prolonge pas les délais.
- Les intentions d’écriture, baux et reçus continuent à protéger la reprise et la
  réconciliation. Un refus ne supprime pas une branche. Un redémarrage reprend la
  proposition en lecture seule, sans nouvel appel IA.
- Le script navigateur lit uniquement la même page en GET, dix secondes entre
  lectures, au plus 90 essais. Il s’arrête après déconnexion ou erreur HTTP. Une
  saisie ou un volet ouvert empêche le rechargement et propose une mise à jour
  explicite. Aucun formulaire ni bouton de décision n’est soumis automatiquement.

## Validations

- Tests transactionnels : parcours complet avec exactement deux cartes Telegram,
  une requête IA simulée et aucun écrit GitHub avant le second accord ; CI en attente,
  publication exacte observée via la vraie route publique locale, refus, duplication,
  sources/texte/accès/base/délai modifiés, CI échouée, invalidation du mandat après
  actualisation, conservation des anciennes décisions et contexte des liens.
- Les tests des anciennes cartes et des opérations historiques restent présents.
  Les tests de navigation ciblent désormais Rédaction sourcée au lieu de Contenu.
- Chromium local : **42 observations** FR/EN, cinq états, largeurs 1 440/768/390/360 px,
  liens aller-retour du même article et deux mises à jour automatiques. Aucun
  débordement global, erreur JavaScript ni requête externe. Captures examinées.
- Compilation et Ruff F/E9 réussis. Résultat de la CI complète à consulter sur la PR.

`browser-results.json` contient les observations. Reproduction :

```bash
python seo-agent-web/docs/ux-own-blog-journey/browser_smoke.py
```

Variables facultatives : `CHROMIUM_EXECUTABLE`, `CHROMIUM_LIBRARY_PATH`,
`JOURNEY_BROWSER_OUTPUT`. Les captures locales sont enregistrées dans le répertoire
choisi. Comptes et données fictifs, IA/GitHub simulés, tous les appels externes bloqués.
Ce navigateur n’utilise aucune session authentifiée de production (C002). La recette
réelle du prochain article reste une vérification opérateur ; aucune rédaction ou
publication réelle n’a été lancée pour tester ce lot. C016 est inchangé.
