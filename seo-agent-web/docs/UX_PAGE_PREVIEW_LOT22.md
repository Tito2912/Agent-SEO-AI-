# Lot 22 — Préparer l’aperçu de la page choisie

Base : `3f35536`, après la fusion du lot 21. Branche et worktree isolés ; aucune PR concurrente publiée lors de l’examen. Le moteur de langues, les pages juridiques et les branches de Claude ne sont pas modifiés.

## Bilan avant correction

**P** : constat dans le navigateur Edge de production, session ouverte par le propriétaire. **G** : lecture du dépôt du site témoin par la connexion GitHub. **O** : JSON de la console serveur fourni par le propriétaire. **C** : examen du code. Les contrôles Chromium locaux utilisent des comptes, rapports et réponses fictifs ; ils ne prouvent aucune écriture réelle chez un fournisseur.

| Écran / étape | Preuve | Problème et conséquence | Proposition / priorité |
| --- | --- | --- | --- |
| Préparation → ancienne opération bloquée | O | Une ancienne opération d’un autre projet bloque les préparations du même compte. | La procédure existante confirme une branche identique au commit initial, sans résultat enregistré ni débit ; l’opérateur abandonne cette seule opération, avec `released: true`. Aucune branche ou donnée de consommation supprimée. Résolu sans modifier le journal. |
| Anomalie → aperçu GitHub d’une page | P, G, C | Après libération, une demande autorisée finit par « Contenu patché manquant ». Le code de ce chemin choisit un fichier générique sans recevoir l’URL. Le dépôt contient pourtant une source Markdown propre à la page, avec une description de 172 caractères, utilisée par la route dynamique. Un client ne peut ni comprendre l’échec ni savoir quoi corriger. | Employer la carte URL → source existante pour les familles de contenu par page et de longueur ; refuser une source inconnue, ambiguë ou une arborescence incomplète avant l’IA. P1. |
| Aperçu → réponse sans changement | C | Le générateur peut retourner `no_change` sans contenu : l’API affiche alors une erreur technique. Un contenu identique pourrait également être décompté avant que l’interface constate un diff vide. Le retour interne de l’IA en production n’a pas été observé : ce mécanisme est reproduit localement, pas affirmé comme un journal fournisseur. | Refuser explicitement ces deux résultats, sans aperçu, décompte, écriture ou déclaration de réussite. Le problème reste à vérifier. P1. |
| Dialogue → suite après refus | P, C | Le bouton permet de relancer la même préparation et le texte initial annonce toujours son coût. | Suspendre les boutons de préparation et de confirmation dans cette fenêtre, expliquer l’arrêt et proposer le suivi, le dépôt du projet ou un autre choix. Fermer puis rouvrir reste une décision explicite. P1. |

## Parcours et règles communes

Choisir une anomalie et une URL → résoudre sa propre source → préparer un aperçu → relire les différences → confirmer la création d’une proposition → relire et publier → vérifier par une nouvelle analyse.

Une source inconnue ne devient pas un fichier générique choisi au hasard. Une absence de modification ne signifie pas que le problème du site a disparu. Le suivi conserve le projet et ses reçus. Les refus connus proposent une action utile ; ils ne créent aucun nouvel envoi automatique. Les étapes, liens, focus et cibles tactiles réutilisent le dialogue existant. Les cinq écrans consommateurs chargent son script v5.

## Réalisation

- La préparation simple transmet l’URL déjà choisie au résolveur. Les familles de contenu par page (`_PER_PAGE_CONTENT_KEYS`) et de longueur utilisent `repo_index`, comme le correcteur étendu. Une source unique est exigée ; les templates partagés et fichiers non publiés sont exclus. Cette sélection ne consomme aucun appel IA supplémentaire. Les autres familles conservent leur résolution existante.
- Les résultats `no_change` et strictement identiques à la source produisent un refus explicite avant l’enregistrement et le décompte d’un aperçu. Les opérations sans écriture ni débit sont terminées par le mécanisme existant ; aucun changement des verrous, identités de requête ou règles de facturation.
- Le signal `preview_unavailable` retire les actions devenues inutiles, remplace l’introduction et affiche une sortie contextualisée. Une case d’accord modifiée ne réactive pas la confirmation. Un aperçu valide conserve sa relecture et sa confirmation distinctes. Les anciens aperçus payés restent récupérables sans nouvelle génération.
- Cinq traductions ajoutées au catalogue anglais existant ; aucune modification du moteur de traduction ou des layouts partagés.

## Vérifications et limites

| Contrôle | Résultat |
| --- | --- |
| [Constats réels expurgés](ux-page-preview/production.json) | Libération opérateur confirmée ; nouvelle fenêtre v4 observée en production ; une seule demande de génération effectivement envoyée après le déblocage. Refus « Contenu patché manquant », sans aperçu ni PR. Quota réel toujours **0/2** après la demande ; ancien suivi conservé. Le premier essai de lecture du suivi a expiré avant tout clic de préparation et n’a pas généré un second appel IA. |
| Tests serveur | **18 nouveaux cas** : source propre à une page Next/MDX ou HTML, racine, contexte de requête, source absente/ambiguë/incomplète/illisible, famille partagée conservée, URL transmise par les endpoints titre et description, refus sans débit ni tâche « corrigée », reprise d’un aperçu payé et réponse anglaise. Suites existantes de droits, quotas, reprises, correspondance des routes et syntaxe réussies. Première passe : 1 284 réussis / 2 ignorés ; vérification finale des 18 nouveaux cas et des traductions : 51 réussis, avec recouvrement des deux passes. |
| [Chromium local](ux-page-preview/browser.json) | **26 observations** : deux refus et un aperçu valide × FR/EN × 1 440/768/390/360 px, puis les deux destinations réelles du projet fictif. Une seule requête par préparation ; aucune confirmation spontanée, réactivation forcée ou répétition automatique. Fermeture et retour du focus vérifiés. Aucun débordement, erreur JavaScript ou appel extérieur. |
| [Contrastes locaux](ux-page-preview/contrast.json) | **48 échantillons**, minimum **5,01:1**, sur les messages et liens. Cibles de 44 px minimum et focus clavier visible. Ce contrôle partiel ne constitue pas un audit WCAG intégral. [Source inconnue FR](ux-page-preview/source-unresolved-fr-360.png) · [Sans changement EN](ux-page-preview/no-change-en-360.png). |
| Contrôles statiques | Ruff F/E9, compilation Python, syntaxe JavaScript, Bandit medium/high et espaces vérifiés. Avertissements de dépréciation et `nosec` existants. La CI et la revue du dernier commit sont consignées dans la PR avant fusion. |

**Limite de recette :** aucun aperçu réussi, nouvelle PR du site témoin, application ou nouvelle analyse n’est attesté. Le résultat interne du fournisseur IA n’est pas accessible depuis l’erreur affichée. Les fichiers et textes du client, les identifiants de compte, les cookies et les JSON opérateur bruts restent hors du dépôt. Aucun déploiement manuel de production, modification de formule ou fusion d’une PR du site client.
