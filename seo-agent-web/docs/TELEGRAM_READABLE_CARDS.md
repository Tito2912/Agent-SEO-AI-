# Cartes Telegram : lecture guidée et décisions explicites

## Audit et parcours cible

Les captures réelles du pilote SAV des 6 et 7 novembre 2026 montrent un dossier JSON
sur plusieurs pages, des retours à la ligne échappés et une réponse finale entourée
de clés techniques. Le contrôle complet des autres cartes est réalisé dans le code.

| Carte ou étape | Problème | Conséquence | Amélioration P1 |
| --- | --- | --- | --- |
| Rédaction SAV et éditoriale | Question, sources et métadonnées en JSON | Effort de lecture élevé avant un appel payant | Rubriques, texte naturel et extraits lisibles |
| Envoi SAV | Réponse et destinataire dans un objet technique | Risque de valider sans bien relire l'email | Coordonnées, fournisseur et réponse exacte séparés |
| Brief et brouillon | JSON et export Markdown technique | Repères et preuves difficiles à parcourir | Libellés lisibles et références proches du texte |
| Blog | Fichiers et effet regroupés | Préparation GitHub et publication peu différenciées | Étapes 1/2 et 2/2, destination et fichiers distincts |
| Navigation | Flèches seules et pages coupées au milieu des mots | Lecture et avancement difficiles à suivre | Paragraphes préservés, rubriques répétées et progression |
| Confirmation | Effet et identifiant sans contexte de destination | Doute au dernier clic | Rappel du sujet et du destinataire avant confirmation |
| Décision enregistrée | Énumération interne | Résultat métier ambigu | Accord/refus lisible et lien de suivi, sans promesse de livraison |

Les notifications de comptes et abonnements ont déjà un titre court, une icône et
des valeurs échappées. Elles sont conservées. Aucun fichier d'intégration CMS ou de
traduction de Claude n'est modifié.

Parcours : repérer l'action → lire question et preuves → parcourir les pages →
approuver/refuser → relire le récapitulatif → confirmer → suivre le résultat dans Noyaru.

## Règles communes

- Titres courts, une icône par rôle, paragraphes et boutons nommés ; FR/EN.
- Coût d'un appel IA, effets GitHub, destinataire, fournisseur et limites visibles.
- Texte métier complet conservé, découpage à 2 800 unités UTF-16 par page ; 16 pages au plus.
- Les empreintes complètes restent dans le contrôle serveur. Les sources sont présentées
  avec un repère court de version et leur texte ; aucune IA n'est appelée pour les résumer.
- HTML uniquement pour les titres contrôlés ; toutes les valeurs variables sont échappées.
- Les confirmations, limites, versions, droits et callbacks existants restent applicables.
  Une page doit avoir été affichée avec succès avant l'accord.
- Les cartes déjà enregistrées conservent leur dossier. Seul un renvoi explicitement
  demandé dans Noyaru régénère leur présentation et invalide les anciens boutons.
- Les copies SAV restent chiffrées et soumises à leur conservation existante.

La décision enregistrée confirme un accord, pas le succès de l'opération, la livraison
d'un email ou la publication d'un article. La recette Telegram réelle reste distincte
des tests de transport simulé.

Référence : [formatage et boutons du Bot API](https://core.telegram.org/bots/api#formatting-options).

## Validation

- 77 tests locaux ciblés : dix types de cartes en FR/EN, pagination Unicode,
  texte variable échappé, coordonnées et réponse exacte conservées, stockage SAV
  chiffré, anciens boutons, renvoi explicite et échec d'affichage sans déverrouillage.
- Compilation Python, Ruff F/E9, Bandit medium/high et `git diff --check` réussis.
- Aperçu HTML local produit à partir du même générateur de cartes, avec des données
  fictives. Les boutons de cet aperçu sont inertes.
- Le navigateur Edge de recette est indisponible pendant la limite d'usage du
  pilotage ; Chromium Linux ne démarre pas faute de bibliothèque système. Aucune
  nouvelle vérification visuelle dans le vrai bot Telegram n'est revendiquée.
- La suite complète, y compris les contrôles HTTP exclus des tests locaux en
  raison du blocage AnyIO de cet environnement, doit passer dans GitHub Actions
  avant fusion. Après déploiement automatique, une carte « Test » permet de
  vérifier le rendu Telegram sans appel IA ni email SAV.
