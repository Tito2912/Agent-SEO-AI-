# Lot 29 — Autorisation temporaire pour un site de démonstration

Demande explicite du propriétaire : accès de recette gratuit et temporaire pour
le compte témoin. Base `16a4ab1`, branche et worktree isolés.

## Constat avant changement

| Écran | Preuve | Conséquence | Évolution | Priorité |
| --- | --- | --- | --- | --- |
| Abonnement du compte témoin | Navigateur en production : Free, 10 sites conservés / limite de 1, 673 pages restantes | Un nouveau projet de démonstration ne peut pas être ajouté. | Autoriser temporairement le seul domaine de démonstration. | P1 pour la recette |
| Fiche client | Code : lecture seule, aucun accès de recette individuel | L’opérateur ne peut pas accorder cette exception sans modifier les règles générales ou simuler un abonnement. | Écran réservé au propriétaire système, expiration, motif, confirmation et retrait. | P1 |
| WordPress manuel | Code des routes CMS | Déjà accessible en Free ; les droits du vrai WordPress et les confirmations restent requis. | Aucun changement de droits CMS ou de formule nécessaire. | Conforme |

## Règles retenues

Une autorisation par compte et origine HTTPS exacte, de 1 à 48 heures, 24 heures
par défaut. Aucun joker, chemin, identifiant ou port inhabituel. Les contrôles
d’adresse publique habituels restent exécutés lors de l’ajout. Le propriétaire
système peut accorder, renouveler et retirer cette permission depuis la fiche
client. L’autorisation et son journal sont enregistrés dans la même transaction.
Un identifiant de version protège le retrait d’une autorisation renouvelée.

La permission concerne uniquement l’ajout de ce projet au-delà de la limite.
Les plans, compteurs, rôles, factures, webhooks et quotas d’IA restent intacts.
L’expiration ne supprime pas un projet créé ; la recette prévoit son nettoyage
explicite après rétablissement des valeurs WordPress.

## Validation

À compléter après tests, navigateur, CI, revue et attribution réelle. Aucune
permission n’est encore attribuée au moment de ce bilan initial.
