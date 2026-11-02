# Lot 29 — Autorisation temporaire pour un site de démonstration

Demande explicite du propriétaire : accès de recette gratuit et temporaire pour
le compte témoin. Base initiale `16a4ab1`, puis intégration sans conflit de `d05a53a` et `9a8fa23` de Claude. Branche et worktree isolés ; catalogues, moteur i18n et captures de Claude préservés.

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

- **66 tests ciblés réussis** : autorisations, isolation des comptes et origines, expiration sans cron, ancien formulaire et imports, transaction avec audit, retrait périmé et concurrence, console opérateur, migrations depuis l’ancien schéma et après `create_all`, cohérence du schéma et données préservées. Les contrôles de fiche client et comptage des plans avaient également réussi dans une première sélection de 139 tests.
- **30 affichages Chromium locaux réussis**, à 1 440, 768, 390, 360 et 320 px, JavaScript actif ou désactivé : écran opérateur et formulaire client FR/EN, cibles de formulaire de 44 px, aucun débordement. Captures relues et présentation mobile affinée.
- Parcours réel dans cette copie : confirmation décochée bloquante, autorisation par son formulaire, ajout du seul site prévu sans JavaScript, nouvel ajout désactivé après création et retrait opérateur sans JavaScript. Aucun appel IA, analyse ou écriture WordPress.
- Les **66 tests et 30 affichages sont répétés avec succès sur la base combinée `9a8fa23`**, incluant aussi l’ajout depuis la fenêtre modale JavaScript ; l’ajout natif sans JavaScript reste attesté par la première recette.
- Le démarrage réel par Alembic est vérifié depuis une base vide. La migration est additive et possède un retour arrière limité à la nouvelle table.

La CI, la revue du dernier commit et l’attribution réelle restent consignées dans la PR. **Aucun accès n’est encore attribué en production** à ce stade. Le nouveau bandeau client est bilingue dans un composant isolé ; l’écran interne suit la langue française actuelle de la fiche administrateur.

## Console privée de l’opérateur

L’écran est accessible depuis la fiche client, « Accès de recette ». Pour une console d’exploitation privée, `ops/recipe_access.py` dispose aussi de `inspect`, `grant` et `revoke`. Toutes les commandes restent en lecture sans `--apply`. Une écriture requiert l’email cible répété avec `--confirm-email`, un propriétaire système configuré et présent, ainsi qu’un motif, une origine publique HTTPS et une durée de 1 à 48 heures. Le canal `operator_console` est conservé dans l’audit ; aucune connexion applicative n’est simulée. Le retrait nécessite l’identifiant exact de l’autorisation.

Exemple de préparation sans écriture :

```sh
python /app/seo-agent-web/ops/recipe_access.py grant --email COMPTE --site https://demo.example.com/ --hours 24 --reason "Recette WordPress"
```

La recette réelle ajoutera uniquement le domaine de démonstration confirmé par le propriétaire, puis rétablira WordPress, retirera sa connexion et nettoiera le projet créé.
