# Lot 28 — Recette Stripe isolée et langue des écrans hébergés

Base initiale : `1058bb7`, après les lots 26/27. Branche et worktree isolés.
Aucun catalogue, moteur de langues ou capture de Claude modifié.

## Audit avant changement

| Écran / parcours | Preuve | Problème / conséquence | Proposition | Priorité |
| --- | --- | --- | --- | --- |
| Abonnement anglais → Stripe Checkout | Navigateur réel, copie locale de Noyaru et API Stripe de test | Noyaru est en anglais, mais Stripe et son HTML restent en français (`locale=fr`). Rupture de compréhension au paiement. | Transmettre la langue courante de la requête à Stripe. | P2 |
| Gérer l'abonnement → portail Stripe | Code uniquement au constat initial | La langue est aussi imposée au français. | Même règle pour le portail, le parcours de résiliation et son repli. | P2 |
| Choisir Solo sans consentement | Navigateur réel local | La soumission est bloquée avant tout envoi, comme attendu. | Conserver la confirmation obligatoire. | Conforme |
| Annuler Checkout | Navigateur réel Stripe test → Noyaru local | Le retour vers Abonnement confirme l'annulation ; le compte reste Free. | Conserver les droits et le retour explicite. | Conforme |

## Environnement réellement utilisé

Le propriétaire confirme que les essais antérieurs utilisaient les variables
Stripe de test dans l'environnement Noyaru existant. La présente recette utilise
une copie locale de `main`, une base SQLite neuve, un compte fictif, aucun worker,
aucun fournisseur IA et aucun envoi SMTP. Les variables de production sont intactes.

Un bac à sable Stripe temporaire indépendant, sans paiement réel, fournit les
produits/tarifs fictifs et les webhooks signés relayés au serveur local. Le CLI
officiel 1.55.6 est téléchargé dans `/tmp` et son SHA-256 vérifié. Aucun compte
Stripe de production ni donnée client n'est réutilisé. Clés, cookies et URLs de
sessions restent privés et sont exclus des preuves publiables.

Le premier Checkout est bien un abonnement de test, non payé, de 49 EUR. Le
passage en anglais est confirmé dans Noyaru, tandis que le fournisseur conserve
`locale=fr` et son HTML français. Les préférences de facturation du client Stripe
et les montants ne sont pas modifiés par le correctif de langue de l'interface.

## Réalisation et validation

À compléter après tests et recette du correctif. Aucun paiement simulé réussi
ni contrôle en production n'est encore revendiqué par ce bilan initial.
