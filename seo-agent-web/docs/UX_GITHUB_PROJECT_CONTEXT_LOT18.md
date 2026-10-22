# Lot 18 — Choisir le dépôt du bon compte

## Audit avant modification

Examen de `608a611`, comprenant le main `77bddbc` et le lot 17. Branche et worktree séparés de Claude. **L** : Chromium local avec comptes et fournisseur GitHub fictifs ; **C** : code. Aucun accès authentifié de production utilisé pour ce lot.

| Écran / état | Preuve | Problème et conséquence | Amélioration | Priorité |
| --- | --- | --- | --- | --- |
| Réglages de l’analyse → GitHub → sélection des dépôts, membre d’un site partagé | L, C | Le bouton appelle `/api/github/repos` sans le site. Le serveur propose alors les dépôts personnels du membre, tandis que l’enregistrement vérifie les accès du propriétaire. Le client peut choisir un dépôt ensuite refusé, ou ne pas retrouver celui du site. | Transmettre le site à la liste ; conserver le même compte pour la proposition et la validation. | P1 |
| Intitulé « Mes dépôts » et contexte de la connexion | L, C | Le possessif suggère le compte personnel même pour le site partagé. | Employer « Choisir un dépôt » et préciser la connexion qui fournit la liste, près du champ. | P2 |
| Chargement du script du sélecteur | C, revue de la PR #50 | L’URL garde `?v=1` malgré l’ajout du consentement par Claude. Un navigateur peut réutiliser une ancienne version. | Actualiser la version du script à chaque modification de ce lot, sans changer le mécanisme de consentement. | P2 |

[Constat navigateur avant correction](ux-github-project-context/baseline.json) : membre, français, 360 px. Le transport fictif retourne bien les dépôts personnels en l’absence de contexte ; aucun appel GitHub réel. Ce défaut distinct n’explique pas à lui seul le délai de lecture constaté durant la recette de production du lot 17.

## Parcours cible

Ouvrir les réglages du site → consulter sa connexion → demander les dépôts accessibles au compte qui gère ce site → choisir le dépôt et sa branche → enregistrer explicitement → afficher la sélection confirmée. Hors projet, les connexions du compte conservent leur liste personnelle. L’activation automatique garde son consentement et ses contrôles serveur existants.

## Réalisation et validation

Code `eece4b4`, intégré avec le correctif de revue WordPress du lot 17 dans `9d111f4`. Trois fichiers applicatifs : requête du sélecteur, son libellé et son aide, deux traductions anglaises. Le script passe à `?v=2`, ce qui recharge également l’envoi du consentement ajouté par Claude. Aucun changement du backend, des règles de consentement, des textes juridiques ou du moteur de langues. Les branches et fichiers de travail de Claude ne sont pas modifiés.

| Vérification | Résultat et limite |
| --- | --- |
| Tests existants ciblés | **73 réussis** : compte propriétaire pour la liste, maintien de la liste personnelle hors projet, droits, réglages et consentements, traductions. Un avertissement existant. |
| [Chromium après correction](ux-github-project-context/browser.json) | **16 parcours**, propriétaire/membre × FR/EN × 1 440/768/390/360 px : bon compte, choix du dépôt, branche proposée, enregistrement explicite puis état confirmé. Aucun débordement, erreur JavaScript ou appel extérieur. Les requêtes passent par les vrais endpoints locaux ; seul le transport GitHub est fictif. |
| Consentement dans le navigateur | **Un cas supplémentaire** à 390 px : sans case cochée, aucun POST ; après confirmation, `consent: true` et `mode: auto` sont reçus par le serveur local. URL du script `?v=2` vérifiée. Aucun dépôt distant créé ou modifié. |
| [Contrastes ciblés](ux-github-project-context/contrast.json) | **32 échantillons**, minimum **4,63:1**, fonds composés ; bouton d’au moins 44 px et aide reliée au champ par `aria-describedby`. Ce n’est pas une certification WCAG intégrale. |
| Contrôles statiques | Syntaxe JavaScript avec Node, parsing du template Jinja, catalogue JSON et contrôle des espaces réussis. La CI complète du dernier commit publié figure dans la PR avant fusion. |
| Inventaire courant | Régénéré à `9d111f4`, **201 routes et 89 templates** ; aucun nouvel endpoint. |

[Avant, membre à 360 px](ux-github-project-context/member-before-360.png) · [Après, membre à 360 px](ux-github-project-context/member-after-360.png).

La recette utilise des identités et dépôts fictifs ; elle ne rejoue pas OAuth et ne prouve pas les droits réels du jeton de production. Le délai de lecture GitHub non attesté au lot 17 et les remarques de revue de #50 sur la conservation des preuves de consentement restent des sujets distincts ; ce petit correctif d’interface ne les déclare pas résolus. Aucun déploiement manuel de production.
