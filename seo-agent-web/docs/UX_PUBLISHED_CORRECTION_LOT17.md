# Lot 17 — Reprendre une correction publiée et suivre son contrôle

Base examinée : `e2b98caf3964231527ba3761cad462025647b349`, après les PR #47 et #49 de Claude. Worktree isolé ; textes juridiques et moteur de langues conservés.

## Audit avant modification

**P** : navigateur Edge de production, session du compte de test ouverte directement par le propriétaire. **L** : Chromium local, comptes fictifs. **C** : code. Aucun mot de passe, cookie, jeton, identifiant de compte ou statistique nominative n’est conservé dans ces preuves versionnées.

| Écran / état | Preuve | Problème et conséquence | Proposition | Priorité |
| --- | --- | --- | --- | --- |
| Bilan du site, analyse active ; lien vers le traitement | P à 360 px, C | « Voir dans Exécutions » ouvre la liste Activité avec le traitement surligné. Il faut encore ouvrir son suivi, alors que le client consulte déjà cette analyse précise. Le nom ne correspond plus au menu. | « Voir le suivi » vers le détail exact, dès le HTML et après actualisation JavaScript. Même accès direct après échec ou annulation d’une première analyse. | P2 |
| Correction déjà publiée, modification déclarée, aucun contrôle terminé | P, C | L’action de vérification est correcte, mais les instructions de modification et le choix de plateforme occupent encore une grande partie de l’écran. Le client peut repartir vers une étape déjà terminée. | Donner la priorité au contrôle et ranger les instructions dans un panneau repliable. Les réafficher après un contrôle inconclusif ou un problème encore présent. | P2 |
| Contrôle terminé mais non confirmé | P, C | Le contrôle réel de 127 pages refuse légitimement une ancienne correction dont le périmètre complet est inconnu. La reprise dit pourtant « Votre modification attend son contrôle », et le parcours remet une nouvelle analyse en avant. Le client peut consommer des pages sans résoudre la cause. | Annoncer le résultat non confirmé et sa raison ; proposer le bilan exact et le choix d’une autre correction. Garder un nouvel essai explicite dans un panneau secondaire. Aucune preuve ni règle de vérification modifiée. | P1 |
| Connexions Google/Bing/GitHub du site témoin | P | Autorisations enregistrées. Sur les 28 jours consultés, Google et Bing répondent avec zéro ligne ; aucun message de déconnexion. | Conserver la distinction autorisation / accès aux données / absence de résultats. Cette lecture ne rejoue pas les consentements OAuth. | Conserver |
| Correction suivie sur `/about-de` | P, GitHub et HTML public | Une proposition GitHub déjà fusionnée attend encore la déclaration de publication dans Noyaru. Le HTML sert bien `lang="de"` et un hreflang allemand cohérent. Ce n’est pas une nouvelle erreur d’écriture. | Reprendre le suivi existant, déclarer la modification réellement publiée, lancer un seul contrôle normal puis consulter son résultat. Aucune nouvelle modification du site. | Recette |

## Parcours cible

Proposition relue et publiée → déclaration dans le suivi → nouvelle analyse décomptée normalement → détail du traitement → bilan et résultat du contrôle → choix explicite d’une correction suivante. La déclaration ne devient pas une preuve de résolution. Les instructions restent disponibles sans prendre la place de l’action suivante.

## Réalisation et validation

Code applicatif `e87f884dca6c853bc73aec21ec4c8147c7e10a34`. Le calcul des preuves, le périmètre historique des correctifs, les prix, les quotas, les permissions et les appels d’écriture restent inchangés.

- La reprise commune de l’accueil, du bilan et des problèmes distingue désormais **contrôle non confirmé** et **modification en attente d’un premier contrôle**. Elle affiche la raison conservée par le serveur et conduit à son explication.
- Le parcours non confirmé propose le bilan de contrôle exact et un choix explicite d’une autre correction. Le nouvel essai et son coût restent dans un panneau repliable ; aucune relance automatique. Les instructions sont directement consultables pour comprendre l’échec. Le bouton de nouvelle déclaration reste disponible comme action secondaire.
- Avant le premier contrôle, les instructions peuvent être rouvertes au clavier ; elles cèdent la place à la vérification. Les actions de contrôle ont une hauteur d’au moins 44 px et des espacements adaptés au mobile.
- Le lien du bilan ouvre directement `/jobs/{id}`, avec et sans JavaScript ; l’actualisation de la progression conserve cet identifiant. Les états interrompus sans bilan ont aussi ce lien direct. Les gardes serveur restent celles du détail existant.

| Vérification | Résultat et limite |
| --- | --- |
| [Navigateur local](ux-published-correction/browser.json) | **42 observations Chromium**, FR/EN, 1 440/768/390/360 px. Attente, instructions ouvertes/fermées au clavier, suivi exact, sans JavaScript, contrôle inconclusif, reprise commune. Aucun débordement, erreur JavaScript ou appel distant. Données et rapports fictifs. |
| [Contrastes](ux-published-correction/contrast.json) | **56 échantillons**, minimum **6,82:1** sur les nouveaux repères, boutons et raisons ; fonds RGBA composés. Résumés et actions de contrôle d’au moins 44 px. Aucun audit WCAG intégral revendiqué. |
| Tests ciblés | **112 réussis**, dont les cas de périmètre ancien inconnu, URL non revisitée, bilan partiel, contrôle antérieur à la modification, résolution confirmée et reprise suivante ; accès direct au bon traitement et refus d’un autre compte. Un avertissement existant. |
| Contrôles statiques | Compilation, Ruff F/E9, Bandit medium/high, 89 templates Jinja et contrôle des espaces réussis. Avertissements `nosec` existants. La CI complète sur le commit publié figure dans la PR. |
| [Inventaire courant](UX_SCREEN_INVENTORY_CURRENT.md) | Régénéré à partir du code courant, avec le nouvel écran juridique de Claude : **201 routes, 89 templates**. Aucun nouvel endpoint de ce lot. |

Un premier passage local a échoué sur la reprise d’une correction suivante ; le cas isolé passe aussi bien sur la branche que sur une archive du main intact. La sélection ciblée finale de 112 tests passe, sans affaiblir cette assertion. La CI complète reste un contrôle distinct.

### Actualisation après la fusion de Claude

La CI initiale sur `d40df8e` réussit : **8 625 tests**, 45 ignorés, compilation, Ruff, audit des dépendances et Bandit medium/high verts ([exécution](https://github.com/Tito2912/Agent-SEO-AI-/actions/runs/39653158759)).

Le main `77bddbc`, comprenant la PR #50 de Claude, est ensuite intégré dans cette branche isolée par `89f6fab`. La fusion conserve les traductions des deux lots, sans conflit ni modification supplémentaire des textes juridiques, des consentements ou du moteur de langues. **170 tests ciblés** passent sur cet ensemble : correction guidée, reprise, suivi, internationalisation, activation des modes automatiques et pages juridiques. Les **42 observations Chromium** sont rejouées, FR/EN à 1 440/768/390/360 px, sans débordement, erreur JavaScript ou requête distante ; la capture mobile est relue. L’inventaire courant est régénéré à cette révision, toujours **201 routes et 89 templates**. La CI de cette révision actualisée reste une validation distincte consignée dans la PR avant fusion.

[Attente du contrôle sur mobile](ux-published-correction/waiting-fr-360.png) · [Contrôle non confirmé sur mobile](ux-published-correction/inconclusive-fr-360.png) · [Contrôle non confirmé en anglais](ux-published-correction/inconclusive-en-1440.png).

## Recette authentifiée de production

Le propriétaire a fourni `voiceoverstudioai.com` et a lui-même ouvert la session de test dans une fenêtre Edge à profil temporaire distinct. L’identité attendue est vérifiée sans enregistrer son adresse dans le dépôt. Aucun cookie, mot de passe, jeton ou fichier de profil n’est importé dans le code ou les preuves publiques.

[Compte rendu expurgé](ux-published-correction/production.json) :

1. Compte personnel et connexions du projet consultés. Google et Bing sont autorisés ; GitHub a une connexion enregistrée et le dépôt attendu est sélectionné. Les écrans Google/Bing présentent des données reçues mais **aucune ligne** pour les 28 jours affichés. Ce n’est pas une preuve d’absence de visiteurs ni de ventes.
2. Une ancienne proposition GitHub est déjà fusionnée. Le HTML public de `/about-de` répond 200 avec `lang="de"`, canonical et hreflang allemand cohérents. Le site n’est pas modifié pendant la recette.
3. La publication existante est déclarée via le formulaire normal ; l’écran passe à la vérification sans annoncer de résolution.
4. Un seul clic sur « Lancer la vérification » déclenche le crawler réel. Le suivi actif, la liste Activité, son détail et le bilan exact sont consultés. **127 pages**, analyse `20261021-175401` terminée, rapport disponible.
5. Résultat réellement conservé : **non confirmé**, « Le périmètre complet de cet ancien correctif est inconnu. » Cette limite reste intacte ; le HTML correct d’une page ne certifie pas tout un ancien lot.
6. Le choix d’une autre correction s’ouvre avec trois familles de problèmes du rapport courant, puis permet de revenir au suivi. Aucune nouvelle correction n’est choisie automatiquement et l’ancien reçu reste conservé.
7. Le compteur de pages passe de 102 à 229 : **127 pages effectivement décomptées**, après ajustement de la réservation. Assistant et corrections IA restent à zéro. Aucun paiement ou changement de formule.
8. Vérifications explicites à **360 px** : suivi de correction, abonnement contextualisé, détail de l’analyse, Performance Google et Bing. Pas de débordement du document ; pas de chargement de graphique encore visible lors du contrôle. Consultation sur la fenêtre de bureau de **1 030 px** également consignée.
9. La tentative de déconnexion applicative n’est pas attestée par le pilotage : aucun retour à la connexion n’a été observé. Elle n’est pas comptée comme réussite et reste à examiner séparément. Le nettoyage porte uniquement sur la fenêtre et le profil temporaires créés pour la recette ; les connexions fournisseurs enregistrées ne sont pas supprimées.

La version déployée n’est pas identifiée par cette recette : les correctifs applicatifs de ce lot sont vérifiés localement, pas présentés comme déjà visibles en production. Les consentements OAuth ne sont pas rejoués. La lecture de la liste de dépôts via l’API Noyaru n’a pas pu être attestée dans le délai de pilotage de 45 secondes ; aucune nouvelle tentative ni écriture GitHub n’est faite. La lecture de l’ancienne PR fusionnée via GitHub et les réglages enregistrés ne prouvent pas à elles seules les droits d’écriture actuels de Noyaru.

Suites : vérifier cette lecture GitHub et le consentement réel dans une recette dédiée ; éprouver une nouvelle proposition et sa publication sur un dépôt témoin avant toute généralisation. WordPress en ligne reste distinct du laboratoire ci-dessous. Aucun déploiement de production déclenché manuellement.

## Laboratoire complémentaire

Sur la base examinée, le laboratoire du dépôt utilise un vrai WordPress **6.9.10** et Yoast **28.7** jetables : **12 contrôles réussis**, 32 requêtes API, 6 écritures locales comprenant application, rétablissement et changements externes de test. Aperçu sans écriture, droits insuffisants, double envoi, HTML réel, vérification par rapport construit depuis ce HTML, rétablissement et déconnexion sont contrôlés. Le crawler complet et le transport HTTPS public ne sont pas exécutés dans ce laboratoire.

Les **3 tests de passerelle** et les **5 contrôles du paquet de démonstration sur boucle locale** passent. Aucun service Render ni conteneur hébergé n’est créé ; la variante Docker limitée en mémoire n’est pas rejouée. Node 24.22.0 temporaire, archive vérifiée contre le SHA-256 officiel ; dépendances du laboratoire verrouillées.

Les derniers ajouts de Claude et la refonte passent **74 tests ciblés**. **34 observations Chromium locales**, FR/EN à 1 440/768/390/360 px, couvrent le nouvel écran d’annonces en lecture, l’historique, le bilan et le compte sans site, ainsi que ses refus propriétaire ordinaire/anonyme. Aucun envoi d’annonce, débordement de document ou erreur JavaScript relevé. Les **201 routes HTTP chargées** correspondent à **198 déclarations + 3 routes automatiques**, avec **89 templates** à cette révision.
