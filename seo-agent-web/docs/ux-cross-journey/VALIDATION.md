# Recette du lot 16

Code vérifié : `af9f55139cf1a4bb027f1c2c29ad20661e9f0874`, base `d7f4f24`.

- `baseline.json` : 21 constats avant modification ; `long-site-before.json` : mesure complémentaire du bouton long avant correction.
- `browser.json` : 506 observations locales finales, données fictives, zéro échec de statut/langue/débordement, zéro erreur JavaScript, zéro appel distant. Deux POST locaux de choix/déclaration de correction. Rapport de vérification simulé.
- `contrast.json` : 160 échantillons, minimum 4,68:1, aucune défaillance sur les éléments contrôlés.
- `production.json` : 66 observations anonymes et un recontrôle réussi après un délai de chargement. La révision déployée est inconnue ; ces preuves ne concernent pas le code local non déployé.
- 148 tests ciblés réussis, un avertissement existant. Compilation, Ruff F/E9, Bandit medium/high, 88 templates Jinja et contrôle des espaces réussis. La CI complète du commit publié est consignée dans la PR.

Les scripts Chromium reproductibles sont `browser-smoke.py` et `contrast-smoke.py`, avec leur `fixture.py`. Ils nécessitent les dépendances de développement, Chromium et ses bibliothèques. Adapter le chemin Chromium dans les scripts si nécessaire ; les sorties sont dans `/tmp/noyaru-cross-browser`. Chaque lancement crée sa base locale temporaire et désactive le worker. Aucun identifiant réel. Le test de correction n’appelle pas le réseau ni un moteur d’analyse ; il injecte un rapport synthétique dans la fonction de vérification locale.

Les mesures de contraste composent les fonds calculés RGBA sans dégradé ; elles ne constituent pas un audit WCAG complet. Les contrôles au clavier et captures ont été relus. L’administration locale utilise un profil fictif ; la production est uniquement anonyme en lecture. Les limites et la matrice par écran figurent dans [le bilan](../UX_CROSS_JOURNEY_LOT16.md).
