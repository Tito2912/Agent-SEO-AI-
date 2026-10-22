# Lot 20 — Retrouver l’état confirmé de l’automatisation

Base examinée : `6a59cc7`, après le lot 19. Branche et worktree séparés, aucune PR concurrente publiée lors de la première lecture. Les textes juridiques et le travail de Claude sont conservés.

## Audit avant modification

**L** : Chromium local, membre d’un projet fictif ; vrai backend, fournisseur GitHub fictif. **C** : code. Les coupures et pannes sont simulées localement, sans accès de production.

| Écran / état | Preuve | Problème et conséquence | Amélioration | Priorité |
| --- | --- | --- | --- | --- |
| Contenu → rédaction automatique, arrêt confirmé | L, C | Le serveur enregistre l’arrêt et le message annonce « éteint », mais le titre et le badge restent « activés/allumé ». Le client reçoit deux informations contradictoires. | Mettre à jour tous les indicateurs à partir du réglage confirmé par le serveur ; distinguer les modifications encore en attente. | P1 |
| Contenu → réponse perdue après l’enregistrement | L, C | Le serveur a arrêté le mode, mais le navigateur indique seulement « Erreur réseau » et permet de renvoyer la demande. Le client ignore si sa décision a été appliquée. | Afficher un état à confirmer, suspendre un nouvel envoi et proposer une lecture du réglage enregistré, sans recharger tout le contenu en préparation. | P1 |
| Réglages GitHub → erreur 503 | L, C | Le message demande d’actualiser l’état, mais les actions restent autorisées. | Appliquer le même verrou d’incertitude que pour une réponse interrompue, jusqu’à une lecture réussie. | P1 |
| Contenu → enregistrement en cours | C | Seul le bouton est désactivé ; les champs peuvent changer pendant l’envoi. Une réponse peut alors concerner d’autres choix que ceux affichés. | Verrouiller seulement les réglages concernés pendant la demande ; préserver le travail de rédaction. | P2 |
| Contenu → sans JavaScript | C | Le bouton d’enregistrement n’a pas de comportement natif, sans explication. | Conserver l’état enregistré consultable et expliquer que la modification nécessite JavaScript. | P2 |
| Contenu → langues du dépôt indisponibles | C, puis L après correction | L’envoi collecte uniquement les cases déjà chargées. Une liste absente peut remplacer les langues enregistrées par une liste vide. | Conserver les choix non représentés par les cases disponibles ; vérifier cette conservation avec un échec de chargement simulé. | P1 |

Le constat local avant changement est conservé séparément des validations finales. Une simulation de réponse perdue laisse réellement le POST local se terminer avant de couper son retour ; elle ne provoque aucune publication.

## Parcours cible

Consulter l’état enregistré → modifier les choix → voir qu’ils restent à enregistrer → confirmer → attendre sans changer les champs concernés → retrouver l’état confirmé et la prochaine échéance. Si le résultat est inconnu : actualiser uniquement ces réglages, puis reprendre la décision. Les accords explicites et la transaction du lot 19 restent inchangés.

## Réalisation et validation

Le nouvel endpoint **GET `/api/projects/{slug}/content/auto`**, authentifié et limité aux comptes accessibles, renvoie le réglage courant avec `Cache-Control: no-store`. Il ne lance ni rédaction ni fournisseur et ne modifie ni programmation, accord ou consommation. Le POST conserve ses champs de réponse existants et ajoute le même instantané public ; `last_run` et `depuis` restent internes.

Les indicateurs de Contenu sont mis à jour ensemble après confirmation. Les changements en attente sont signalés ; si la réponse est inconnue, le texte ne prétend plus qu’ils sont non enregistrés. La prochaine échéance est masquée jusqu’à une lecture réussie. L’actualisation après incertitude restaure les réglages enregistrés et demande à nouveau l’accord explicite ; une actualisation ordinaire préserve les modifications en cours. La rédaction manuelle reste disponible et son texte est conservé.

Dans les réglages GitHub, les réponses 5xx/408 et les réponses inexploitables imposent aussi une lecture de contrôle. Un refus de validation connu laisse la saisie disponible. La liste des dépôts et l’enregistrement ne peuvent plus se chevaucher ; les boutons de sélection sont verrouillés pendant une mutation. Le retrait confirmé d’un dépôt reste compatible avec la réponse existante.

Règles visuelles : « État enregistré » distingue le badge des champs modifiables ; un état à confirmer est écrit en toutes lettres, sans dépendre d’une couleur. Le message dynamique dispose d’un rôle `status`. Les deux actions de Contenu mesurent au moins 44 px et reviennent à la ligne sur petit écran ; le CSS reste limité à cette section. Sans JavaScript, les choix enregistrés sont lisibles, les champs désactivés et une aide explique la limitation.

| Vérification | Résultat / limites |
| --- | --- |
| [Reproduction avant modification](ux-automation-state/baseline.json) | **Trois défauts reproduits**, Chromium FR 360 px, sur `6a59cc7`. Une réponse réellement perdue après un POST local réussi, un arrêt confirmé et une panne de journal GitHub. [Arrêt contradictoire](ux-automation-state/stale-content-state-before-360.png), [réponse perdue](ux-automation-state/lost-content-response-before-360.png), [503 GitHub](ux-automation-state/github-503-before-360.png). |
| Tests automatisés | **157 réussis**, dont **huit nouveaux** dans `tests/test_automation_confirmed_state.py`. Lecture sans répéter les effets du POST, consommation et programmation inchangées, activation puis arrêt après perte de droits de rédaction, propriétaire/membre/autre compte/anonyme, slugs identiques de propriétaires distincts, ancien réglage sans réécriture. Les tests des accords, des transactions, des quotas, des réglages et de l’i18n existants passent. 43 avertissements de dépréciation existants. |
| [Chromium final](ux-automation-state/browser.json) | **58 observations locales** : FR/EN à 1 440/768/390/360 px pour arrêt, activation, réponse perdue après enregistrement, récupération par GET et 503 GitHub ; cas supplémentaires à 360 px : refus 400, 503/408, HTML, JSON invalide, succès incomplet, session expirée, lecture en échec, conservation des modifications, langues indisponibles, champs verrouillés, retrait GitHub et lecture sans JavaScript FR/EN. Quatre observations supplémentaires couvrent les demandes GitHub en cours, le refus connu et la session expirée pendant la lecture. Pas d’erreur JavaScript ni de débordement dans la matrice vérifiée. |
| [Contrastes](ux-automation-state/contrast.json) | **16 échantillons** des nouveaux états confirmés, minimum **4,66:1**. Pas un audit WCAG intégral ni une recette avec lecteur d’écran. |
| Contrôles statiques | Compilation Python, Ruff F/E9 sur backend/tests, syntaxe JavaScript, Bandit medium/high et contrôle des espaces réussis. Avertissements de dépréciation et `nosec` existants. La CI complète et la revue du dernier commit publié sont consignées dans la PR avant fusion. |
| Inventaire courant | Régénéré au commit applicatif : **202 routes HTTP et 89 templates** ; un GET de récupération ajouté. Les routes et états historiques restent documentés séparément. |

```sh
python -m pytest -q seo-agent-web/tests/test_automation_confirmed_state.py \
  seo-agent-web/tests/test_automation_consent_integrity.py \
  seo-agent-web/tests/test_les_modes_automatiques_sont_prouves.py \
  seo-agent-web/tests/test_le_mode_automatique_est_etroit.py \
  seo-agent-web/tests/test_le_mode_automatique_se_regle.py \
  seo-agent-web/tests/test_content_visibility_journey.py \
  seo-agent-web/tests/test_analysis_activity_journey.py \
  seo-agent-web/tests/test_i18n.py \
  seo-agent-web/tests/test_i18n_reglages_analyse.py
```

Base temporaire, worker désactivé, fournisseurs et comptes fictifs. [Récupération nécessaire dans Contenu](ux-automation-state/content-uncertain-after-fr-360.png) · [État confirmé en anglais](ux-automation-state/content-confirmed-after-en-360.png) · [GitHub à confirmer](ux-automation-state/github-uncertain-after-fr-360.png) · [Sans JavaScript](ux-automation-state/no-javascript-fr-360.png).

**Limites :** écrans clients authentifiés vérifiés avec des comptes locaux, sans nouvelle session authentifiée de production. Aucun OAuth réel rejoué, aucune rédaction, fusion de PR d’un site client, facturation, email ni publication distante. Aucun déploiement manuel de production. Pas de modification des règles d’automatisation, des transactions du lot 19, du moteur de langues ou des textes juridiques. Cinq traductions d’interface ajoutées au catalogue anglais existant ; branches et travail non publié de Claude préservés.
