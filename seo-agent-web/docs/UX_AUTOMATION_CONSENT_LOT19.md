# Lot 19 — Enregistrer l’accord avec le réglage automatique

Base examinée : `d3697d2`, après les lots 17 et 18. Aucune PR ouverte lors de la première lecture. Branche et worktree isolés ; textes juridiques, moteur de langues et branches de Claude conservés.

## Audit avant modification

**C** : code. **T** : endpoints locaux, comptes et transport GitHub fictifs, erreurs SQL injectées. Les observations navigateur sont ajoutées séparément après correction. Aucun accès de production ni écriture distante.

| Écran / étape | Preuve | Problème et conséquence | Amélioration | Priorité |
| --- | --- | --- | --- | --- |
| Réglages GitHub, mode automatique déjà actif, autre dépôt | C, T | La garde compare seulement le mode précédent. Un appel API peut remplacer le dépôt sans nouvel accord et sans nouvelle trace, alors que le nouveau dépôt hérite des fusions automatiques. | Comparer le dépôt normalisé et la branche cible ; demander l’accord pour une nouvelle cible automatique et conserver son ancien/nouveau périmètre. | P1 |
| Réglages d’analyse et fenêtre GitHub des corrections, enregistrement | C, T | Le réglage est validé en base avant l’écriture du journal, dont les erreurs sont absorbées. Le serveur peut annoncer une réussite sans preuve d’accord. | Réglage et preuve dans une seule transaction ; panne → erreur explicite, sans réussite ni enregistrement partiel. | P1 |
| Contenu, publication automatique et changement de section | C | Même ordre de validation séparée. Une nouvelle section peut aussi hériter de l’autorisation sans trace de ce changement. | Appliquer le même enregistrement indivisible et redemander l’accord lors du changement de destination automatique. | P1 |
| Modification concurrente pendant la lecture des droits GitHub | C | Le contrôle repart de l’ancien objet projet. Une désactivation ou un autre réglage intervenu entre-temps peut être écrasé. | Relire et verrouiller le projet dans la courte transaction d’enregistrement ; aucune requête fournisseur sous ce verrou. | P1 |
| Accord d’un membre pour un projet partagé | C | Le journal conserve l’acteur et le slug, sans identifiant du projet ou du propriétaire. Deux comptes peuvent avoir le même slug. | Ajouter ces identifiants à la preuve, en conservant acteur, langue et version de l’avertissement. | P2 |
| Cases d’accord, après un changement de destination | C, navigateur local | Un accord déjà coché peut rester présent après changement de dépôt, de branche ou de section. Il semble couvrir la nouvelle destination. | Décocher l’accord lors du changement ; bloquer l’envoi automatique tant que la nouvelle cible n’est pas confirmée. | P1 |
| Libellé d’accord des réglages GitHub | Mesure Chromium locale | Contraste de **4,39:1** sur le fond de l’avertissement, inférieur à 4,5:1 pour ce petit texte. | Style partagé, texte lisible, cible d’au moins 44 px et repère de focus ; conserver les textes de Claude. | P2 |

## Parcours cible

Choisir la cible → relire l’avertissement existant → confirmer explicitement → vérifier les accès → relire le réglage courant → enregistrer ensemble la décision et sa preuve → afficher la réussite. Une panne ne devient pas une confirmation. Un nouvel essai identique après une réussite ne crée pas une seconde activation ; une nouvelle destination automatique demande un nouvel accord.

## Réalisation et validation

Code métier `be4e4e6`, tests supplémentaires `df2d591`, composant commun `6fd9b2e`. Les textes contractuels, avertissements de Claude, catalogue anglais, moteur de langues, prix, quotas, correcteurs et planification restent conservés.

- Dépôt GitHub comparé après normalisation des espaces et de la casse ; branche comparée exactement. Une nouvelle destination automatique exige l’accord et garde l’ancienne/nouvelle cible. La répétition du même réglage ne crée pas une autre activation.
- Même protection pour la section de publication automatique. Les trois interfaces décochent l’accord après changement de destination ; la sélection de dépôt de l’écran Corrections transmet aussi le site, comme les réglages du lot 18. Le script passe à `?v=3`.
- Le projet est relu dans une courte transaction, après la lecture des droits GitHub. Verrou de ligne PostgreSQL ou `BEGIN IMMEDIATE` SQLite ; aucun appel fournisseur sous ce verrou. Réglage et preuve sont validés ensemble. Le journal général des autres actions conserve son comportement.
- Une erreur SQL renvoie **503**, avec le message existant traduit invitant à actualiser l’état. Aucun succès n’est affiché. Les erreurs de contrainte testées conservent l’ancien réglage et ne laissent aucune preuve partielle ; le nouvel essai réussit après rétablissement. Cela concerne aussi un retrait : l’erreur ne signifie pas que l’automatisation a été arrêtée. Une perte de réponse après validation reste un résultat à actualiser, sans prétendre annuler une transaction déjà validée.
- La preuve conserve acteur, propriétaire, identifiant du projet, cible, langue et version de l’avertissement. Les données existantes ne sont pas présentées comme une preuve d’accord reconstituée.
- Le composant d’accord est commun aux trois écrans : texte lisible, label d’au moins 44 px, case utilisable au clavier et focus visible. L’accord reste masqué lorsque la publication automatique n’est pas choisie. Aucun nouveau texte juridique.

| Vérification | Résultat et portée |
| --- | --- |
| [Reproduction avant modification](ux-automation-consent/baseline.json) | **Six échecs attendus** sur `d3697d2` : autre dépôt/branche sans accord acceptés ; quatre cas de panne du journal annonçant une réussite. Endpoints et base locaux. |
| Tests ciblés | **149 réussis** dans la sélection initiale : intégrité, modes automatiques, réglages, droits, périmètre GitHub et i18n. Après ajout des trois derniers cas, les **19 nouveaux tests d’intégrité** passent ; **46 tests de traduction et consentement** passent après le composant commun. Ces sélections se recoupent. Avertissements de dépréciation existants. |
| Nouveaux cas d’intégrité | Nouvelle cible refusée sans accord ; normalisation et répétition ; preuve avec acteur/propriétaire ; panne d’insertion du journal ou de mise à jour du projet ; nouvel essai ; section de contenu ; état modifié ou projet supprimé durant la lecture fournisseur ; CSRF et autre compte ; deux demandes identiques simultanées, GitHub et contenu. |
| [Chromium final](ux-automation-consent/browser.json) | **30 observations**, FR/EN à 1 440/768/390/360 px, sur les trois interfaces. Changement de cible, accord décoché, refus sans nouvel accord, confirmation au clavier et enregistrement ; six pannes de journal à 360 px puis reprise. Aucun débordement, erreur JavaScript ou appel extérieur. Fournisseur et comptes fictifs, vrais endpoints locaux. |
| [Contrastes avant](ux-automation-consent/contrast-before.json) / [après](ux-automation-consent/contrast.json) | **24 échantillons finaux**, minimum **13,27:1**, fonds composés. Minimum du libellé des réglages avant modification : **4,39:1**. Les trois labels atteignent 44 px. Pas de certification WCAG intégrale. |
| [PostgreSQL réel jetable](ux-automation-consent/postgres-local.json) | **Six contrôles**, PostgreSQL **16.15** : activation et preuve, refus d’une autre cible sans accord, deux violations réelles de contrainte du journal avec retour arrière puis nouvel essai, deux paires de demandes identiques simultanées avec une seule preuve. Comptes et GitHub fictifs ; ce n’est pas toute la suite sur PostgreSQL. Paquets Ubuntu extraits dans `/tmp`, sans installation système ; socket Unix privée, serveur arrêté après les contrôles. |
| Contrôles statiques | Compilation Python, Ruff F/E9, syntaxe JavaScript, Bandit medium/high et contrôle des espaces réussis. Avertissements `nosec` existants. CI complète du dernier commit publié consignée dans la PR avant fusion. |
| Inventaire courant | Régénéré au dernier commit applicatif : **201 routes HTTP et 89 templates**. Aucun endpoint supplémentaire. |

```sh
python -m pytest -q seo-agent-web/tests/test_automation_consent_integrity.py
python -m pytest -q seo-agent-web/tests/test_les_modes_automatiques_sont_prouves.py \
  seo-agent-web/tests/test_le_mode_automatique_se_regle.py \
  seo-agent-web/tests/test_le_mode_automatique_est_etroit.py \
  seo-agent-web/tests/test_analysis_activity_journey.py \
  seo-agent-web/tests/test_i18n.py \
  seo-agent-web/tests/test_i18n_traduction_complete.py \
  seo-agent-web/tests/test_i18n_reglages_analyse.py \
  seo-agent-web/tests/test_la_liste_montre_les_projets_relies_a_github.py
```

Les passages locaux utilisent une base temporaire, `DATABASE_URL` vide et le worker désactivé ; la recette PostgreSQL possède son propre cluster jetable. [Erreur dans les réglages](ux-automation-consent/settings-failure-fr-360.png) · [Erreur dans Contenu](ux-automation-consent/content-failure-fr-360.png) · [Erreur dans Corrections](ux-automation-consent/corrections-failure-fr-360.png).

Ce lot vérifie l’enregistrement technique de nouvelles décisions. Il ne constitue pas un avis juridique, ne certifie pas les preuves historiques et ne rejoue ni OAuth ni publication réelle. Aucun site client modifié, paiement, email ou déploiement manuel de production.
