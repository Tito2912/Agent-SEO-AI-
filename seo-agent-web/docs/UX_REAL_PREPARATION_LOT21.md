# Lot 21 — Reprendre la préparation sans impasse

Base examinée : `32ba38d`, après le lot 20. Aucune PR concurrente publiée ; branche et worktree séparés. Les textes juridiques, le moteur de langues et les branches de Claude sont conservés.

## Audit avant modification

**P** : production, navigateur Edge dédié, session du compte témoin ouverte par le propriétaire. **A** : production anonyme, Chromium. **C** : code. Les contrôles locaux après correction sont consignés séparément. Aucun identifiant de compte, dépôt privé, cookie, secret ou contenu source client n’est versionné.

| Écran / état | Preuve | Problème et conséquence | Amélioration / priorité |
| --- | --- | --- | --- |
| Choisir une correction → redirections volontaires | P, C | Le choix propose des redirections décrites comme une canonicalisation volontaire, « pas des défauts ». Un novice peut chercher à supprimer un comportement correct. | Exclure du choix guidé les URL explicitement classées « canonicalisation attendue » par les preuves du rapport ; conserver diagnostics, inconnues, boucles et suivis existants. P1. |
| Préparation d’un aperçu GitHub → ancienne opération interrompue | P, C | Le serveur refuse la demande avant la préparation. La fenêtre garde le bouton disponible et ne propose aucun lien d’assistance. Le suivi général ne contient pas nécessairement le reçu technique qui bloque le compte. | Suspendre les envois de cette fenêtre sur le signal `recovery_required`, expliquer le contrôle nécessaire et proposer suivi/assistance. Conserver le verrou et la procédure opérateur. P1. |
| Préparation → connexion GitHub à vérifier | C | Le lien de secours cible `#github-section`, absent de l’écran actuel. | Cibler `#github-repo`, le panneau existant qui s’ouvre à partir de son ancre. P2. |
| Site témoin → connexions et version publiée | A, P | Le précédent bilan ne prouvait ni la lecture réelle des dépôts ni le déploiement du dernier lot. | Comparer les ressources publiées, lire le nouvel endpoint et les repères de l’interface ; distinguer ces constats d’un SHA complet du serveur. Recette. |

## Parcours cible

Choisir un problème réellement à traiter → choisir une page précise → consulter son état actuel → préparer une seule proposition → relire les changements → confirmer la proposition → autoriser son application → vérifier par une nouvelle analyse. Une préparation interrompue ouvre un parcours de vérification ; elle ne devient ni un nouvel envoi automatique ni une preuve de modification du site.

## Recette réelle avant correction

- **32 observations anonymes** : sept pages publiques FR/EN, ordinateur 1 440 px et mobile 390 px ; deux redirections d’écrans privés vers la connexion, protection 401 de l’API et santé HTTP 200. Pages publiques attendues en HTTP 200, langues correctes, aucun débordement ni erreur JavaScript relevé. GET/HEAD uniquement.
- **Trois ressources** des lots 19/20 identiques octet pour octet à `main` : `analysis-github.js?v=4`, `content-journey.css?v=2`, `automation-consent.css?v=1`. Le SHA complet de production n’est pas exposé et reste non attesté.
- Session du compte attendu et projet témoin confirmés. Le nouvel endpoint GET de Contenu renvoie son instantané ; l’interface GitHub charge le script v4 et présente le mode enregistré « review ». Aucun réglage modifié.
- Le bouton de lecture des dépôts répond en moins d’une seconde, retrouve le dépôt enregistré et laisse la sélection intacte. Ce contrôle réel complète la limite du lot 18 ; il ne teste pas une écriture GitHub.
- Les écrans Google/Bing affichent maintenant des données ; leurs valeurs commerciales ne sont pas exportées dans le dépôt. Les consentements OAuth ne sont pas rejoués.
- Choix explicite d’une méta-description encore longue dans le HTML public actuel, sur une seule page française. Le nouveau suivi est créé sans effacer l’ancien reçu. Aucune modification déclarée comme appliquée.
- Le propriétaire autorise explicitement un aperçu IA et son décompte. Une seule demande est envoyée par l’interface ; le serveur répond qu’une ancienne correction interrompue doit être reprise. Aucun aperçu ni PR créé ; compteur IA confirmé à **0/2** avant et après. Le code relie ce message au contrôle du journal avant exécution du correcteur.
- La procédure existante `ops/CORRECTION_RECOVERY.md` prévoit une inspection opérateur. Une commande de liste en lecture seule, limitée au compte témoin, est préparée pour sa console serveur. Aucun verrou, reçu, événement de consommation ou branche n’est supprimé ou réinitialisé.

## Réalisation et validations finales

Code applicatif `e38a0d6`. Le choix guidé filtre les URL par preuve exacte, uniquement pour `redirect_3xx`. Les preuves absentes, anciennes, inconnues ou contradictoires ne sont pas transformées en preuve de bon fonctionnement. Une sélection forgée est refusée par le POST existant, qui relit la même liste de candidats. Les compteurs, rapports, suivis historiques et règles de vérification restent intacts ; un diagnostic conservé ne devient pas un certificat de site sans anomalie.

Dans la fenêtre partagée, `recovery_required` suspend préparation et confirmation, même après un changement de case d’accord. Le titre et le message expliquent le contrôle nécessaire ; les liens vers le suivi et `/support` restent accessibles. Fermer puis rouvrir la fenêtre constitue une nouvelle décision explicite ; aucun essai automatique n’est ajouté. Les refus de validation ordinaires conservent leur comportement. Le secours GitHub ouvre le panneau existant `#github-repo` du bon projet. Le script passe à `?v=4` sur ses cinq écrans ; aucune modification du layout partagé ou du moteur de langues. Trois traductions ajoutées au catalogue anglais existant.

| Contrôle | Résultat / limite |
| --- | --- |
| [Production anonyme](ux-real-preparation/public.json) | **32 observations** et **trois empreintes de ressources**, avant ce correctif. Versions publiées cohérentes avec les lots 19/20, SHA complet non attesté. Aucun formulaire soumis. |
| [Observations connectées expurgées](ux-real-preparation/authenticated-observations.json) | **11 constats**, sur la session du compte témoin : identité attendue sous forme booléenne, sélection du projet, choix de correction, lectures des états, lecture des dépôts, quota, refus et liens proposés. Ce relevé n’est pas une recette complète de toutes les pages authentifiées. Aucune donnée nominative ni nom de dépôt privé exporté. |
| [Pages publiques ciblées](ux-real-preparation/public-page-targets.json) | Trois pages répondent 200, avec une description unique. Longueurs normalisées conservées sans republier les textes. La page française choisie comporte encore 172 caractères ; le constat du rapport reste observable. Il ne constitue pas une promesse de classement ni une limite universelle imposée par Google. |
| [Reproduction locale](ux-real-preparation/baseline.json) | Un refus de même forme que la production, injecté dans Chromium FR à 360 px, garde bien le bouton actif et le lien d’assistance absent avant correction. [Capture](ux-real-preparation/interrupted-before-fr-360.png). |
| Tests ciblés | **1 318 réussis**, dont **neuf nouveaux** : preuve attendue exclue du choix mais conservée dans le diagnostic, refus du POST forgé, boucle conservée, preuves inconnues/anciennes/contradictoires, remplissage des trois choix et maintien d’un suivi historique. Parcours de relecture, permissions, reprise, preuves et traductions existants réussis. Huit avertissements de dépréciation existants. |
| [Chromium final local](ux-real-preparation/browser.json) | **37 observations** : choix guidé et trois modes de préparation × FR/EN × 1 440/768/390/360 px, refus connu distinct, refus à la confirmation après aperçu fictif, destination GitHub exacte et deux choix sans JavaScript. Une demande par refus de préparation ; aucun nouvel envoi après activation forcée de la case ou clic sur le bouton suspendu. Fermeture et retour du focus contrôlés. Aucun débordement, erreur JavaScript ou appel extérieur. Réponses de refus simulées, comptes et rapports fictifs ; aucune IA ni écriture GitHub. |
| [Contrastes](ux-real-preparation/contrast.json) | **24 échantillons**, minimum **5,01:1**, sur le message et les liens de reprise. Cibles d’au moins 44 px, focus clavier visible par le halo existant. Ce n’est pas un audit WCAG intégral ni une recette avec lecteur d’écran. [FR mobile](ux-real-preparation/interrupted-after-fr-360.png) · [EN mobile](ux-real-preparation/interrupted-after-en-360.png). |
| Contrôles statiques | Compilation Python, Ruff F/E9, syntaxe JavaScript, Bandit medium/high et contrôle des espaces réussis ; avertissements `nosec` existants. La CI complète et la revue du dernier commit publié sont consignées dans la PR avant fusion. |
| Inventaire courant | Régénéré au commit applicatif : **202 routes HTTP et 89 templates** ; aucune nouvelle route ou template. |

```sh
python -m pytest -q seo-agent-web/tests/test_guided_actionable_choices.py \
  seo-agent-web/tests/test_first_verified_correction.py \
  seo-agent-web/tests/test_next_step_journey.py \
  seo-agent-web/tests/test_correction_explicit_review.py \
  seo-agent-web/tests/test_preview_must_be_comparable.py \
  seo-agent-web/tests/test_correction_evidence.py \
  seo-agent-web/tests/test_correction_capability_policy.py \
  seo-agent-web/tests/test_i18n.py
```

**Limite opérationnelle :** ce lot corrige les invitations et les sorties de l’interface. Il ne libère aucune opération du journal. La suite de la recette réelle dépend du résultat de l’inspection opérateur prévue par le [runbook existant](../ops/CORRECTION_RECOVERY.md). Ni aperçu abouti, nouvelle PR du site témoin, application ni nouvelle analyse ne sont attestés. Aucun déploiement manuel de production, paiement, changement de formule, réinitialisation de reçu ou modification du site client.
