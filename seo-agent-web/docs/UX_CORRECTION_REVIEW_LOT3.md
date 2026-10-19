# Lot 3 — Préparer, relire et suivre les corrections

Suite de l’[audit complet](UX_AUDIT_2026-10-19.md) et du [premier résultat](UX_FIRST_RESULT_LOT2.md), sur main `d215d40` après fusion de la PR #24. Ce lot traite en priorité la validation explicite et les repères du parcours de correction.

## Bilan ciblé et réponse apportée

| Écran / parcours | Problème et conséquence | Amélioration | Priorité |
| --- | --- | --- | --- |
| Automatisation, correctif unitaire | Le script envoyait `confirm:true` dès réception de l’aperçu. Le client n’avait pas d’occasion de relire. Constat initial dans le code. | Fenêtre commune de préparation puis relecture ; une case de confirmation et un bouton distinct précèdent la création de la proposition. | P0 |
| API de correctif unitaire, mode automatique | Le serveur rappelait lui-même l’application après la génération de l’aperçu. Corriger le seul navigateur ne suffisait pas. | La préparation retourne un aperçu dans tous les modes. L’application exige une requête de confirmation distincte. Elle ne rappelle pas le modèle et ne facture pas une deuxième proposition. | P0 |
| Corrections et détail d’anomalie, propositions GitHub | Trois implémentations de préparation, messages et reprise. L’ancien détail montrait seulement les trente premières lignes du fichier. | Composant commun avec page concernée, dépôt, branche, coût annoncé et différences complètes. Les anciens aperçus restent lisibles en version proposée complète, explicitement distinguée d’une comparaison. | P1 |
| Préparation étendue / globale depuis ces écrans | Le réglage automatique du projet pouvait autoriser une fusion ultérieure sans nouvelle lecture du changement préparé. | Confirmation du périmètre et du coût avant préparation du brouillon ; option serveur `review_only:true`, sans droit de fusion automatique. La relecture et la fusion humaines se font sur GitHub. Les traitements globaux ne promettent pas un aperçu du diff avant la création de leur branche. | P0 |
| Réponse de création perdue | Un échec réseau peut survenir après la création effective de la proposition. Une répétition aveugle peut être confuse et coûteuse. | Recherche bornée par lecture du suivi, avec comparaison des propositions déjà présentes ; aucun second envoi d’écriture automatique. Le lien de reprise reste disponible. | P1 |
| Proposition GitHub non publiée | Le statut « en cours » pouvait déjà être interprété comme une modification déclarée, avec invitation prématurée au contrôle. | Relecture/publication d’abord ; déclaration explicite de mise en ligne avant le contrôle. L’accueil reprend cet état. Une preuve de résolution déjà acquise reste valide. | P1 |
| Statuts manuels | « Corrigé » pouvait être compris comme un résultat confirmé. | « Modifié · à vérifier » et « Déclarées modifiées ». Une déclaration de modification reste distincte des preuves calculées par l’analyse. | P1 |
| Correction guidée et WordPress unitaire | Progressions différentes au changement de méthode. | Même macro et quatre étapes : Préparer → Relire → Appliquer → Vérifier ; pour GitHub, Proposer remplace Appliquer. Après résultat WordPress vérifié, accès au choix de la correction suivante. | P1 |
| Lot WordPress | Plusieurs pages peuvent être à des étapes différentes. | Compteurs À préparer / À relire / État à confirmer / Enregistrées, puis nombre vérifié parmi les pages enregistrées. Aucune progression globale déclarée terminée sur un succès partiel. | P1 |
| Corrections avec GitHub sur téléphone | Le groupe de boutons conservait une largeur de 521 px à 390 px malgré le retour à la ligne. | Le groupe peut rétrécir ; les boutons de statut se répartissent sur deux lignes avec des cibles tactiles plus grandes. | P1 |
| Détail du problème | Le nom technique du projet remplaçait son nom usuel dans le fil de navigation ; destination GitHub absente de la fenêtre. | Nom du projet autorisé, dépôt et branche explicites ; préparation conservant l’analyse de référence. | P2 |

Les constats P0 d’origine étaient issus du code, sans exécution sur un dépôt client. Leur nouvelle séquence est maintenant vérifiée au navigateur local avec API de correction simulée, et séparément par les routes HTTP réelles avec transport GitHub simulé.

## Parcours et règles communes

Préparer une proposition → relire ce qui change → confirmer l’étape d’écriture → publier selon la méthode du site → faire vérifier le problème par une nouvelle analyse.

- **Manuel** : suivre les instructions, déclarer la modification publiée, puis faire contrôler la page. La déclaration seule ne valide pas le résultat SEO.
- **WordPress** : valeur proposée, aperçu avant/après, application explicitement validée, résultat distinct de l’enregistrement ; rétablissement toujours disponible selon les contrôles existants.
- **GitHub unitaire** : préparation facturée une seule fois, relecture des différences, création explicitement confirmée d’une proposition, vérifications du dépôt puis lecture et fusion sur GitHub. Le résultat SEO reste à contrôler après publication.
- **GitHub étendu/global** : explication du périmètre et des limites avant préparation ; le diff se relit dans le brouillon GitHub, avec fusion humaine requise pour ces actions d’interface. Les droits des autres traitements déjà configurés ne sont pas déduits des nouveaux boutons.

La fenêtre native gère le focus et son retour au déclencheur ; sa fermeture ne vaut jamais confirmation. Le titre demeure visible pendant le défilement, les différences sont consultables au clavier et les erreurs persistent avec leur lien de récupération. Les étapes actuelles portent `aria-current="step"`. Les résultats partiels, une session expirée, un quota atteint, un conflit et une réponse manquante reçoivent des messages distincts.

L’option de relecture absente ou fausse garde l’identité des anciens reçus de correction. Une reprise ne réécrit pas leur clé et ne relance pas silencieusement une préparation déjà payée. Les contrôles de compte, de quota et de changement concurrent du fichier restent appliqués côté serveur.

## Validation

[Matrice navigateur principale](ux-correction-review/evidence.json) : **81 affichages locaux** à 1440, 768, 390 et 360 px, plus une [recette ciblée](ux-correction-review/focused-evidence.json) du lot WordPress mixte et de la confirmation mobile. Aucun débordement horizontal, HTTP 500 ni erreur JavaScript dans ces matrices.

Parcours contrôlés : ouverture sans requête de correction ; préparation sans confirmation implicite ; relecture obligatoire ; annulation/Escape et retour du focus ; double clic ne donnant qu’une confirmation ; analyse de référence conservée ; préparations étendues/globales avec relecture humaine ; résultat partiel ; quota/session ; ancien aperçu ; fichier ayant changé ; réponse perdue récupérée par lecture, sans renvoi de l’écriture.

La recette GitHub du navigateur intercepte l’API de correction : elle prouve les interactions et les requêtes envoyées, pas une connexion à GitHub. Les tests HTTP vérifient séparément la politique serveur, les reçus et la facturation avec des transports simulés. La recette WordPress passe par le backend réel local et un transport WordPress fictif : aperçu sans écriture, application validée, rapport synthétique de contrôle et choix suivant. Le lot mixte garde une page à préparer alors qu’une autre est enregistrée puis vérifiée.

[Relecture ordinateur](ux-correction-review/review-1440.png) · [Confirmation téléphone](ux-correction-review/confirmation-390.png) · [Lot partiel](ux-correction-review/batch-mixed-360.png) · [Résultat WordPress](ux-correction-review/verified-390.png) · [Proposition encore à publier](ux-correction-review/github-awaiting-publication-390.png).

Ruff F/E9, compilation Python, analyse des 73 templates Jinja et Bandit moyen/élevé sont exécutés sur les cibles de CI. Les résultats chiffrés des tests ciblés et de la CI complète figurent dans la PR.

Les connexions authentifiées de production, les opérations sur des dépôts/sites clients réels et un lecteur d’écran ne sont pas revendiqués comme testés. Ce lot ne déploie pas de modification en production. Les autres chantiers de navigation, connexions et contenus restent décrits dans l’audit.
