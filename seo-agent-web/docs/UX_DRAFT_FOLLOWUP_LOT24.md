# Lot 24 — garder la page choisie après le brouillon GitHub

## Bilan et priorité

| Écran / étape | Vérification | Problème et conséquence | Correction | Priorité |
| --- | --- | --- | --- | --- |
| Confirmation de l’aperçu simple | Code et reproduction en tests | Le reçu de PR remplace toute la note de tâche et efface `first_correction.selected_at` ainsi que le texte du client. | Conserver la sélection et les notes lors de l’enregistrement du nouveau reçu. | P1 |
| Correction guidée après confirmation | Lecture réelle du compte témoin : une cible guidée et une ancienne sélection existent. Confirmation non exécutée avant correction. Reproduction locale dans Chromium. | Sans le repère, le suivi peut revenir vers l’ancienne page. | Le choix le plus récent garde sa priorité et présente la PR de cette page. | P1 |
| Reprise après perte de réponse GitHub | Reproduction en tests avec PR acceptée et réponse perdue | La restauration du journal réécrit elle aussi la note et perd le choix du client. | Appliquer la même conservation pendant la restauration, sans nouvelle écriture GitHub. | P1 |
| État de progression | Code, tests et Chromium local | Recopier toute l’ancienne note conserverait une déclaration de publication ou une preuve de contrôle obsolète. | Garder uniquement la date du choix et le texte du client ; utiliser le nouveau reçu de proposition. | P1 |

Le parcours cible reste : aperçu relu → consentement → PR en brouillon → relecture et publication explicite → déclaration de mise en ligne → analyse de contrôle. Une PR créée ne signifie jamais que le site est corrigé. Les composants, libellés FR/EN, étapes et règles visuelles existants sont conservés.

## Mise en œuvre

`first_correction.proposal_note` conserve les données durables du client, puis sérialise le nouveau reçu serveur. Appel au moment de la confirmation simple et de la restauration des tâches depuis le journal. Aucun changement de schéma, quota, droits, verrous, fournisseurs IA ou confirmations.

Dossier et branche isolés depuis `main` après #56 (`2a246a0`). Aucun changement du travail juridique de Claude (#58), des catalogues ou du moteur de langues.

## Validation

- Régression reproduite avant correctif : deux échecs pour la sélection perdue, en confirmation normale et en récupération après interruption.
- **116 tests ciblés réussis, 2 ignorés** : correction client, première correction vérifiée et activation. Les nouveaux cas vérifient aussi le crédit unique, l’appel IA unique, la PR en brouillon, les notes conservées et l’absence de fausse réussite.
- **8 parcours Chromium locaux**, FR/EN, 1440/768/390/360 px : préparation, consentement, confirmation et retour au suivi passent par les vraies routes de l’application, sur comptes et rapports fictifs. GitHub et IA sont simulés. Le suivi conserve la page et le lien GitHub, à l’étape « Relire ». Aucun débordement horizontal, erreur JavaScript ou appel externe.
- Compilation, Ruff et Bandit medium/high réussis. La suite complète et la CI sont consignées dans la PR avant fusion.

Preuves : [résultats Chromium](ux-draft-followup/browser.json), [suivi mobile français](ux-draft-followup/followup-fr-mobile.png).

## Recette réelle distincte

L’aperçu payé du compte témoin reste ouvert et inchangé. Le propriétaire a autorisé la création de sa branche et de sa PR en brouillon ; cette opération attend la correction du suivi. Aucun nouveau modèle, crédit, nouvel audit, PR du site témoin, fusion client ou déploiement manuel n’est déclenché par ce lot.
