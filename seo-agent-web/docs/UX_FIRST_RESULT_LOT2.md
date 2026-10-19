# Lot 2 — Du premier bilan à la correction suivante

Suite de [l’audit complet](UX_AUDIT_2026-10-19.md) et du [lot 1](UX_FOUNDATIONS_LOT1.md), sur main `4b691d6`. Le correctif qui rend les rapports lisibles avant de déclarer une analyse terminée et le bandeau signalant un rapport perdu sont conservés.

## Bilan ciblé et changements

| Écran / situation | Problème et conséquence | Réponse apportée | Priorité |
| --- | --- | --- | --- |
| Accueil, site existant | Carte de reprise, liste de prochaines étapes et réussite séparées : plusieurs invitations concurrentes. | Une carte commune avec une action liée à l’état réel, des étapes compactes et une aide dépliable. Connexions facultatives dans l’aide. | P1 |
| Accueil, premier résultat déjà obtenu | L’accomplissement initial pouvait prendre le pas sur la nouvelle correction choisie. | Le premier succès reste acquis ; l’action de reprise suit le dernier choix explicite. Une correction en cours est prioritaire sur le succès d’un autre projet. | P1 |
| Bilan et anomalies, résultat exploitable | Relancer l’analyse dominait visuellement le choix d’une correction. | La même carte invite à choisir, reprendre ou consulter une correction selon les données. Nouvelle analyse reste disponible en action secondaire. | P1 |
| Analyse active ou en annulation | Un bilan précédent pouvait détourner du suivi ; le bouton de lancement restait actif pendant une annulation. | Reprise de l’analyse active, message spécifique d’annulation et lancement désactivé pendant cet état. | P1 |
| Consultation historique | Une invitation courante au milieu d’un ancien rapport risquait de mélanger les contextes. | Retour au bilan actuel avant de poursuivre ; aucune ancienne analyse transmise à l’action de correction. Les liens de consultation des problèmes gardent l’analyse affichée. | P1 |
| Rapport perdu | Le bandeau explicatif de main ne remontait pas à l’accueil. | L’accueil signale le rapport manquant ; bilan et anomalies gardent le bandeau et proposent l’historique disponible. | P1 |
| Anomalies, premier rapport absent | Un projet autorisé sans rapport était annoncé « introuvable » avec HTTP 404. | État vide HTTP 200 : site conservé, bilan attendu ou reprise vers le projet. Projet inconnu et autre compte restent en 404. | P1 |
| Anomalies, filtre sans résultat | Le même texte servait au filtre vide et au bilan sans problème. | Message propre au filtre, suppression des filtres en gardant analyse et comparaison, distinction d’un résultat sans problème. | P1 |
| Bilan partiel sans problèmes listés | Zéro ligne pouvait être compris comme un bilan complet sans problème. | Explication des limites pour refus de pages, durée atteinte, données incomplètes ou absence de pages. Réussite seulement sur rapport exploitable et complet. | P1 |
| Projets sur téléphone | Le tableau restait à faire défiler pour atteindre mesures et actions. | Présentation en fiches à deux colonnes de mesures, identité et action sur toute la largeur ; mêmes éléments, formulaires et sélection multiple. | P1 |
| Identité du projet | Sans nom dans le rapport, le nom technique du dossier apparaissait. | Nom et adresse du projet autorisé utilisés sur accueil, bilan et anomalies. Date lisible localisée. | P2 |
| Comptes avec identifiants de projet identiques | Une analyse d’un autre compte accessible pouvait être rattachée au projet affiché. | Recherche et validation par propriétaire exact, identifiant du projet et type d’analyse ; priorité à l’analyse active même depuis un ancien lien de travail. | P1 |

## Parcours et règles communes

Site enregistré → analyse disponible → choix d’une page → préparation → modification déclarée → contrôle par une nouvelle analyse → résultat confirmé → choix suivant.

L’action principale indique l’étape réellement disponible. Déclarer une modification ne vaut pas vérification. Une réussite déjà confirmée reste acquise pendant la correction suivante. Un rapport historique, manquant ou partiel ne devient pas un bilan récent complet. Les occurrences sont expliquées comme des cas détectés : plusieurs problèmes peuvent toucher la même page.

La carte réutilisable `_next_step.html` et `journey.css` emploient les couleurs, bordures, boutons et espacements Noyaru existants. Progression nommée pour les aides techniques, étape courante marquée avec `aria-current="step"`, texte lisible sans animation, action sur toute la largeur sur téléphone. Les fiches mobiles gardent les en-têtes de tableau pour les aides techniques ; les cases de sélection ont un nom explicite. La sélection globale demeure visible hors des en-têtes masqués.

Les autorisations, quotas, réservations de pages et confirmations avant écriture continuent de passer par les contrôles existants. La nouvelle carte ne publie aucune correction ; elle ouvre le suivi ou soumet au point d’entrée existant de l’analyse.

## Vérification au navigateur

[Matrice principale](ux-first-result/evidence.json) : **212 affichages locaux**, 53 situations à 1440, 768, 390 et 360 px. Aucun débordement horizontal de page, aucune erreur JavaScript et aucun HTTP 500. Comptes, rapports et résultats de contrôle synthétiques ; serveur HTTP local et Chromium.

Situations : aucun projet, quota épuisé, aucun rapport, attente, analyse active, annulation demandée, échec, annulation confirmée, échec avec ancien bilan, problèmes disponibles, correction choisie, modification déclarée, résultat vérifié, nouvelle correction sélectionnée, résultat sans problème, pages refusées, limite de durée, rapport perdu, filtre sans résultat, consultation d’une ancienne analyse avec comparaison.

Interactions réelles des formulaires au navigateur : ajouter et analyser une adresse produit un seul projet et un seul travail en attente ; retour accueil et reprise ; choix d’une correction ; déclaration de modification ; retour au suivi ; réception d’un rapport de contrôle synthétique ; choix de la correction suivante et reprise sans effacer le premier succès. Sur mobile : tableau et lignes accessibles, sélection multiple, fenêtre d’ajout et fermeture au clavier, suppression des filtres avec contexte conservé, lien d’évitement et focus du contenu.

Le dernier ajustement de date et de suivi est aussi revérifié dans une [matrice ciblée](ux-first-result/focused-evidence.json). Les captures jointes correspondent aux données fictives : [accueil ordinateur](ux-first-result/projects-1440.png), [accueil téléphone](ux-first-result/projects-390.png), [bilan](ux-first-result/overview-1440.png), [filtre sans résultat](ux-first-result/filtered-issues-390.png), [correction suivante](ux-first-result/next-correction-390.png).

Les écrans authentifiés de production, les connexions Google/Bing/CMS réelles et un lecteur d’écran ne sont pas revendiqués comme testés dans ce lot. L’inventaire des autres écrans et le reste de la refonte demeurent dans l’audit. Aucun déploiement de ce lot n’est effectué.

## Contrôles automatisés

Les tests de parcours couvrent le choix réel du bouton, les étapes non acquises avant vérification, le contrôle qui retrouve le problème, la reprise d’une nouvelle correction, les limites du bilan, le retour après filtre et la séparation des comptes avec le même identifiant de projet. Les tests antérieurs sur le premier audit, les corrections, les CMS et les rapports perdus restent exécutés.

Ruff F/E9, compilation Python sur les cibles de CI et Bandit moyen/élevé passent. Résultats de la suite complète et de la CI GitHub ajoutés à la PR une fois disponibles.
