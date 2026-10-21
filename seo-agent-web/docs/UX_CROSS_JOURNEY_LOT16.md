# Lot 16 — Recette transversale et navigation commune

## Audit avant modification

Base `d7f4f24`, intégrant les PR #41, #43, #44 et #46 de Claude et la refonte jusqu’à #45. Aucune PR ouverte lors de la première lecture. Branche/worktree isolés. Les textes juridiques, articles, écrans Activité/Réglages et moteur de langues de Claude sont conservés. Le lot concerne les comportements communs et le bilan de couverture.

**L — Chromium local** : [21 constats initiaux](ux-cross-journey/baseline.json), comptes fictifs, FR/EN, 360 px, uniquement GET. **C — code** : composants publics, fermeture des confirmations, générateur d’inventaire, liens et transitions. Les preuves de production anonymes et la recette locale transversale sont ajoutées séparément ci-dessous ; elles ne valent pas accès authentifié en production.

| Écran / état | Preuve | Problème et conséquence | Amélioration | Priorité |
| --- | --- | --- | --- | --- |
| Support, statut, documentation, article d’aide, guides et trois pages juridiques, personne connectée à 360 px en français | L, C | Groupe langue/tableau de bord/déconnexion trop large. Huit pages débordent ; les Tarifs ont déjà leur correctif propre. | Harmoniser l’en-tête commun et conserver son retour vers le tableau de bord, la langue et la déconnexion explicite. | P1 |
| Information cookies sans JavaScript | L, C | Bandeau fixe superposé au contenu, bouton Fermer sans effet. | Information lisible dans le flux et bouton absent sans JavaScript ; fermeture et mémorisation facultative lorsque le script fonctionne, y compris si le stockage est refusé. Aucun changement du texte juridique. | P1 |
| Accès clavier des pages publiques | C | Pas de lien d’évitement ni de cible principale, contrairement à l’application. | Réutiliser le lien « Aller au contenu » traduit et une cible principale accessible. | P2 |
| Confirmation en anglais dans l’application | L, C | Bouton et nom accessible « Fermer » restent en français. | Traduction via le mécanisme existant et actualisation de version du script ; succès persistant et retour du focus conservés. | P2 |
| Inventaire et état de l’audit | C | Générateur lié à une date, une révision et un total de routes anciens ; une relance écrase les compléments manuels. Variantes anglaises des articles absentes. | Inventaire courant séparé, révision et compteurs calculés, variantes de contenu et limites d’analyse explicites ; conserver le relevé historique et actualiser l’autorisation de fusion. | P1 |
| Facturation, retour vers un site dont le nom est long, propriétaire et membre | L, C | La recette transversale relève quatre débordements à 360 px, en français et en anglais : le bouton de retour conserve son texte sur une seule ligne. | Autoriser le retour à la ligne dans ce bouton et maintenir une cible de 44 px. | P1 |

## Règles et parcours de recette

Retrouver la navigation après une lecture publique ; changer de langue ; passer au contenu au clavier ; fermer une information ou une confirmation sans perdre le point de reprise. Le parcours client contrôlé reste choix d’un site → bilan → problème → correction choisie → modification déclarée → résultat d’une analyse simulée → correction suivante. Une déclaration manuelle n’est jamais présentée comme une correction vérifiée.

Les données, modèles, connecteurs et rapports locaux sont fictifs. Aucune modification distante de site, aucun email ni paiement réel. La production n’est parcourue qu’anonymement et en lecture seule. La recette couvre les écrans et états effectivement listés, pas toutes les variantes permises par chaque formulaire.

## Réalisation et validation

Code `af9f55139cf1a4bb027f1c2c29ad20661e9f0874`. Les changements applicatifs sont limités aux layouts communs, à leurs styles/scripts, au bouton de retour de facturation et à une entrée du catalogue anglais. Aucun gestionnaire métier, calcul, quota, droit ou texte juridique n’est modifié. Les nouvelles ressources et celles modifiées ont des URL versionnées.

- En-tête public : actions repliables, liens et boutons de 44 px, lien d’évitement et cible principale ; la barre de liens conserve son défilement local sur téléphone. Le correctif spécifique des Tarifs est remplacé par la règle commune.
- Information cookies : texte inchangé, lien souligné, affichage dans le flux sans JavaScript. Avec JavaScript, fermeture au clavier, mémorisation facultative et retour du focus au contenu. Le refus du stockage navigateur ne bloque plus la fermeture. Ce bloc d’information n’est plus déclaré comme une fenêtre de dialogue.
- Confirmation : fermeture en français/anglais, nom accessible traduit, message persistant et retour du focus conservés. Le lien vers un site dont le nom est long peut revenir à la ligne dans Abonnement.
- Inventaire : sortie courante séparée, date/révision et compteurs calculés, variantes des pages juridiques repérées et les 60 variantes d’articles recensées ; le relevé historique et ses compléments ne sont pas écrasés.

| Contrôle exécuté | Résultat et portée |
| --- | --- |
| [Recette Chromium locale](ux-cross-journey/browser.json) | **506 observations**, FR/EN à 1 440/768/390/360 px. Aucun débordement du document, échec de statut/langue attendu ou erreur JavaScript relevé. Les tableaux et la barre publique peuvent conserver leur défilement local prévu. |
| Navigation et états connectés locaux | Tous les liens réels de la barre latérale du propriétaire, projet sans analyse, choix/détail de correction, confirmation persistante puis fermeture au clavier ; compte neuf sans site. Membre d’un projet partagé et six écrans d’administration à 1 440/360 px, FR/EN. Session retirée : retour à la connexion. Les pages sont consultées, tous leurs formulaires ne sont pas soumis. |
| Articles | Les **42 variantes de documentation et 18 de guides** sont affichées une fois chacune à 390 px dans leur langue réelle. Ce contrôle n’est pas une validation éditoriale ou juridique des textes. |
| Parcours complet de correction | Un membre choisit une correction via les vrais formulaires locaux, puis déclare la modification. Le statut reste `in_progress`. Un rapport d’analyse **simulé**, traité par la vraie fonction de vérification, ouvre le résultat puis le choix suivant ; aucune correction suivante n’est choisie automatiquement. Deux POST locaux, aucun lancement d’analyse, aucun modèle ou fournisseur. |
| Sans JavaScript / stockage refusé | Support, documentation, tarifs, CGU, FR/EN à 1 440/360 px : information dans le flux, fermeture cachée. Avec script : fermeture, mémorisation ou stockage refusé et reprise du focus testés dans les deux langues. Menu mobile ouvert/fermé au clavier. |
| [Contrastes ciblés](ux-cross-journey/contrast.json) | **160 échantillons**, minimum **4,68:1**, fonds RGBA composés. Textes de navigation, information cookies, fermeture de confirmation et retour long de facturation ; boutons/liens modifiés d’au moins 44 px. Ce n’est pas une certification WCAG. |
| [Production anonyme](ux-cross-journey/production.json) | **66 observations initiales** : 40 pages publiques, 24 pages d’accès et 2 redirections d’écrans privés. FR/EN à 1 440/390 px. Un délai de chargement sur l’inscription anglaise à 390 px ; **recontrôle réussi, HTTP 200**, chargement terminé. Aucun débordement sur les pages chargées, ni erreur JavaScript. Seulement GET/HEAD ; aucun formulaire soumis. Révision déployée non déterminée. |
| Tests ciblés | **148 réussis**, un avertissement existant. Inventaire non destructif, navigation mobile, i18n, aide, compte/équipe, résultat vérifié, prochaine étape, connexions et Activité/Réglages de Claude. |
| Contrôles statiques | Compilation, Ruff F/E9, Bandit medium/high, 88 templates Jinja et `git diff --check` réussis. Avertissements `nosec` préexistants. La suite complète de CI sur le commit publié est consignée dans la PR. |
| [Inventaire courant](UX_SCREEN_INVENTORY_CURRENT.md) | **195 déclarations + 3 routes FastAPI = 198**, rapprochées des 198 routes HTTP réellement chargées localement. **88 templates**, 54 repérés dans les gestionnaires. Extraction statique des formulaires/fenêtres/panneaux ; ne prouve pas leur ouverture. |

Commandes de tests, avec une base temporaire, un `DATABASE_URL` vide et le worker désactivé :

```sh
python -m pytest -q \
  seo-agent-web/tests/test_current_ux_inventory.py \
  seo-agent-web/tests/test_navigation_mobile.py \
  seo-agent-web/tests/test_i18n.py \
  seo-agent-web/tests/test_public_help_navigation.py \
  seo-agent-web/tests/test_account_team_journey.py \
  seo-agent-web/tests/test_first_verified_correction.py \
  seo-agent-web/tests/test_next_step_journey.py \
  seo-agent-web/tests/test_search_result_journey.py \
  seo-agent-web/tests/test_analysis_activity_journey.py \
  seo-agent-web/tests/test_i18n_activite.py \
  seo-agent-web/tests/test_i18n_reglages_analyse.py
python seo-agent-web/ops/ux_inventory.py
```

[Support connecté, français, 360 px](ux-cross-journey/support-connected-fr-360.png) · [Support anglais et accès clavier](ux-cross-journey/support-connected-en-360.png) · [Retour avec nom de site long](ux-cross-journey/billing-long-site-en-360.png) · [Information sans script](ux-cross-journey/notice-nojs-fr-360.png) · [Tarifs publics en production](ux-cross-journey/production-fr-pricing-390.png).

## Couverture et suites

Cette matrice relie les familles d’écrans à leurs bilans livrés et aux contrôles actuels. **L** = navigateur local avec données fictives ; **P** = navigateur de production anonyme ; **C** = code. Les bilans liés gardent leurs preuves et limites propres ; ils ne sont pas remplacés par une prétention de couverture exhaustive de toutes les branches.

| Écrans / étapes | Lot livré et preuve détaillée | Recontrôle de ce lot | Accès / validation encore nécessaire |
| --- | --- | --- | --- |
| Accueil public, Tarifs, comparatif | [15 — tarifs et quotas](UX_PRICING_QUOTAS_LOT15.md) | L, P FR/EN ; navigation commune et quatre largeurs en local | Anonyme ; achat réel distinct. |
| Connexion, inscription, invitation | [10 — accès](UX_AUTH_ENTRY_LOT10.md), [6 — équipe](UX_ACCOUNT_TEAM_LOT6.md) | P login/signup ; tests de compte et droits | Liens d’invitation locaux déjà couverts au lot 6 ; Google d’inscription et emails réels à vérifier. |
| Mot de passe oublié, reset, confirmation et renvoi | [12 — récupération](UX_ACCOUNT_RECOVERY_LOT12.md) | P formulaires/jetons absents ou invalides | Réception SMTP et liens réels, hors de cette recette. |
| Liste des sites, premier site, ajout et suppression | [13 — sites](UX_SITE_MANAGEMENT_LOT13.md), [2 — premier résultat](UX_FIRST_RESULT_LOT2.md) | L liste propriétaire et compte neuf sans site ; tests de prochaine étape | Session requise ; fenêtres, refus et suppressions synthétiques documentés au lot 13. Aucun ajout en production. |
| Bilan, analyse, problèmes, détail, historique | [1 — repères](UX_FOUNDATIONS_LOT1.md), [2 — résultat](UX_FIRST_RESULT_LOT2.md), [7 — activité](UX_ANALYSIS_ACTIVITY_LOT7.md) | L menus, problème, site sans analyse ; tests de résultat/activité | Session et site autorisé ; analyse réelle sur un site témoin encore à prévoir. |
| Choisir → préparer → déclarer → vérifier → suivante | [3 — corrections](UX_CORRECTION_REVIEW_LOT3.md) | L parcours manuel complet, rapport ultérieur simulé | Session et site autorisé ; vérification distante réelle non exécutée. |
| Plateforme, WordPress unitaire/lot, aperçu, application et rétablissement | [3 — corrections](UX_CORRECTION_REVIEW_LOT3.md) | L accueil plateforme ; code et bilans des opérations simulées conservés | Les opérations détaillées ne sont pas rejouées dans ce lot. Recette sur WordPress témoin avec confirmation humaine requise. |
| GitHub unitaire/étendu, fenêtres et automatisation de projet | [3 — corrections](UX_CORRECTION_REVIEW_LOT3.md) | L écran Automatisation sans dépôt ; tests existants en CI | Le P0 de confirmation est traité au lot 3. Préparation, PR et publication réelles non exécutées ici. |
| Google/Bing du site, Performance et mots-clés | [4 — connexions et résultats](UX_SEARCH_RESULTS_LOT4.md) | L écrans sans connexion ; tests fournisseurs simulés | Session, fournisseur et propriété réels nécessaires pour OAuth, sélection et retour. |
| Contenu, validation, concurrents, liens entrants et opportunités | [5 — visibilité](UX_CONTENT_VISIBILITY_LOT5.md) | L écrans du menu Pro sans fournisseur ; C pour la validation de contenu dans ce lot | Génération/validation simulées au lot 5 ; qualité réelle des modèles et publication à vérifier séparément. |
| Réglages d’analyse, Activité et détail de traitement | [7 — activité](UX_ANALYSIS_ACTIVITY_LOT7.md), i18n de Claude #43 | L réglages/activité ; tests FR/EN après intégration de Claude | Session requise ; états détaillés et jobs synthétiques dans le bilan 7, pas tous rejoués ici. |
| Exports, packs, aperçu de fichiers et refus | [11 — exports](UX_EXPORTS_FILES_LOT11.md) | C ; contrôles détaillés antérieurs conservés | Session et droits d’export ; imports dans les outils réels du prestataire non attestés. |
| Mon compte, préférences, connexions, sécurité, suppression | [6 — compte](UX_ACCOUNT_TEAM_LOT6.md) | L consultation FR/EN et fermeture du succès ; aucun formulaire sensible soumis | Session ; OAuth et suppression réels non exécutés. |
| Équipe, rôles, abonnement et compte propriétaire | [6 — compte](UX_ACCOUNT_TEAM_LOT6.md), [15 — quotas](UX_PRICING_QUOTAS_LOT15.md) | L propriétaire/membre, billing avec contexte et nom long ; tests de périmètre | Session ; invitations, portail et paiement Stripe réels non exécutés. |
| Assistant, conversation et erreurs | [14 — assistant](UX_ASSISTANT_LOT14.md) | Widget chargé avec les écrans ; C. Conversation détaillée non rejouée ici | Modèles et erreurs simulés au lot 14 ; qualité/disponibilité réelle à vérifier. |
| Support, statut, documentation et guides | [8 — statut](UX_PUBLIC_STATUS_LOT8.md), [9 — aide](UX_HELP_NAVIGATION_LOT9.md), documentation de Claude #32 | L, P listes ; L chacune des 60 variantes d’articles | Pas d’authentification exigée ; statut « configuré » ne devient pas preuve de disponibilité distante. |
| CGU, confidentialité, mentions légales, versions anglaises | Chantier juridique de Claude #39/#41/#46 | L, P affichage et navigation ; C variantes inventoriées | Aucun texte contractuel modifié ni avis juridique. Le DPA de #44 reste un brouillon documentaire distinct des articles publics. |
| Administration : automatisation, système, exploitation, activation, liste et fiche client | [6 — administration](UX_ACCOUNT_TEAM_LOT6.md) | L les six écrans FR/EN, 1 440/360 px ; aucune sauvegarde | Session admin/propriétaire système selon l’écran. Pas d’accès administrateur de production utilisé. |
| Erreurs, session expirée, déconnexion, confirmations | [6 — compte](UX_ACCOUNT_TEAM_LOT6.md), [14 — assistant](UX_ASSISTANT_LOT14.md) | L retrait de session et fermeture persistante/clavier ; P redirections privées | 403/404/429/500/503 et erreurs API dans les bilans/tests ; aucun incident de production provoqué. |
| Callbacks OAuth, endpoints techniques, Swagger, schéma, cron/webhooks | [Inventaire courant](UX_SCREEN_INVENTORY_CURRENT.md), C | 198 routes HTTP chargées ; pas de parcours Swagger ni de webhook réel | Audience et droits propres à chaque route. Ce ne sont pas des écrans clients à considérer comme validés par leur seul recensement. |

Suites ordonnées, sans élargir cette PR :

1. **P1 — recette authentifiée réelle** : compte témoin, Google/Bing et CMS/dépôt témoins, analyse puis vérification réelle. Elle doit confirmer les permissions, retours OAuth et écritures explicitement validées. Elle n’est pas remplacée par les transports fictifs.
2. **P1 — accès et abonnement réels** : délivrabilité des liens, invitation d’équipe, checkout/portail avec compte Stripe de test. Aucun envoi ni paiement de production nécessaire pour cette recette.
3. **P2 — contexte des articles** : les CTA des guides retournent encore à l’application sans projet explicitement transporté. À améliorer seulement lorsqu’un contexte de site autorisé est réellement connu ; ne pas déduire une cible d’écriture depuis un article public.
4. **P2 — accessibilité et compatibilité humaines** : lecteur d’écran, zoom, Safari/Firefox et parcours tactile. Les contrôles Chromium, clavier et contrastes échantillonnés ne les remplacent pas.
5. **P2 — juridique et i18n** : coordination avec Claude pour les suites documentaires ; aucune publication du DPA ou modification contractuelle par ce lot.

L’inventaire et les bilans couvrent le périmètre examiné et les améliorations livrées. Les validations ci-dessus restent ouvertes ; aucune déclaration d’achèvement global ni de conformité intégrale n’est faite.
