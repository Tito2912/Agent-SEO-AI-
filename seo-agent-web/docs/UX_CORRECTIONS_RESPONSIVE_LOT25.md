# Lot 25 — Les actions de Corrections restent dans la page

Date : 2 novembre 2026. Base initiale : `793d507` ; base finale : `0e9c976`, incluant les nouvelles traductions publiées par Claude pendant ce lot.

## Bilan de l'écran

| Écran et état | Problème constaté | Conséquence | Amélioration | Priorité |
| --- | --- | --- | --- | --- |
| Corrections, anomalies candidates, largeur intermédiaire | Le groupe de quatre actions reste sur une ligne au-delà de 640 px, même quand sa largeur dépasse celle de la carte. | Le bouton GitHub sort de la fenêtre ; un défilement horizontal apparaît. | Autoriser le retour à la ligne dans ce groupe à toute largeur et le borner à la largeur disponible. | P2 |

Le défaut a été mesuré sur l'écran authentifié en production, en français, à
1 030 px : largeur du document 1 033 px, bord droit du groupe à 1 033,47 px.
La reproduction locale sur la vraie route confirme la cause, avec un écart plus
important dû aux polices système de Linux : 1 172 px de document à 1 030 px pour
le compte gratuit connecté à GitHub.

## Changement et règles communes

Trois lignes CSS dans `templates/corrections.html` ciblent uniquement les actions
de `.corr-candidates-heading` : `flex-wrap: wrap`, `min-width: 0`, `max-width: 100%`.
Le groupe revient naturellement à la ligne selon l'espace disponible, sans
masquer de contenu. L'ordre visuel et l'ordre du clavier restent ceux du document.

Les liens Anomalies, ZIP et PDF gardent le projet et l'analyse sélectionnée. Le
compte gratuit garde son invitation vers l'abonnement ; Solo garde l'export ZIP.
Le bouton GitHub dépend toujours de la connexion existante. Aucune route, règle
de quota, confirmation ou opération sur un site n'est modifiée.

Les traductions et les captures de documentation restent réservées à Claude.
Ce lot ne modifie aucun texte, catalogue, fichier de langue, image, layout
partagé ou fichier backend. Les nouveaux commits de `main` sur les tests Windows, la vérification des
corrections en lot et les statuts Core Web Vitals/Search Console/Bing sont
présents dans la base.

## Vérifications réellement exécutées

- **Navigateur local Chromium, vraies routes** : comptes, projets, abonnements et
  rapports fictifs ; aucune mutation HTTP ni requête externe autorisée.
- **Avant** : 36 cas, trois configurations (gratuit avec GitHub, Solo avec GitHub,
  gratuit sans GitHub), FR/EN, largeurs 1 440, 1 030, 1 024, 768, 390 et 360 px.
  Six cas débordent : les trois configurations FR à 1 030 et 1 024 px.
- **Après** : 84 cas sur ces trois configurations et deux langues, à 1 440, 1 280,
  1 100, 1 060, 1 030, 1 024, 1 000, 950, 900, 768, 640, 390, 360 et 320 px,
  plus quatre cas sans JavaScript (FR/EN à 1 030 et 360 px) : **88 cas réussis**.
  Aucun débordement, quatre actions présentes et dans la fenêtre, aucune erreur
  JavaScript. L'ordre Tab des quatre actions est vérifié dans les 84 cas avec JS.
  Les liens conservent le projet et le crawl ; l'export ZIP Solo est vérifié.
- Captures de recette locale examinées à 1 030 et 360 px, données fictives ;
  elles restent des preuves de travail locales, pas des captures du produit.
- **Tests existants** : parcours de correction client, première correction vérifiée,
  i18n, traduction complète et inventaire UX : **134 réussis, 2 ignorés**.
- `git diff --check` réussi. La CI complète et la revue automatique sont requises
  avant fusion ; leurs résultats sont consignés dans la PR.

La vérification authentifiée en production ci-dessus décrit le défaut avant
correction. La matrice après correction est locale ; elle ne constitue pas une
recette de tous les écrans ni une vérification humaine au lecteur d'écran.
