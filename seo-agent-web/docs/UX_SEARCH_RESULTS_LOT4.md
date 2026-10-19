# Lot 4 — Connexions et résultats

Base : main après fusion de la PR #25 (`b7d3c7e`). Audit initial : [UX_AUDIT_2026-10-19.md](UX_AUDIT_2026-10-19.md).

## Bilan avant modification

| Écran | Observation dans le code | Conséquence | Proposition | Priorité |
| --- | --- | --- | --- | --- |
| Performance et API search-items/CSV | Une réponse distante réussie mais vide est remplacée par un ancien CSV sans reprendre ses dates. | Des chiffres anciens peuvent porter la période demandée ; tableau et graphique divergent. | Respecter la réponse reçue ; historique conservé dans les rapports datés. | P0 |
| Performance | Réglages du compte global en sortie d’erreur ; filtres inaccessibles sans données ; zéro affiché comme position. | Perte du projet, changement de source difficile, classement fictif. | Connexions du site avec retour conservé, filtres disponibles, valeurs absentes explicites. | P1 |
| Vue d’ensemble | Requêtes de graphiques même sans autorisation ; état vide confondu avec panne ; changement de période pouvant garder les anciennes courbes. | Grande zone vide, message trompeur et comparaison de périodes faussée. | Cartes compactes, états chargé/vide/erreur/session distincts ; retirer les anciennes valeurs au changement de période. | P1 |
| Mots-clés | Toute erreur devient « non connecté » ; absence de recommandation présentée comme preuve de conversion ; tutoiement. | Mauvais diagnostic et promesse commerciale sans mesure. | Distinguer autorisation/service/vide ; critères et dates explicites ; vouvoiement. | P1 |
| Connexions | Autorisations locales par projet et retours OAuth déjà protégés, à conserver ; réglages avancés et compte mélangés. | Difficulté à retrouver le site concerné. | Écran dédié Google et Bing ; compte autorisé distingué de l’accès effectif aux données. | P1 |

## Parcours et règles communs

Résultats → connexions du site → autoriser Google/Bing → retour aux mêmes résultats (source, période, recherche, page). Autoriser un compte ne prouve pas encore l’accès aux statistiques de ce site : seule la réponse du fournisseur le confirme. Une déconnexion reste limitée au projet et conserve ses analyses enregistrées.

Les filtres restent accessibles dans tous les états. Une réponse reçue sans lignes n’est ni une panne ni une preuve de réussite SEO. Les chiffres, tableaux et graphiques indiquent leur source et leur période. Un changement de période ne conserve pas des courbes anciennes sous un nouveau libellé. Les termes impressions, taux de clics et position moyenne sont expliqués sans promesse de classement ni de conversion.

## Validation

Les preuves ci-dessous distinguent les contrôles serveur, le navigateur et les accès restant à vérifier. Les comptes des essais sont fictifs. Les vraies autorisations Google/Bing et les données de production nécessitent un accès authentifié et ne sont pas vérifiées par ces simulations.

## Changements livrés

- **Connexions du projet** : nouvelle route `/projects/{slug}/settings/search`, entrée « Google et Bing » dans les réglages du site, actions partagées avec les badges existants. Autorisation, activation et disponibilité des données restent des notions distinctes. Liens depuis le compte vers chaque projet. Retour OAuth à cet écran, puis aux résultats d’origine, avec source, période, dimension et recherche conservées.
- **Performance / API / CSV** : une réponse vide ne reçoit plus les lignes d’un ancien CSV sous de nouvelles dates. Filtres disponibles même en erreur, période non standard conservée, recherche effaçable, position absente affichée « — ». Synthèse des lignes reçues séparée du filtre du tableau ; export nommé « CSV complet ». L’actualisation des statistiques remplace l’invitation à lancer une nouvelle analyse sur cet écran.
- **Vue d’ensemble** : aucune requête de statistiques sans autorisation effective et activation. Les rapports historiques restent consultables avec leur propre période. Au changement de période, les anciennes valeurs disparaissent avant la réponse ; une panne ou un redimensionnement ne réaffiche pas une ancienne courbe. Les liens « Voir tout » reprennent la période choisie.
- **Graphiques et petits tableaux** : succès vide, un seul jour reçu, panne, session expirée et données enregistrées distingués. Reprise vers la connexion du site ou reconnexion avec retour. Les messages d’erreur résistent au redimensionnement et au choix d’un indicateur.
- **Mots-clés** : états de connexion/panne/vide distincts, période modifiable, dates du fournisseur lorsqu’elles sont disponibles, vouvoiement, lien vers les résultats. L’absence de piste n’est plus présentée comme une preuve de conversion. Les comparaisons de clics restent indicatives.
- **Règles visuelles** : deux cartes de connexion sur ordinateur, une colonne sur mobile ; boutons et champs qui reviennent à la ligne ; titres de colonnes numériques lisibles ; tableaux défilables au clavier ; explication commune des indicateurs.

## Preuves recueillies

| Vérification | Réalisée | Limite |
| --- | --- | --- |
| Chromium local, vue d’ensemble / performance / mots-clés / connexions | 64 affichages : quatre écrans × quatre états × quatre largeurs (1 440, 768, 390, 360 px). Aucun débordement du document ni erreur JavaScript. | Comptes fictifs et réponses Google/Bing simulées dans le serveur local. |
| Interactions Chromium | Retour vers les résultats Bing filtrés ; échec au changement de période puis redimensionnement ; succès avec un seul jour ; session expirée et lien de reprise ; aide des indicateurs et navigation clavier élémentaire. | Le 401 de session est injecté par le navigateur ; les autres réponses passent par les routes HTTP réelles avec transports simulés. Ce n’est pas une recette lecteur d’écran complète. |
| Tests HTTP ciblés | 51 tests passent : nouveaux états, API/CSV vides, retours OAuth et filtres, droits d’accès, séparation des comptes/projets, déconnexion locale Google/Bing existante. | Échanges de jetons et services distants simulés. |
| Vérifications statiques | Ruff F/E9, compilation Python, 76 templates Jinja analysés et Bandit medium/high passent. | La CI complète est indiquée dans la PR. |
| Production authentifiée / OAuth réel | Non exécutés. | Nécessitent un compte et des autorisations réels. Aucun site client n’a été modifié pendant ces essais. |

[Preuves navigateur structurées](ux-search-results/evidence.json), [Performance sur ordinateur](ux-search-results/performance-data-1440.png), [Connexions sur mobile](ux-search-results/connections-data-390.png), [Panne Google dans les mots-clés](ux-search-results/keywords-error-390.png), [Vue d’ensemble sans autorisation](ux-search-results/overview-disconnected-360.png).

Le lot 5 conserve le travail sur la préparation de contenu, les propositions de réécriture depuis les mots-clés, les concurrents et les liens entrants. Le lot 6 conserve la réorganisation complète du compte, des abonnements et de l’administration. Les identifiants de données, les règles de calcul et de facturation, la portée des droits et les confirmations de modification du site ne sont pas redéfinis par ces composants.
