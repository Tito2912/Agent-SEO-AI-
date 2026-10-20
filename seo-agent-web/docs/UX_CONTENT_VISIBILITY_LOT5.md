# Lot 5 — Rédiger, relire et faire connaître son site

Base : main après la PR #26 (`e94169f`). Suite de [l’audit initial](UX_AUDIT_2026-10-19.md).

## Bilan avant modification

| Écran | Preuve initiale | Observation avant modification | Conséquence client | Proposition | Priorité |
| --- | --- | --- | --- | --- | --- |
| Contenu | Code | Deux modes et coût par langue déjà présents ; rédaction possible sans analyse dans un mode qui la demande, options automatiques proches de la saisie manuelle. | Prérequis découvert après le clic ; distinction rédaction/publication difficile. | Garder les deux modes, progression et prochaine étape, prérequis et quota visibles, automatisation regroupée. | P1 |
| Aperçu d’article | Navigateur local | L’iframe reste blanche : la politique globale interdit les cadres. | Le texte généré ne peut pas être relu dans l’aperçu. | Autoriser uniquement le cadre isolé de cet écran, bloquer scripts/ressources dans son document et proposer le Markdown en repli. | P1 |
| Validation de contenu | Code | « Valider et fusionner » et « part en ligne » malgré une attente possible des vérifications du dépôt ; validation déjà donnée peu visible. | Un client peut croire la page publiée, ou valider plusieurs fois faute d’état clair. | Séparer relire, autoriser la fusion, attente des contrôles, fusion et déploiement ; relecture explicite avant validation. | P1 |
| Réécriture de mots-clés | Code | Le clic appelle immédiatement le modèle et crée une proposition GitHub. | Dépense et portée de l’action découvertes trop tard. | Réutiliser le récapitulatif de préparation, avec page/recherche/coût et création distincte de la fusion. | P1 |
| File des liens entrants | Code | « Répondre » copie/ouvre un onglet puis enregistre immédiatement la réponse comme envoyée. | Un simple clic devient une publication fictive dans le suivi, même si la copie échoue. | Copier/ouvrir puis déclaration explicite après envoi ; statut déclaré distinct d’un lien vérifié. | P0 |
| Recherche de liens | Code | Jargon, tutoiement, champs sans nom accessible, compteurs divisés par 200 sans reprendre la limite réelle. | Mauvaise compréhension de l’action et de la consommation disponible. | « Faire connaître mon site », étapes chercher/relire/envoyer, libellés liés et solde serveur sans dénominateur inventé. | P1 |
| Liens vers/dans le site | Code | Sources et liens internes réunis sous « Backlinks & netlinking » ; connexion globale en sortie. | Confusion sur les données importées et les liens de ses propres pages. | Groupes explicites, origine/période, retour aux connexions du site. | P2 |
| Concurrents | Code | Parcours déjà utile ; sujets parfois présentés comme absents de tout le site malgré un périmètre d’analyse limité. | Une piste éditoriale peut être prise pour un constat exhaustif. | Garder sélection et coûts existants ; préciser le périmètre observé et le passage vers Contenu. | P2 |

## Parcours cible et règles communes

Choisir un sujet ou une recherche → vérifier les prérequis et le coût → préparer un brouillon → relire → autoriser la proposition ou sa fusion selon l’écran → attendre les vérifications → publier selon la plateforme → vérifier le résultat. Les modes manuels et automatiques restent distincts, avec leurs règles existantes.

Pour un message sur un autre site : trouver une discussion → préparer et relire la réponse → copier et ouvrir le site → envoyer soi-même → déclarer l’envoi. La copie, l’ouverture d’un onglet et une déclaration ne prouvent pas la présence d’un lien.

Les composants utilisent les espacements, cartes, contrôles, dialogues et progressions déjà établis. Aucun état visuel ne remplace une preuve serveur ni ne recalcule les droits ou la facturation.

## Vérifications et limites

Les preuves ci-dessous distinguent les contrôles navigateur, les tests serveur et les accès restant à vérifier. Les contenus et comptes des essais sont fictifs ; les transports GitHub/modèles/recherche sont simulés. La publication réelle, l’envoi de messages et les comptes de production authentifiés restent hors de ces essais.


## Changements livrés

- **Contenu** : progression Choisir → Rédiger → Relire → Publier ; destination, coût par version et solde à l’ouverture conservés. Analyse absente, dépôt indisponible et quota insuffisant donnent une action de reprise. Les choix restent verrouillés pendant une rédaction. Un brouillon réussi invite à sa relecture ; une réponse perdue invite à actualiser le journal avant de réessayer. Un changement de choix permet de préparer un autre brouillon. Les réglages automatiques sont regroupés dans un panneau dédié.
- **Aperçu** : le texte HTML est réellement visible dans Chromium. Seul l’écran Contenu autorise un cadre de même origine ; l’iframe garde un sandbox sans permissions et une politique interne `default-src 'none'`. Le texte en Markdown reste lisible sans cadre. Une sonde de navigateur confirme le blocage d’un script et d’une image distante. La politique personnalisée de l’exploitant reste prioritaire.
- **Validation** : case de relecture obligatoire dans l’interface, bouton « Autoriser la fusion », état « Contenu validé · fusion en attente » et état fusionné distinct d’une mise en ligne. Le mécanisme serveur de validation et de contrôle avant fusion est conservé ; l’écran GET ne déclenche pas de fusion.
- **Mots-clés et Concurrents** : le même dialogue de préparation présente page, recherche ou sujet, dépôt et consommation avant la demande. Double-clic protégé, relecture GitHub distincte de la fusion, session/quota/proposition déjà ouverte/réponse perdue traités par le composant commun. Les sujets non couverts peuvent ouvrir Contenu sans dépôt ; les sujets déjà couverts proposent une relecture de la page ou une proposition GitHub.
- **Faire connaître mon site** : recherche → relecture → suivi, noms accessibles des champs, solde renvoyé par le serveur sans limite « /200 » inventée, boutons utilisables sur mobile. Copier/ouvrir n’enregistre jamais un envoi. Une action séparée « J’ai envoyé la réponse » enregistre ensuite la déclaration ; le statut ne prétend pas qu’un lien a été vérifié. Les confirmations de suppression existantes restent présentes.
- **Liens entrants** : distinction entre liens importés vers le site et liens internes/sortants de l’analyse ; connexion Google/Bing du projet, lien vers la recherche de discussions. Les anciens envois ne sont pas revérifiés par cette modification : « enregistré » décrit leur statut de suivi.
- **Aide** : quatre pages documentaires alignées avec le mode CMS sans dépôt, le coût par version/fichier, les cadences disponibles, les modes de publication et l’envoi manuel sur les sites tiers. Les montants et quotas des formules restent calculés par le serveur.

## Validation recueillie

| Contrôle | Preuve | Limite |
| --- | --- | --- |
| 77 cas Chromium locaux à 1 440, 768, 390 et 360 px | Contenu Free/Solo/Pro, analyse absente, quota, dépôt prêt/indisponible ; quatre états de validation ; liens/discussions/concurrents ; récapitulatif et succès de réécriture ; reprise, double-clic, focus, copie refusée, déclaration et aperçu. Aucun débordement horizontal ni erreur JavaScript. | Les réponses de rédaction, les rubriques GitHub et les propositions sont interceptées dans le navigateur. Les contrôleurs d’écran utilisent les vraies routes et données fictives locales. |
| Copie/ouverture/déclaration | Aucune requête `mark-sent` ni modification de la base après copie ou copie refusée ; une requête après déclaration explicite, avec état serveur `contacted`. | Vrai serveur et vraie base locale. Aucun message externe envoyé ; l’ouverture du site tiers est simulée. |
| Régression du dialogue de correction | Parcours Automatisation et WordPress du lot 3 repris sur cette branche ; rendu aux quatre largeurs, proposition en attente de publication, lot WordPress mixte. | GitHub simulé ; transport WordPress fictif. |
| Serveur et droits | Tests HTTP existants et nouveaux : quotas, versions linguistiques, modes, validation différée, absence de fusion sur GET, CSRF, membre du compte payeur, accès étranger refusé, portée de la politique des cadres. | Services tiers et modèles simulés ; résultats complets dans la PR. |
| Contrôles statiques | Ruff F/E9, compilation Python, 76 templates Jinja et Bandit medium/high. | La CI ajoute la suite complète et l’audit des dépendances. |
| Production authentifiée | Non parcourue dans ce lot. | Aucune publication réelle, fusion de proposition client, invitation, paiement ou envoi externe pendant les essais. |

[Preuves navigateur](ux-content-visibility/evidence.json), [Contenu sur ordinateur](ux-content-visibility/content-github-1440.png), [Relecture sur ordinateur](ux-content-visibility/review-initial-1440.png), [Analyse manquante](ux-content-visibility/content-no-crawl-390.png), [Article sur mobile](ux-content-visibility/article-ready-390.png), [Attente après validation](ux-content-visibility/review-approved-390.png), [Récapitulatif de réécriture](ux-content-visibility/keyword-recap-390.png), [Suivi des discussions](ux-content-visibility/outreach-390.png).

Un test de rafraîchissement des concurrents contrôlait un domaine dans un échantillon de réponse limité à vingt entrées. Il contrôle maintenant le travail réellement créé pour le concurrent attendu, ce qui reste valable quand d’autres tests ont rempli la base.

Le lot 6 reste consacré au compte, à l’équipe, aux abonnements partagés et à l’administration, dont les limites figurent dans l’audit initial. La recette avec de vrais comptes et une technologie d’assistance reste distincte des simulations présentées ici.
