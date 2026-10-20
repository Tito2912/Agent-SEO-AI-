# Lot 9 — Trouver de l’aide et choisir son guide

Bilan du 20 octobre 2026 avant modification, sur `main` `5c574a2` (PR #33 fusionnée), complété par la réalisation ci-dessous. C = examen du code ; N = navigateur Chromium local. Les deux écrans sont publics et ne nécessitent pas de session. Les preuves ne constituent pas une recette de production authentifiée.

## Coordination

Worktree et branche propres : `noyaru-help-navigation`, `improve/noyaru-aide-et-documentation`. Périmètre : Support et accueil de la documentation, leurs seuls contextes dans `backend/app.py`, composants statiques propres à ces écrans, tests et bilan UX. Aucun changement des pages juridiques, des articles Markdown français/anglais, du moteur et des catalogues d’internationalisation, ni des layouts communs. Les traductions existantes et les métadonnées localisées des guides sont réutilisées. Le travail de Claude non publié reste invisible ; relire les PR publiées avant livraison.

## Constats avant modification

| Écran / état | Preuve | Problème et conséquence | Proposition | Priorité |
| --- | --- | --- | --- | --- |
| `/support`, `/en/support` | C + N | L’utilisateur doit composer un email sans orientation vers la récupération d’accès, le dépannage ou les quotas. | Entrées directes selon le besoin, contact toujours visible, accès à la documentation. | P2 |
| Support → statut | C + N | Badge « live » non traduit et lien `/status` inclus dans un texte HTML traduit ; le lien anglais perd son adresse localisée et suggère une supervision continue. | Lien explicite localisé vers l’état du service ; retirer cette promesse. | P1 |
| `/docs`, `/en/docs` — début du parcours | C + N | « Lancer un crawl » puis « Corriger en pull request » dominent. WordPress et la correction manuelle sont peu visibles pour un débutant. | Comprendre le bilan et vérifier une correction ; trois méthodes de correction présentées sans connexion présumée. | P1 |
| Catalogue des guides | C + N | Tous les articles sont présentés à la suite, sans recherche ni accès direct aux rubriques. | Recherche locale dans les cartes, sommaire et compteur ; état sans résultat avec remise à zéro. | P2 |
| Lecture et clavier | C + N | Aides et métadonnées petites, cartes denses et petites cibles de titre. | Styles communs limités aux deux pages, texte lisible, cibles de 44 px, focus visible et mises à jour annoncées. | P2 |

## Parcours et règles

Besoin d’aide → accès au compte / guide adapté / état du service / contact. Documentation → premier site / compréhension du bilan / vérification → méthode WordPress, manuelle ou GitHub → article existant dans la bonne langue. La consultation d’un guide ne lance aucune analyse ni correction et ne suppose pas un projet sélectionné.

Conserver les métadonnées SEO, les quotas et les articles de référence. Utiliser le résolveur de langue existant : une page sans traduction garde son adresse française. Recherche uniquement dans les cartes chargées ; aucun envoi du texte recherché, stockage local ou nouvelle API. Sans JavaScript, tous les guides et liens essentiels restent disponibles.

## Réalisation et validation

Support propose maintenant la récupération d’accès, trois guides ciblés (dépannage, contrôle d’une correction, quotas), le contact par email et l’état du service. L’ancre Contact reste visible dès le début, même quand les guides précèdent son bloc. Les liens vers le statut et la documentation utilisent les résolveurs de langue existants. Aucune promesse « live », aucun envoi d’email automatique ni formulaire de données personnelles ajouté.

L’accueil de la documentation présente l’ajout du site, la compréhension du bilan et la vérification d’un changement. WordPress/Yoast, fix-pack manuel et GitHub ont chacun une entrée propre ; aucun accès à un dépôt n’est supposé. Les titres, descriptions et valeurs des guides proviennent des articles existants, résolus avec leurs jetons et leur langue. Les articles non traduits, dont le guide d’analyse à cette date, gardent leur adresse française.

La recherche filtre le texte des cartes présentes (titres, descriptions et métadonnées), sans requête réseau ni persistance ajoutée. Elle ignore les accents et la casse, combine les mots, met à jour les compteurs et les rubriques, annonce le résultat et propose une remise à zéro lorsqu’aucun guide ne correspond. Réinitialiser ou Échap ramène le focus dans la recherche. Sans JavaScript, la recherche reste cachée et tous les guides et liens essentiels sont rendus par le serveur. Le layout commun, dont le bandeau de cookies, reste inchangé ; sa recette sans JavaScript n’est pas couverte intégralement par ce lot.

| Périmètre | Vérification réelle | Limite |
| --- | --- | --- |
| État initial | 8 affichages : 2 pages × 2 langues × 2 largeurs (1440/360). | Serveur et base locaux ; aucun compte réel. |
| Rendu et parcours corrigés | 82 situations Chromium, dont Support → Contact/statut, documentation → WordPress → retour, recherche multi-mots et sans accents, zéro résultat, remise à zéro, Échap, ancres, lien de contact long et contenus sans JavaScript. | Les parcours principaux sont exercés à 1440/768/390/360 px dans les deux langues. Les cas sans JavaScript sont à 1440/360 ; le contact long à 360. |
| Navigation et accessibilité | Tous les cas enregistrés sans débordement de page ni incident JavaScript ; focus visible, cibles des liens/boutons propres au composant d’au moins 44 px, compteur annoncé et aides liées. | Aucun lecteur d’écran humain ; menu, langues et bandeau communs inchangés. |
| Contraste | 4 084 mesures calculées de textes et contrôles visibles, minimum 7,08:1. | Composition RGBA dans Chromium, échantillons des nouveaux composants ; pas une certification globale. |
| Backend et non-régression | 122 tests ciblés réussis, dont 3 nouveaux cas vérifiant les liens réels, langues, repli français, jetons et absence de propagation de paramètres privés. Les tests existants couvrent articles, quotas, traductions, métadonnées et sitemap. | Résultat complet de la CI du commit livré consigné dans la PR. Aucun test existant affaibli. |
| Contrôles statiques | Ruff, compilation Python et 79 templates Jinja, Bandit medium/high réussis. | L’audit de dépendances est exécuté par la CI. |

Preuves : [avant](ux-help-navigation/baseline.json), [recette navigateur](ux-help-navigation/browser-evidence.json), [contraste](ux-help-navigation/contrast-evidence.json). Captures : [ancien Support anglais mobile](ux-help-navigation/before-support-en-360.png), [ancienne documentation](ux-help-navigation/before-docs-fr-1440.png), [Support ordinateur](ux-help-navigation/support-fr-1440.png), [Support anglais mobile](ux-help-navigation/support-en-360.png), [documentation ordinateur](ux-help-navigation/docs-fr-1440.png), [documentation anglaise mobile](ux-help-navigation/docs-en-360.png), [recherche sans résultat](ux-help-navigation/docs-empty-en-390.png).

Aucune nouvelle route ni aucun nouveau template : les deux écrans réutilisent les routes GET/HEAD existantes. Aucun article, tarif, quota, droit d’accès, catalogue de traduction ou page juridique modifié. Aucun email, paiement, appel fournisseur, changement de site client, fusion de PR ou déploiement pendant les essais. Les finitions juridiques restent dans le chantier de Claude ; la production authentifiée, les OAuth réels et la recette humaine avec lecteur d’écran restent ouverts dans l’audit global.
