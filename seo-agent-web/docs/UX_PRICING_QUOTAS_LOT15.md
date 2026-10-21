# Lot 15 — Comprendre les tarifs et suivre ses quotas

## Audit avant modification

Base : `044e7aa`, après fusion de l’assistant #42. Branche et worktree isolés. Claude travaille sur les pages juridiques (#41) et les traductions Activité/Réglages (#43) ; ces écrans, son moteur de langues et les layouts communs ne sont pas modifiés. Les nouveaux textes suivent `docs/I18N.md` : français source, `tr()` et ajout de leurs traductions anglaises uniquement.

**L — navigateur local** : 12 affichages Chromium, FR/EN, 1440/360 px, tarifs publics, abonnement Pro et membre venant d’un site partagé. Comptes et usages synthétiques ; Stripe et IA désactivés, aucun appel externe. **C — code** : catalogue, comparatif, porte de plan, consommation des corrections WordPress/GitHub, articles, quotas et période. Aucun abonnement de production authentifié ni paiement réel examiné.

| Écran / état | Preuve | Problème et conséquence | Amélioration proposée | Priorité |
| --- | --- | --- | --- | --- |
| Tarifs : définition d’une correction | L, C | FAQ limitée au fichier GitHub ; WordPress décompte un titre ou une méta-description dès sa préparation. Le client ne peut pas anticiper sa consommation. | Définition commune pour fichier et texte WordPress, moment de préparation explicite ; distinguer rédaction manuelle et application après relecture. | P1 |
| Accueil : note des tarifs | C | Même définition limitée au fichier. | Réutiliser la définition commune et conserver les cartes résumées déjà livrées. | P1 |
| Abonnement : consommation | L, C | Quatre compteurs affichés ; articles, recherches et brouillons de liens entrants absents. Une consommation peut survenir sans compteur consultable. | Montrer les six usages mensuels, avec consommé, plafond, solde et état textuel. Garder le nombre de sites distinct du mois. | P1 |
| Abonnement : limite atteinte / fonction fermée / plafond absent | C | Un tiret peut signifier plusieurs choses ; barres sans nom ni valeur accessibles. | Distinguer accès du plan, solde épuisé, absence de plafond et administration ; conserver la valeur réelle si elle dépasse le plafond et borner seulement la barre. | P1 |
| Calendrier des quotas et guide FR/EN | C | `usage_add` et `usage_sum` utilisent `YYYY-MM` UTC ; la FAQ documentaire annonce la période de facturation. | Indiquer le mois lu et la remise à zéro au premier du mois UTC, séparément du renouvellement de l’abonnement. | P1 |
| Comprendre avant de choisir | L, C | Définition en bas de la longue grille ; différence entre correction, article et analyse peu visible. | Raccourcis de section et explications communes aux tarifs/à l’abonnement, sans prix ni quantité copiés à la main. | P2 |
| Mobile et clavier | L, C | Listes « non inclus » peu contrastées, comparatif horizontal déjà nommé et défilant. | Garder le défilement, renforcer les textes secondaires concernés, nommer les jauges et prévoir des liens confortables. | P2 |
| Tarifs publics, personne connectée à 360 px | L complémentaire, C | Le groupe langue / tableau de bord / déconnexion dépasse la largeur disponible. Les cartes d’abonnement configuré débordent aussi en anglais. | Retour à la ligne du groupe sur les tarifs et des boutons/prix des cartes, sans modification de layout commun. | P1 |

## Parcours cible et règles

Tarifs → comprendre ce qui est compté → comparer les fonctions → créer son compte ou gérer sa formule. Abonnement → identifier le compte payeur et le site d’origine → lire le mois et ses six compteurs → comprendre le décompte → consulter les formules ou revenir au site. Un achat reste celui du compte connecté ; les garde-fous de site partagé et le retour contextualisé du lot 6 sont conservés.

Les compteurs viennent des mêmes événements et limites que les contrôles serveur. Aucun prix, quota, droit, paiement ou décompte n’est modifié. Une jauge ne prouve ni publication ni correction vérifiée. La préparation d’un texte WordPress peut consommer une unité avant son enregistrement ; l’enregistrement reste soumis à sa confirmation existante.

Les composants communs portent la définition, le guide et la présentation des usages. Texte visible pour chaque état, information non portée par la couleur seule, cibles de 44 px, tableau défilant conservé. Les nouvelles traductions sont ajoutées sans réécrire celles de Claude ; leur combinaison sera vérifiée avant publication.

## Livraison et validation

Le [commit applicatif `ae1eda5`](https://github.com/Tito2912/Agent-SEO-AI-/commit/ae1eda5491c996439c5e7d52a7db84d6b99e6289) regroupe le changement : un module de présentation, un partiel à macros commun et une feuille de styles limitée aux blocs concernés. Le nouveau partiel porte le total à **88 templates** ; aucune nouvelle route.

- La même définition est utilisée dans la note des tarifs de l’accueil, la FAQ et le guide. Elle distingue fichier GitHub et texte WordPress préparé, avec enregistrement après relecture. Articles et analyses ont leur propre explication. L’aide FR/EN reprend le mois civil UTC, le décompte dès la préparation, l’accès Solo du fix-pack et la sélection réelle des premières pages HTML valides pour PageSpeed.
- L’abonnement lit les six métriques existantes avec `usage_sum`, pour le même compte et un même mois fixé avant les lectures. Aucun événement de consommation n’est créé par cette consultation. Solde, nom et valeur de jauge, texte de limite atteinte et accès aux formules sont visibles ; la capacité de sites est séparée du mois.
- Le module distingue porte de plan, moteur de correction fermé, quota fini, plafond mensuel absent et administration. Un plafond nul ne ferme pas une fonction dont la porte est ouverte : cette distinction est aussi reprise dans les cartes, le comparatif et les valeurs interpolées de la documentation. Elle n’ouvre aucun droit côté serveur. Les valeurs consommées ne sont pas ramenées au plafond ; seule la barre l’est.
- Les confirmations, le début immédiat, la résiliation, les changements différés, la réconciliation Stripe et les factures gardent leurs mécanismes existants. Compte personnel, propriétaire du site partagé et retour au site restent distincts. Les liens nouveaux conduisent au guide dans la langue de l’écran et aux sections réelles.
- Les textes « Non inclus » deviennent lisibles visuellement ; boutons et prix des cartes se replient sur plusieurs lignes. L’en-tête des tarifs connectés s’adapte à 360 px avec une règle de cette page, sans modifier le layout partagé. Les cartes résumées de l’accueil et le comparatif défilant sont conservés.

**Preuves locales** :

- [Référence initiale](ux-pricing/baseline.json) : 12 affichages du `main` initial archivé, langue du navigateur fixée et vérifiée dans le HTML. [Abonnement avant sur mobile](ux-pricing/before-pro-360.png). Le débordement de navigation authentifiée a été identifié dans une observation complémentaire avant sa correction.
- [Recette Chromium finale](ux-pricing/browser.json) : **148 cas FR/EN**, 1440/768/390/360 px. Tarifs anonymes et connectés, accueil résumé, six compteurs, membre d’un site partagé, administrateur, quota dépassé, fonction exclue, plafond absent, paiement configuré simulé, liens d’ancre et d’aide, ouverture des explications au clavier, comparatif défilant. Quatre cas vérifient les nouveaux blocs sans JavaScript. Aucun débordement horizontal du document ni erreur JavaScript observé ; aucune requête externe ni requête de mutation. Les formulaires de paiement sont affichés, jamais soumis.
- [Contrastes](ux-pricing/contrast.json) : **6 580 relevés**, **164 combinaisons texte/langue/contraste distinctes**, minimum **7,65:1** sur les éléments échantillonnés des nouveaux blocs et des indications « Non inclus ». Les nouveaux liens et résumés contrôlés ont au moins 44 px de hauteur. Cela ne certifie pas l’accessibilité complète de l’application.
- **176 tests ciblés distincts réussis** : 92 pour quotas, langues, compte payeur, abonnement et tarifs ; 68 pour documentation, WordPress et quota d’articles ; 16 pour la grille complète des cartes. Ces derniers ont été exécutés avec les 13 nouveaux cas de quotas (29 réussis dans cette sélection complémentaire). Le test historique de non-inclusion contrôle désormais le sens des cartes et leur libellé, sans imposer le masquage visuel du texte. Le code applicatif reste celui de `ae1eda5`. Les **13 nouveaux cas** vérifient notamment compte/mois/remboursement, séparation des portes et plafonds, valeurs dépassées et jauges nommées, guide localisé, absence de décompte lors de lectures et reprise de connexion.
- Compilation Python, Ruff `F,E9`, analyse des **88 templates Jinja**, Bandit medium/high et `git diff --check` réussis. Le résultat de la CI complète sur le dernier commit publié sera indiqué dans la PR.

**Coordination avec Claude** : la [combinaison temporaire](ux-pricing/combination.json) de ce code avec #41 (`f0d255c`) et #43 (`b76c683`) se construit sans conflit. **176 tests ciblés y réussissent**, incluant ses nouveaux tests Activité/Réglages et les mentions légales. Le catalogue combiné ne contient aucun doublon et conserve toutes les traductions existantes. Seules **19 nouvelles entrées** ont été ajoutées au catalogue de notre branche, sans réécrire ses traductions ; aucune modification de `docs/I18N.md`, des pages juridiques, des articles, de l’assistant, du moteur de langues ni des layouts communs. Aucune de ses branches n’est modifiée ou fusionnée par cette vérification.

Captures après : [tarifs sur ordinateur](ux-pricing/pricing-fr-1440.png), [compteurs sur ordinateur](ux-pricing/usage-pro-fr-1440.png), [compteurs sur mobile](ux-pricing/usage-pro-fr-360.png), [guide français](ux-pricing/guide-fr-360.png), [guide anglais](ux-pricing/guide-en-360.png). Les captures de blocs isolés utilisent une fenêtre plus haute pour garder le lanceur flottant hors du bloc capturé ; les interactions sont vérifiées à 900 px de hauteur.

## Limites

Les recettes utilisent une base locale et des services simulés. Elles n’attestent aucun paiement, changement d’abonnement, facture distante, réception d’email ou production authentifiée. La qualité des connecteurs et modèles n’est pas évaluée. Une recette humaine avec lecteur d’écran et Safari/iOS reste à faire.
