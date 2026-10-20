# Lot 8 — État public du service

Bilan du 20 octobre 2026, sur `main` `83af462` : PR #29 fusionnée et premiers lots d’internationalisation de Claude intégrés par la PR #31. C = code ; N = navigateur. Les preuves après réalisation sont ajoutées séparément.

Validation actualisée après la fusion de la PR #32 dans `main` `076046e` : intégration sans conflit dans notre branche, **112 tests ciblés réussis** et **24 états navigateur revérifiés**. Les deux écarts documentaires initiaux décrits ci-dessous sont résolus ; ils ne bloquent plus ce lot. La CI complète du commit livré est consignée dans la PR #33.

## Coordination avec le chantier de Claude

Après `claude/noyaru-internationalization-ju7hnc` (PR #31 intégrée), le travail publié de Claude dans `claude/noyaru-doc-a-jour` a été fusionné par la PR #32. Ce lot utilise un worktree et une branche propres (`improve/noyaru-etat-des-services`). Son périmètre est limité au gestionnaire `/status`, au template `status_public.html`, à un module et une feuille de style propres à cette page, à ses tests et au présent bilan. Aucun changement du moteur `backend/i18n.py`, des catalogues `locales/`, des langues, des layouts communs ou des écrans clients. Les textes de cette page réutilisent les traductions existantes, sans nouveau catalogue ni mécanisme parallèle. Vérifier les dernières références distantes et le chevauchement de fichiers avant livraison ; une branche séparée ne garantit pas à elle seule l’absence de conflit.

La combinaison de notre commit `3f4d607` avec le commit publié `560fb1d` de la PR #32 a été simulée par `git merge-tree --write-tree`, sans mise à jour de branche ni fusion effective. Aucun conflit ; le seul fichier existant commun est `backend/app.py`, dans des fonctions différentes (`status_public` et consignes de l’assistant). Une archive de l’arbre combiné a ensuite passé les **42 tests** de `test_public_service_status.py` et `test_i18n.py`. [Preuve de compatibilité](ux-public-status/coordination.json). Cette vérification couvre les commits publiés, pas les modifications non publiées de Claude.

Après la fusion de la PR #32 par le propriétaire, notre branche a intégré `main` `076046e` dans `45a6841`. Les fichiers de travail de Claude et sa branche n’ont pas été modifiés. Les 112 tests de statut, SEO public et internationalisation passent sur cette intégration ; le diff avec `main` reste limité au périmètre de ce lot. La PR #33 n’a pas été fusionnée ni déployée par l’agent.

## Constats avant modification

| Écran / état | Preuve initiale | Problème et conséquence | Proposition | Priorité |
| --- | --- | --- | --- | --- |
| `/status` et `/en/status`, configuration de services externes présente | C + N, français/anglais à 1440 et 360 px | SMTP, stockage objet et paiement sont annoncés « opérationnels » à partir de leur seule configuration. Le client peut écarter à tort une panne. | Afficher les contrôles réellement observés séparément des connexions sans mesure de disponibilité ; ne pas inventer de sonde ni de pourcentage de disponibilité. | P1 |
| Page et métadonnées | C | « En direct » et « best-effort » suggèrent une supervision continue alors que la page est un instantané rendu à la demande. | Intitulé factuel, date de lecture et actualisation explicite en GET ; ne pas présenter un état mis en cache comme nouveau. | P1 |
| Base de données en erreur | C | Le type et les 180 premiers caractères de l’exception sont publics. Une erreur peut contenir des informations internes inutiles au client. | État d’échec simple ; ne pas exposer la chaîne technique. | P1 |
| Suite du parcours | C | Aucune action locale pour actualiser ou obtenir de l’aide. | Actualiser, obtenir de l’aide et retrouver l’accueil, avec la langue conservée. | P2 |
| Lecture et mobile | C | Lignes avec styles locaux répétés, petits libellés et statuts serrés. | Mise en page propre à cet écran, texte lisible, noms/statuts associés et repli mobile ; focus visible. | P2 |

## Parcours et règles

Un visiteur ouvre l’état du service → distingue le résultat de lecture actuel des connexions non vérifiables → actualise explicitement ou ouvre l’aide dans sa langue. La page est publique et ne remplace pas le suivi d’une analyse ou d’une correction. Aucune action de paiement, email ou stockage distant n’est déclenchée par sa consultation.

Conserver les couleurs, cartes et boutons Noyaru. Statut accompagné d’un texte, aucune conclusion globale « tout fonctionne », date explicite, contrôles natifs de navigation, cibles de 44 px, contexte français/anglais conservé. Le layout et le sélecteur de langue de Claude restent inchangés.

## Réalisation et vérification

La page distingue désormais le résultat de la lecture locale actuelle des connexions sans mesure. Le paiement, les emails et le stockage objet restent « Non vérifiable » ; leur configuration n’est plus consultée pour déduire une disponibilité. « Aucune donnée disponible » décrit ces trois mesures manquantes. Le libellé existant est repris dans les deux langues ; aucune traduction parallèle n’est ajoutée.

Le contrôle de la base reste une simple lecture `SELECT 1`, avec fermeture de la session, sans test d’écriture ni promesse concernant toutes les tables ou tous les traitements. Son échec produit un état visible et aucune chaîne d’exception publique. L’état de l’application atteste uniquement que cette page a été servie. La date UTC correspond à cette lecture ; la réponse est `Cache-Control: no-store`. L’utilisateur actualise par un lien GET, ouvre l’aide ou retrouve l’accueil dans sa langue. Aucune mise à jour automatique ni nouvelle API.

Les statuts ont un texte, les champs de description sont associés avec `dl`/`dt`/`dd`, les titres structurent les deux groupes, le focus est visible et les liens du composant mesurent au moins 44 px. La feuille de style est limitée à cette page ; aucun changement des éléments communs de Claude. Canonical, Open Graph et liens `hreflang` sont conservés avec des métadonnées factuelles.

| Périmètre | Vérifié | Limite |
| --- | --- | --- |
| État initial | 4 affichages locaux : français/anglais, 1440/360 px, cinq badges « opérationnel » avec configuration externe simulée. | Aucun fournisseur réel. |
| État corrigé, lecture échouée et récupération | 24 affichages Chromium : 2 langues × 4 largeurs (1440/768/390/360) × 3 états ; navigation vers l’aide dans la bonne langue. | Serveur et SQLite locaux réels ; seule la lecture en échec est injectée, avec une exception contenant une valeur privée fictive. |
| Clavier et mise en page | Focus visible, liens de 44 px, aucun débordement de page ni incident JavaScript dans les cas enregistrés. | Le menu public commun, son défilement horizontal local et le sélecteur de langue de Claude restent inchangés. Recette humaine avec lecteur d’écran non faite. |
| Contraste du composant | 360 mesures calculées, minimum 6,84:1 dans la recette après la fusion de la PR #32. | Échantillons des textes propres à cette page ; composition RGBA des fonds dans Chromium, sans certification globale. |
| Backend | 9 nouveaux tests réussis : lecture, fermeture/échecs de session, français/anglais, état indisponible, absence de déduction depuis une configuration, cache, métadonnées, GET/HEAD et refus de POST avec/sans CSRF. | L’échec de la lecture est simulé ; aucune panne de production provoquée. |
| Tests ciblés, dont ceux de Claude | 112 réussis après l’intégration de la PR #32, avec les contrôles de toutes les pages du sitemap. | Les deux cas documentaires initiaux passent sans modification de leurs tests ; aucune nouvelle traduction dans notre lot. |
| Métadonnées et cartes de partage | 43 tests réussis sur le statut, l’internationalisation et toutes les pages du sitemap ; description suffisamment longue dans les deux langues, sans promesse de supervision continue. | Après correction d’une description trop courte détectée par la suite complète ; contrôles existants conservés. |
| Contrôles de dépôt | Compilation Python et 79 templates Jinja valides, Ruff et Bandit medium/high réussis. | Résultat de la suite complète et de la CI consigné dans la PR. |

Preuves : [avant](ux-public-status/baseline.json), [recette navigateur](ux-public-status/browser-evidence.json), [contraste](ux-public-status/contrast-evidence.json). Captures : [avant anglais mobile](ux-public-status/before-en-360.png), [français ordinateur](ux-public-status/status-fr-1440.png), [anglais ordinateur](ux-public-status/status-en-1440.png), [français mobile](ux-public-status/status-fr-360.png), [anglais mobile](ux-public-status/status-en-360.png), [lecture en échec français](ux-public-status/status-failed-fr-390.png), [lecture en échec anglais](ux-public-status/status-failed-en-390.png).

## Écarts initiaux résolus par la PR #32

Deux échecs sont reproduits sur une archive intacte de `origin/main` `83af462`, indépendamment de ce lot :

- `tests/test_i18n_docs.py::test_a_translation_is_the_same_page[connecter-github]` : français daté du 20 octobre, anglais du 3 septembre ; 7 titres H2 côté français, 6 côté anglais.
- `tests/test_i18n_docs.py::test_a_translation_is_the_same_page[depannage]` : mêmes dates différentes ; 10 titres H2 côté français, 9 côté anglais.

La PR #32 a repris le contenu actuel dans `content/docs/en/connecter-github.md` et `content/docs/en/depannage.md`, avec leurs sections et dates. Les deux tests sont maintenant verts. La [reproduction sur l’ancien main](ux-public-status/main-i18n-reproduction.txt) est conservée comme preuve du constat initial ; elle ne décrit pas l’état courant. Aucun changement de ces fichiers ni affaiblissement de leurs tests par notre lot. Les autres finitions publiques et la production authentifiée restent ouvertes dans l’audit global.
