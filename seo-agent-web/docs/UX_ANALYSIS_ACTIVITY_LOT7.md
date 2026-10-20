# Lot 7 — Réglages de l’analyse et activité

Bilan du 20 octobre 2026 avant modification, sur `main` `960556c` (PR #28 fusionnée), complété par la réalisation et ses preuves ci-dessous. C = examen du code ; les mentions « audit navigateur initial » renvoient à l’audit précédent. Ces écrans nécessitent une session ; les nouveaux essais utilisent uniquement des comptes et rapports fictifs.

| Écran / état | Preuve initiale | Problème et conséquence | Amélioration | Priorité |
| --- | --- | --- | --- | --- |
| Réglages `/projects/{slug}/settings/crawl` | C et audit navigateur initial | Réglages techniques très visibles ; nombreux champs sans label associé ; interrupteurs de 24 px et focus peu visible. | Pages et périmètre d’abord, options avancées repliées, labels et aides associés, cibles tactiles. | P1 |
| Champs avancés et suivi d’URL | C | `strip_params`, profondeur, délais, longueur/paramètres d’URL, règles robots/nofollow sont ignorés par la route et le normaliseur. Le client croit avoir enregistré un choix sans effet. | Retirer les contrôles non pris en charge ; conserver uniquement les paramètres réellement utilisés par le moteur. Ne pas inventer de capacité. | P1 |
| Réglages d’un site partagé | C | Formulaire accessible au membre mais écriture réservée au propriétaire, puis message « Projet introuvable ». | Lecture seule explicite pour le membre, propriétaire identifié ; même garde serveur et aucun droit élargi. | P1 |
| Modules Google/Bing et GitHub | C | Connecteurs mêlés à l’analyse ; champs techniques, diagnostics anticipés (« cause de l’erreur » sans requête), aide minuscule. | Groupes facultatifs, liens vers les connexions du site, explications factuelles ; mêmes confirmations et modes. | P1 |
| Retrait du dépôt GitHub du site | C, confirmé pendant la recette locale | L’interface annonçait le retrait après un POST avec dépôt vide, alors que l’API refusait la demande. Le client pouvait croire le dépôt détaché. | Action explicite de retrait, confirmation, réponse serveur obligatoire ; conserver la connexion GitHub du compte et les PR. | P1 |
| Activité `/jobs` | C et audit navigateur initial | Nom technique, « Aucun job », slug à la place du nom ; absence de filtres et erreurs de mise à jour silencieuses. | Activité, nom réel, filtres sur les traitements affichés, état vide et reprise après déconnexion/indisponibilité. | P1 |
| Suivi `/jobs/{id}` en attente/en cours | C | Le résultat est présenté dès que le dictionnaire de travail existe, donc avant le rapport ; boutons peu contextualisés. | État dominant, progression annoncée, accès au résultat seulement après preuve de lecture, sortie vers le site. | P1 |
| Suivi terminé / bilan absent / échec / annulation | C | Fin d’exécution confondue avec bilan lisible ; retour et relance peu expliqués. | Distinguer traitement terminé et rapport disponible ; relecture du statut avant nouvelle demande, erreurs persistantes. | P1 |
| Mise à jour de l’activité et du suivi | C | Requêtes par intervalle pouvant se chevaucher, 401 et erreurs ignorés ; statut et actions peuvent se contredire. | Lecture périodique sans chevauchement, arrêt sur session expirée et état final, reprise manuelle en lecture ; aucune relance d’écriture automatique. | P1 |

## Parcours cible

Site → réglages essentiels → sauvegarde explicite → analyse suivante. L’enregistrement ne lance pas d’analyse et ne modifie pas les pages du client. Un collaborateur consulte les choix du propriétaire. Google/Bing enrichissent le bilan ; GitHub sert aux corrections et reste facultatif.

Activité → traitement du bon site et du bon compte → attente / exécution / finalisation → bilan effectivement lisible. Les compteurs de pages ne remplacent pas la disponibilité du rapport. Une relance est une nouvelle demande explicite ; une erreur de connexion invite d’abord à relire l’état. Les détails techniques restent accessibles au second niveau.

## Règles visuelles et limites

Composants Noyaru existants ; une action principale par état, texte d’aide lisible, sections repliables, nom du site et contexte conservés, champs nommés, focus visible et cibles de 44 px. Aucun changement de prix, quota, moteur d’analyse ou droit d’accès. Le statut du service public, les dernières finitions publiques et la recette de production authentifiée restent des points ouverts de l’audit.

## Réalisation

Les réglages commencent par le nombre de pages et les sous-domaines ; exploration, données supplémentaires et identifiant du robot sont repliables. Les plafonds affichés sont ceux de la formule effective. Les contrôles sans effet ont été retirés, sans ajouter de capacité au moteur. Le profil d’analyse, les options non exposées et les autres réglages du site sont conservés à l’enregistrement. Un membre consulte le formulaire désactivé et voit qui peut le modifier ; la règle serveur reste réservée au propriétaire. GitHub conserve ses droits existants, distincts de ceux du formulaire d’analyse.

Les liens `#gsc`, `#bing` et `#github-repo` ouvrent leur section. Les listes de propriétés et dépôts annoncent leur chargement et leur erreur. GitHub exige le consentement avant le passage en Full Access ; une réponse perdue bloque les nouvelles écritures et propose une relecture de l’état. Le retrait du dépôt est désormais réel et ne révoque aucun jeton du compte.

Le menu affiche « Activité ». La liste montre les vrais noms des sites et filtre les traitements déjà chargés par site, type et état. Le périmètre récent/encore actif est annoncé ; « Recharger la liste » récupère les nouvelles demandes. Les compteurs suivent les filtres et le retour depuis un traitement les conserve. Les tableaux deviennent des cartes sur mobile.

Le suivi distingue attente, exécution, finalisation, rapport disponible, échec et annulation. Un compteur arrivé au total n’annonce pas un bilan disponible. Le bouton de résultat exige un travail terminé et le rapport exact lisible dans les données du propriétaire. Aucun ancien rapport ne remplace silencieusement celui demandé et aucune URL stockée dans le travail ne détermine le lien. Si deux comptes accessibles possèdent le même slug et que la route serait ambiguë, le lien vers le site est omis ; une future sélection explicite du compte reste à étudier.

Les lectures de suivi sont séquencées, limitées à quatre par lot, avec délai maximal et reprise manuelle après erreur. La session expirée arrête le suivi et propose une reconnexion ; la perte d’accès est visible et cesse d’être interrogée automatiquement. Une relecture n’envoie jamais une annulation, une relance ou une correction. Journaux et outils administrateurs restent accessibles dans les détails. Les documents publics « Lancer une analyse », « Dépannage » et « Connecter GitHub » décrivent maintenant ces capacités et ces reprises.

## Preuves et vérifications

| Périmètre | Vérification réelle | Limite |
| --- | --- | --- |
| Réglages, sections, sauvegarde, propriétés Google/Bing, GitHub, membre en consultation | Navigateur Chromium local, application FastAPI et base SQLite réelles, à 1440 / 768 / 390 / 360 px ; labels, navigation clavier, focus, interrupteurs de 44 px, liens profonds et sauvegarde. | Comptes fictifs ; transports fournisseurs simulés. |
| Liste, filtres, retour, attente/en cours/finalisation/arrêt demandé/annulé/échec/terminé/bilan absent | Même navigateur, rapports locaux réels et travaux fictifs ; aucun débordement horizontal sur les cas enregistrés. | Ne constitue pas une recette authentifiée de production ni un historique exhaustif. |
| Réponse GitHub perdue, refus, session expirée, retrait confirmé/annulé ; erreur de lecture et perte d’accès du suivi | Réponse perdue après exécution réelle du POST local, puis GET de l’état ; assertions d’absence de nouvel envoi et de reprise après relecture. Retrait contrôlé dans la base. | Aucune mutation d’un dépôt ou site distant. |
| Automatisation et corrections existantes | 12 affichages et 3 assertions de parcours : lot WordPress mixte, validation dans la fenêtre et proposition GitHub en attente de publication. | Génération, CMS et fournisseurs simulés. |
| Documentation | 3 pages publiques × 4 largeurs, soit 12 affichages. | Relecture éditoriale et navigateur local. |
| Texte et aides des nouveaux composants | 3 905 mesures de contraste calculé, minimum 4,58:1, aucune sous 4,5:1 ; 89 extraits regroupés dans la preuve. | Échantillon ciblé avec composition des fonds RGBA ; pas une certification globale, ni une recette humaine avec lecteur d’écran. |
| Droits, CSRF, données et rapport exact | 63 tests ciblés réussis, dont 13 nouveaux cas ; suite locale complète : **8 335 réussis, 43 ignorés**. | 471 avertissements existants, principalement de dépréciation. Le résultat CI du commit livré est consigné dans la PR. |

Les [111 situations réglages/activité](ux-analysis-activity/browser-evidence.json), les [12 affichages des documents](ux-analysis-activity/docs-evidence.json), la [non-régression des corrections](ux-analysis-activity/correction-regression.json) et les [mesures de contraste](ux-analysis-activity/contrast-evidence.json) sont conservés. Aucun incident JavaScript n’a été relevé dans ces recettes. Compilation Python/Jinja, Ruff et Bandit medium/high passent également.

Captures : [réglages ordinateur](ux-analysis-activity/settings-essential-1440.png), [réglages mobile](ux-analysis-activity/settings-essential-390.png), [consultation membre](ux-analysis-activity/settings-member-390.png), [activité mobile](ux-analysis-activity/activity-390.png), [bilan disponible ordinateur](ux-analysis-activity/job-ready-1440.png), [bilan disponible mobile](ux-analysis-activity/job-ready-390.png).

Aucun appel fournisseur réel, paiement, email, écriture sur site client ni déploiement pendant la recette. L’authentification en production, les OAuth réels, les paiements/emails et la recette humaine avec lecteur d’écran restent à vérifier. Le statut public du service et les finitions publiques restent des lots ouverts ; cet ajout ne clôt pas l’audit complet.
