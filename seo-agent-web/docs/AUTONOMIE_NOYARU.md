# Autonomie de Noyaru — état, architecture et lots de livraison

Établi le 2 novembre 2026, depuis `main` au commit `71b99a7`.
Demande de Tony : apprentissage des erreurs, autocorrection, veille SEO/GEO,
référencement de Noyaru, SAV par email, marketing sur le blog, Instagram et TikTok.
Claude conserve les intégrations CMS (Shopify, PrestaShop, etc.).

## Résultat attendu

L'agent observe, explique sa décision, prépare une action, l'exécute dans son mandat,
mesure son résultat et adapte la suite. Chaque capacité a un périmètre, un budget,
des preuves et une possibilité d'arrêt. L'apprentissage visé est une mémoire
opérationnelle et l'amélioration évaluée des règles/prompts ; aucun réentraînement
automatique du modèle ni réécriture libre du code de production n'est prévu.

« Toujours à jour » doit devenir une propriété vérifiable : sources et dates de
révision visibles, échéance de relecture, signalement d'une connaissance périmée.
Le modèle seul ne fournit pas cette garantie.

## Audit de départ

Cet inventaire est un examen du code, pas une recette réelle des futurs services.

| Fonction | Déjà présent dans le code | Travail nécessaire |
| --- | --- | --- |
| Corrections | Journal durable, verrou du compte payeur, reprise, quotas, aperçus et reçus de contrôle | Historique exploitable, diagnostic des échecs, décisions évaluées et tentatives bornées |
| Assistant | Historique récent lié à la personne et au projet, trois fournisseurs | Mémoire factuelle du projet, documentation datée, accès aux outils selon mandat |
| Veille SEO/GEO | Instructions et connaissances du modèle | Registre des sources, collecte planifiée, changements datés, validation des nouvelles règles |
| Référencement de Noyaru | Moteur d'audit de sites, Search Console/Bing, contenu et suivi de corrections | Projet propre strictement lié à noyaru.com, plan éditorial, suivi des résultats, orchestration |
| SAV | Page publique de contact et envois transactionnels | Réception des emails, fils de discussion, classification, réponses, escalades et mesure de résolution |
| Marketing | Génération/planification de contenu, campagnes d'acquisition et automatisations existantes | Ligne éditoriale, médias adaptés, connecteurs sociaux, calendrier, validation et mesure |

Points d'appui : `backend/correction_guard.py`, `correction_journal.py`,
`correction_reconciliation.py`, `first_correction.py`, `_verify_corrections_after_crawl`
et `/api/assistant/chat` dans `backend/app.py`. Le contrôle actuel ne doit pas être
remplacé par une déduction à partir de l'état d'une PR ou d'un code HTTP 200.

## Lot 1 : mémoire factuelle des contrôles

Livré par la PR #66, fusionnée et vérifiée en production le 3 novembre 2026 :

- Table additive `correction_observations`, migration `20261102_0019`.
- Enregistrement depuis le calcul serveur d'un nouveau contrôle, dans la même
  transaction que la vérification de la tâche. Aucun import automatique des anciennes
  notes : leur provenance historique ne peut pas être reconstituée avec certitude.
- Résultats : `resolved`, `still_present`, `unverified`. Une référence qui ne confirme
  pas le défaut reste signalée ; aucune causalité, progression de classement ou
  amélioration des citations IA n'est déduite.
- Identité de l'application et du périmètre : un nouveau brouillon, une nouvelle
  déclaration de modification, une restauration CMS ou un changement de référence
  rend l'ancienne observation inapplicable aux conseils actuels.
- Une répétition du même contrôle ne crée pas de doublon. Plusieurs contrôles d'une
  seule modification ne sont pas présentés comme plusieurs tentatives.
- Lecture des contrôles par date de début, même si les travaux finissent dans un
  autre ordre. Fenêtre de 30 jours, 200 observations examinées et 8 types d'anomalies
  au maximum dans le contexte ; toute troncature est indiquée.
- L'assistant reçoit les derniers résultats applicables du seul projet autorisé.
  Aucun texte libre, contenu de page, URL, identifiant de compte ou secret du
  journal n'est ajouté à son contexte. Les notes ne peuvent pas fabriquer un reçu.
- Lecture JSON privée : `GET /api/projects/{slug}/correction-memory`, sous les droits
  d'équipe existants. Pas de route publique d'écriture ou de partage intercomptes.
- Suppression des observations avec la tâche, le projet ou le compte.

Le comportement attendu de l'assistant est désormais lié aux observations :

| Dernier constat applicable | Suite demandée à l'assistant |
| --- | --- |
| Défaut toujours présent | Vérifier publication, page et périmètre avant de suggérer une nouvelle tentative |
| Contrôle incomplet/non comparable | Obtenir un contrôle exploitable ; ne pas qualifier la stratégie d'inefficace |
| Défaut absent, référence inconnue | Confirmer la référence avant d'attribuer un succès |
| Défaut résolu, référence confirmée | Expliquer la disparition observée ; proposer l'étape suivante sans promesse de classement |
| Aucune observation exploitable | Dire que les preuves manquent ; ne pas inventer une expérience antérieure |

Limites du lot : mémoire de diagnostic pour l'assistant, sans exécuteur autonome,
arrêt technique des tentatives, nouvelle publication, nouvel envoi d'email ou tâche
de veille. La retenue du modèle est une instruction, pas un verrou de sécurité.
Une observation décrit l'état d'un défaut, pas la qualité générale d'un algorithme.
Les tâches et applications retirées restent dans l'historique tant que leur parent
existe, mais ne nourrissent plus le conseil courant. La rétention légale/opérationnelle
de cet historique sera définie avant l'exploitation du futur SAV.

## Lot 2 : veille et base de connaissances datée

Implémenté par la PR #67, avec recette locale et contrôles GitHub avant fusion :

- Écran propriétaire **Système → Veille SEO/GEO**, à
  `/settings/system/seo-knowledge`, en français et en anglais.
- Trois documents officiels suivis :
  [Google Search Essentials](https://developers.google.com/search/docs/essentials),
  [Google et la recherche générative](https://developers.google.com/search/docs/fundamentals/ai-optimization-guide),
  [Bing AI Performance](https://blogs.bing.com/webmaster/2026/2/Introducing-AI-Performance-in-Bing-Webmaster-Tools-Public-Preview/).
- Consultation planifiée une fois par jour via le workflow existant, à 03:00 UTC ;
  chaque source décide si une lecture est due. Une lecture réussie rend la suivante
  éligible après 20 heures : cette marge absorbe les écarts du cron quotidien et la
  durée des autres tâches, pour éviter de sauter un jour. Après une erreur, une nouvelle tentative devient éligible après une
  heure ; elle aura lieu au prochain appel du cron ou à une demande du propriétaire.
- Collecteur HTTPS sans identifiants ni proxy d'environnement, limité aux adresses
  compilées. Résolution publique et connexion à l'IP validée avec certificat vérifié
  pour le domaine d'origine. Aucune redirection suivie. Réponses, taille et lecture
  bornées ; une coquille JavaScript, une réponse vide ou une erreur ne valent pas lecture.
- Empreinte du contenu principal et de ses liens, pas seulement du début du texte.
  Historique de métadonnées et d'un court extrait : dix versions récentes plus la
  version approuvée, sans miroir intégral des articles.
- Tables additives `seo_knowledge_sources` et `seo_knowledge_revisions`, migration
  `20261102_0020` depuis `0019`. Elles ne contiennent pas de données des projets clients.
- Bail de cinq minutes par source, écriture conditionnée à son jeton, reprise bornée
  après interruption. Une ancienne lecture ne peut pas remplacer une plus récente.
- Trois repères FR/EN d'amorçage, relus pour cette livraison : ils ne sont activés
  qu'après une lecture dont l'empreinte correspond **exactement** à la version relue.
  Une inscription d'audit identifie cette validation livrée.
- Un nouveau contenu met le repère concerné en attente de relecture. Le propriétaire
  consulte la source, vérifie les deux résumés puis confirme la version. Le serveur
  refuse les versions dépassées, les lectures anciennes ou interrompues et celles
  encore en cours. La validation et ses résumés sont conservés dans le journal.
- Une source non consultée avec succès depuis plus de sept jours est ancienne. Une
  erreur de lecture est signalée, même si une ancienne version était exploitable.
  Ces états ne sont pas présentés comme une connaissance actuelle de l'assistant.
- Le propriétaire peut suspendre une source ou retirer son repère immédiatement.
  Une actualisation ne réinstalle pas un repère retiré. Une suspension invalide la
  lecture en cours ; les reprises répétées ne l'annulent pas lorsqu'elle est déjà active.
- L'assistant reçoit seulement les résumés validés, actuels et leurs références/date
  de consultation, dans la langue de l'interface. Il ne reçoit ni les extraits bruts,
  ni une instruction d'exécution provenant d'un document externe. Un article externe
  ne prouve pas qu'une fonction décrite existe déjà dans Noyaru.

Le collecteur ne fait aucun appel IA, aucun envoi d'email ni modification de site.
Les questions adressées à l'assistant gardent leur quota habituel. Un incident sur
une source reste visible dans l'écran et dans la réponse du cron, y compris pendant
le délai avant la prochaine tentative. Aucun autre traitement quotidien n'est
supprimé ou déclenché manuellement par cette livraison.

Les [consignes Bing](https://www.bing.com/webmasters/help/bing-webmaster-guidelines-30fba23a)
restent une référence de consultation : leur adresse redirige vers une application
JavaScript, dont une réponse HTTP seule ne permet pas la lecture fiable. Elles ne
figurent pas parmi les trois sources automatisées. Ce périmètre initial ne couvre
pas tous les sujets SEO/GEO ; il rend les sources utilisées et leurs limites visibles.

Les nouveaux repères doivent encore être relus par le propriétaire. Leur promotion
automatique après évaluation s'inscrit dans le moteur de décisions du lot 3 ; le
collecteur ne transforme pas de lui-même une nouvelle page en règle d'action.
Les précédents résumés restent consultables dans l'historique des validations :
revenir à un repère antérieur exige une nouvelle vérification contre la version actuelle.

## Lot 3A : décisions après contrôle et limites de recommandation

Livré par la PR #68, fusionnée et vérifiée en production le 3 novembre 2026 :

- Le moteur de contrôle transmet son périmètre exact au journal de décisions,
  dans la même transaction que l'observation et le résultat de la tâche.
- Table additive `correction_decisions`, migration `20261103_0021` depuis `0020`.
  Une empreinte représente le périmètre ; aucune URL, note, clé ou contenu brut
  n'est dupliqué dans cette table ou le journal de décision.
- Cinq suites possibles : vérifier la publication avant une autre stratégie,
  obtenir un contrôle exploitable, confirmer le défaut initial, demander une revue,
  ou passer à la correction suivante après résolution confirmée.
- Les compteurs portent sur des modifications déclarées distinctes avec défaut
  encore observé et référence confirmée. La déclaration doit précéder le contrôle ;
  une application CMS doit aussi porter l'état appliqué confirmé du même projet et
  de la même tâche. Les brouillons, fusions GitHub et changements de statut seuls
  ne prouvent pas la publication et ne sont pas comptés. Une restauration CMS n'est
  pas une nouvelle tentative corrective. Un changement de référence sans nouvelle
  déclaration ne crée pas une nouvelle application. Plusieurs contrôles d'une même
  modification ne comptent qu'une fois ; un contrôle incomplet n'est pas un échec.
  Une résolution confirmée
  du même périmètre ouvre un nouveau cycle.
- Fenêtre de 30 jours et 200 observations au maximum par tâche. Après deux
  applications distinctes avec défaut persistant, ou avec un historique incomplet,
  le moteur arrête de recommander une nouvelle tentative automatique.
  Cette limite porte sur la recommandation ; ce lot n'active aucun exécuteur et
  n'interdit pas les interventions manuelles du parcours existant.
- Les anciennes observations sans périmètre capturé restent signalées comme
  incomplètes. Une actualisation ne reconstruit pas leur périmètre à partir d'une
  ancienne note. Seul un nouveau calcul réel du moteur peut fournir cette preuve.
- Les calculs d’un projet sont sérialisés sur PostgreSQL par un verrou de
  transaction pris avant l’écriture de l’observation. Il ne verrouille pas les
  lignes des tâches clients ; SQLite conserve sa sérialisation des écritures.
- Les contrôles sont ordonnés par leur début, pas par leur arrivée. Un résultat en
  retard met à jour la décision de l'application courante ; il ne remplace pas son
  dernier résultat par un ancien. L'identité d'une application continue d'être
  invalidée par une nouvelle proposition, déclaration, restauration ou référence.
- Chaque révision de décision conserve sa version de politique, l'empreinte des
  preuves et son motif dans le journal d'audit existant. Les lignes de décision
  disparaissent avec l'observation, la tâche, le projet ou le compte ; le journal
  d'audit garde sa politique de conservation préexistante.
- Écran privé **Corrections → Suite de vos corrections**, bilingue FR/EN, liens
  contextualisés vers le bon contrôle, la bonne page et les instructions. Lecture
  JSON privée sous `/api/projects/{slug}/corrections/decisions` ; actualisation POST
  protégée par CSRF, droits d'équipe et limite de fréquence.
- GET sans écriture en base. L’écran peut relire les rapports conservés dans le
  stockage autorisé du compte. Un rapport absent renvoie à la tâche précise dans le
  suivi, sans substituer une analyse plus récente. Le POST ne fait que calculer et
  journaliser les décisions des
  observations serveur applicables. Aucun accès réseau, crédit IA, nouvelle analyse,
  modification de site, email, publication ou changement de droits.
- L'assistant reçoit la décision et les compteurs bornés du seul projet autorisé,
  sans URL, note ou identifiant de tâche. Une décision ne vaut pas mandat d'exécuter.

## Lot 3B : premier exécuteur, préparation autonome d'un aperçu

Implémenté dans `feat/noyaru-autocorrection-executor`, depuis `main` `8c28003`.
L'activation est volontaire et limitée à une tâche ; aucun compte existant ne
reçoit de mandat à la migration. Claude conserve les intégrations CMS.

- Écran **Corrections → Suite de vos corrections → Préparation autonome**, FR/EN,
  à `/projects/{slug}/corrections/autonomy`. Les membres lisent le suivi ; seul le
  propriétaire autorise, suspend, retire le mandat ou confirme le brouillon GitHub.
- Premier périmètre : titres et descriptions de pages GitHub, dont la source est
  résolue par le parcours unitaire existant. Aucun choix de fichier IA, correction
  CMS, correction structurelle, audit supplémentaire, email ou publication autonome.
- Un contrôle serveur de moins de sept jours doit encore observer le défaut après
  une modification déclarée, sur cette seule page. Historique complet et première
  application en échec requis ; le seuil du lot 3A reste bloquant. Le propriétaire
  confirme avoir vérifié la publication, la page et le fichier avant d'autoriser.
- Une autorisation couvre **un seul aperçu**, pendant **24 heures**, avec au plus
  **1 crédit** selon le décompte existant. Forfait et quota courants sont relus au
  départ sous le verrou payeur. Le décompte intervient uniquement lorsqu'un aperçu
  exploitable est enregistré ; la réservation du travail ne promet pas une place
  de quota avant son départ.
- Bornes fournisseur supplémentaires réservées avant chaque requête HTTP : au plus
  4 requêtes, 360 000 caractères d'entrée et 20 000 jetons de sortie au total ; au
  plus 100 000 caractères et 8 000 jetons de sortie par requête. Le repli technique
  OpenAI sur la température réserve lui aussi une requête. Ces plafonds sont des
  limites conservatrices, pas une mesure de coût en euros ni de consommation réelle.
- Table additive `autocorrection_runs`, migration `20261103_0022`. Elle réunit
  mandat, file durable, preuves opaques, limites, réservations et lien vers le
  `CorrectionOperation` existant. Clé unique par tâche et application : répéter un
  contrôle, cliquer deux fois ou renouveler la demande ne crée pas un autre aperçu.
  Les consentements versionnés et transitions sont audités sans URL ou contenu brut.
- Ordonnanceur du service web toutes les 30 secondes, file bornée ; route de secours
  `POST /cron/autocorrection-previews` sous le secret cron existant. La route n'est
  pas ajoutée au workflow quotidien dans ce lot. Les demandes survivent à un
  redémarrage ; les ordonnanceurs de test restent désactivables.
- Avant chaque lecture GitHub et requête IA : relecture de la propriété, du mandat,
  de l'expiration, du contrôle, de l'application, de la configuration et des limites.
  Les lectures sont bornées au dépôt autorisé. Les écritures GitHub sont refusées
  dans le contexte de préparation, y compris GraphQL. Une note client ou un contenu
  de fichier ne peut étendre ces droits.
- Retrait du mandat et suspension du projet sans attendre le long verrou payeur.
  Une préparation en attente reste reprenable après suspension, dans sa validité.
  Arrêt opérateur par `NOYARU_AUTOCORRECTION_PAUSED=true` ; les ordonnanceurs respectent
  aussi `SEO_AGENT_DISABLE_SCHEDULERS`. Une requête déjà réservée/partie peut terminer ;
  son résultat et son éventuel crédit sont conservés. L'arrêt ne promet pas de
  rappeler une requête déjà transmise.
- Erreur ou timeout fournisseur : arrêt sans repli automatique aveugle. Après
  interruption, seul un aperçu déjà sauvegardé peut être réconcilié et décompté une
  fois. Sans reçu exploitable, revue requise et journal conservé ; aucun verrou
  effacé pour relancer le modèle. Les reprises transitoires plus générales restent
  hors de ce premier exécuteur.
- L'aperçu est affiché avec son diff échappé. Une **confirmation humaine distincte**
  autorise la branche et la PR en brouillon par le chemin existant. Le serveur relit
  sa cible à chaque transport, refuse une source distante changée, interdit tout
  nouvel appel IA dans cette confirmation et ne fusionne pas la PR. Aucun code
  fourni par un champ caché n'est accepté : le serveur reprend son aperçu enregistré.

Ce lot ne prépare pas plusieurs stratégies en boucle et ne publie pas une correction
sur un site client. La prochaine extension devra apporter ses propres preuves de
publication, mandats par type d'écriture, contrôle et reprise après résultat inconnu.
Les scénarios peuplés se testent localement avec transports simulés ; une recette
réelle consommant un crédit nécessite un mandat propre au compte/projet choisi.

## Lot 3C : publication GitHub et contrôle sous mandat distinct

Implémentation du 4 novembre 2026, depuis `main` `447139c`. Ce pilote nécessite un
nouvel accord du propriétaire sur l'aperçu exact, distinct du mandat de préparation.
Écran FR/EN : `/projects/{slug}/corrections/autonomy/publications`, lié depuis les
préparations. Les membres autorisés peuvent lire le suivi ; seul le propriétaire
peut autoriser et révoquer. Les GET ne créent aucun mandat et ne démarrent aucune
analyse. La migration additive `20261103_0023` n'active aucun ancien aperçu.

Périmètre : un aperçu enregistré pour un titre ou une description, une page, un
fichier, une branche, une PR, une fusion et un contrôle. Aucun nouvel appel IA,
crédit IA, connecteur CMS, email, réglage de dépôt ou retour arrière automatique.
Le propriétaire relit le diff et accepte séparément la publication puis le contrôle.
La version du mandat, le fichier/code approuvé, la cible, le propriétaire, les
réglages d'analyse et l'application d'origine sont conservés par empreinte.
Un défaut résolu, une cible, un code ou des droits changés arrêtent la publication.

Séquence durable :

1. Accord valable 24 heures, au plus 240 lectures et 5 écritures GitHub. Chaque
   requête réserve sa place avant transport, sous le verrou partagé du compte.
   Une pause globale/projet suspend la suite ; un arrêt bloque les requêtes suivantes.
2. Création du brouillon par le chemin GitHub existant, avec son journal et le
   contrôle du fichier source inchangé, lié au reçu de cet aperçu précis. Un autre
   aperçu du même fichier ne peut remplacer ce reçu. Les écritures permises sont limitées au
   fichier/code approuvé et à la branche/PR de cette opération. Les PR gérées sont
   exclues du vérificateur automatique historique.
3. Relecture des protections du dépôt : au moins un contrôle obligatoire, branche
   à jour imposée et protections appliquées aux administrateurs. Noyaru ne modifie
   pas ces protections. Les règles GitHub supplémentaires continuent de s'appliquer.
   Après validation du code, sortie du brouillon pour permettre le démarrage de la CI.
   Puis résultats complets des check-runs et statuts ; un contrôle obligatoire neutre ou
   ignoré ne vaut pas réussite. Une liste partielle, un échec, un fichier/commit ou
   une base changés arrêtent le mandat. Une attente reste bornée par durée/requêtes.
4. Fusion squash avec le SHA de tête relu dans le corps
   de la requête. Aucune fusion forcée. La branche strictement protégée traite les
   changements de base entre la lecture et la fusion ; une réponse inconnue se
   réconcilie en lecture, sans rejouer l'écriture. Une écriture incertaine sans reçu
   récupérable demande une relecture et conserve le journal pour diagnostic.
5. La fusion seule ne lance aucun crawl. Noyaru attend un Deployment GitHub pour le
   commit fusionné et l'environnement choisi, marqué production/non temporaire,
   avec son dernier statut réussi et l'origine exacte du site. Une version remplacée,
   un mauvais domaine ou un statut d'échec arrêtent la suite. Ce reçu est celui de
   l'intégration de déploiement du client ; ce pilote exige cette intégration.
6. Une fois le déploiement confirmé : déclaration de l'application, réservation du
   quota et création d'un seul job dans la même transaction. Budget de 1 à 300 pages,
   limité par les réglages courants, le plan et le solde du compte. Le verrou de quota
   est partagé avec les réservations manuelles existantes. La date UTC du contrôle
   suit la déclaration, indépendamment du fuseau du worker. La cible vient du projet,
   même si un ancien fichier d'analyse contient une autre adresse.
7. Le worker revérifie droits, cible, réglages, budget et absence de réexécution.
   Un arrêt annule et rembourse un job en attente ; un job lancé reçoit l'annulation
   existante. Une ancienne copie du job ne peut retirer cette demande ni annuler un
   reçu de facturation. L'usage connu est ajusté une seule fois dans la période de la
   réservation. Un usage inconnu conserve la réserve pour rapprochement.
8. « Correction vérifiée » exige l'observation de contrôle réellement enregistrée par
   le vérificateur existant pour ce job, cette tâche et cette nouvelle application,
   avec défaut résolu et référence confirmée. Analyse terminée, fusion et déploiement
   ne suffisent pas. Un résultat incomplet ou un défaut persistant demande relecture ;
   aucun second crawl ni nouvelle correction IA ne part sous le même mandat.

Validation : tests du protocole avec transports simulés, worker réel avec rapport
local et vérificateur existant, concurrence de quotas, migration aller/retour,
formulaires et 40 cas Chromium FR/EN à 1440/1024/768/390/360 px. Les tests locaux
n'envoient ni code à GitHub, ni requête IA, ni crawl sur un site client.
La recette de production de cette livraison reste en lecture seule : aucune
publication ni consommation de pages réelles n'est implicitement autorisée.

Références primaires consultées pour le protocole GitHub :
[SHA de fusion](https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28#merge-a-pull-request),
[protections strictes](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches),
[statuts de déploiement](https://docs.github.com/en/rest/deployments/statuses?apiVersion=2022-11-28).

Les familles CMS, la restauration et la modification des stratégies/prompts restent
à étendre avec leurs propres mandats et évaluations. Le moteur ne réécrit pas le
code du service en cours d'exécution. Le référencement propre/blog, le SAV email
et les réseaux sociaux restent les lots suivants décrits ci-dessous.

## Lot 4A : référencement propre et briefs éditoriaux

Implémentation du 4 novembre 2026 : espace propriétaire à
`/settings/system/own-seo`, liaison explicite au seul projet détenu par le propriétaire
pour `https://noyaru.com/`, priorités techniques, propositions depuis les relevés
Google/Bing et idée de guide depuis des sources actuelles/validées. Briefs durables,
relecture, export Markdown et passage dans Contenu avec le sujet prérempli.
Aucun appel IA, nouvelle analyse, publication ni activation par migration.

Le défaut de sélection Bing est corrigé dans le web et le crawler ; les propriétés
étrangères/non confirmées des rapports enregistrés sont aussi exclues à la lecture,
sans réécrire les preuves originales. Les données clients restent hors de cet espace.

Audit, règles de périmètre, limites et validation détaillés dans
[REFERENCEMENT_NOYARU.md](REFERENCEMENT_NOYARU.md). Le lot 4B se poursuit ci-dessous :
rédaction sourcée privée en 4B1, puis publication du blog et observation des
résultats en 4B2. Une proposition de contenu ne prétend pas prouver un
manque éditorial ni un gain futur ; les citations IA sont distinctes des positions.

## Lot 4B1 : rédaction sourcée privée

Depuis `main` `b8a83b0`, le parcours de rédaction du propre référencement est séparé
des transports clients. Écran propriétaire `/settings/system/own-seo/writing` :
brief actuel relu → résumés officiels validés et faits produit publics → autorisation
pour ce dossier → brouillon privé avec références par paragraphe → relecture des
affirmations → export daté. Aucun droit de publication ou d'écriture GitHub.

Chaque autorisation expire après 24 heures et permet une seule requête de texte ;
deux requêtes au plus sur 24 heures glissantes par propriétaire, administrateur inclus,
entrée bornée, sortie de 3 600 tokens au plus. Ce n'est pas un plafond monétaire.
Une opération durable et un bail empêchent le double appel après double clic ou
redémarrage ; une requête déjà partie n'est pas répétée. Les reprises de la tâche
quotidienne traitent au plus une opération explicitement autorisée, jamais un nouveau
sujet. La pause, les droits, le brief et les sources sont revérifiés avant l'appel
et avant d'en conserver le résultat. Les textes rejetés ou incertains ne déclenchent
aucun nouvel essai automatique.

Les contrôles de structure et d'identifiants de sources ne certifient pas les faits.
La relecture explicite porte sur une empreinte précise et exige des preuves encore
actuelles ; les exports historiques affichent leurs limites. Audit, comportement
et validations : [REDACTION_SOURCEE_NOYARU.md](REDACTION_SOURCEE_NOYARU.md).
La publication native du blog et l'observation des résultats restent le lot 4B2.

## Lot 5 : SAV email

Canal confirmé : `contact@noyaru.com`, actuellement transféré vers Gmail. La boîte
Gmail réceptrice et sa connexion seront choisies dans une étape dédiée, sans secret
dans le chat. Vérifier le droit d'envoyer avec l'adresse de support avant le pilote.
La connexion Google des clients, utilisée pour Search Console, ne doit pas être
élargie au SAV de Noyaru. La lecture Gmail relève de permissions restreintes ;
évaluer le périmètre et les exigences applicables avant de retenir ce transport.
Un récepteur entrant dédié est une autre option à examiner.
[Permissions Gmail, consultées le 2 novembre 2026](https://developers.google.com/workspace/gmail/api/auth/scopes).

À livrer : ingestion autorisée, tickets/fils avec identifiant de message unique,
catégorie, langue, urgence, preuves produit et état de traitement. Une redirection
ne suffit pas à démontrer l'identité d'un titulaire de compte.

- Première étape : classer et préparer des réponses à partir de la documentation
  actuelle, avec brouillons et motifs d'escalade ; aucun envoi automatique activé.
- Pilote autonome limité ensuite aux réponses d'aide validées, dans les fils
  entrants légitimes. Anti-doublon, anti-boucle, gestion des retours et limite d'envois.
- Escalade : facturation litigieuse, remboursement, suppression de compte,
  demande d'accès, incident de sécurité, données privées, ambiguïté ou réponse
  insuffisamment étayée. Une demande reçue ne vaut pas autorisation d'une action.
- Les emails/pièces jointes sont des entrées non fiables. Aucune instruction reçue
  ne peut accorder des droits, retirer un verrou ou déclencher une publication.
- Mesurer résolution, reprises humaines, erreurs et satisfaction ; contrôler un
  échantillon de réponses et retirer une règle qui dégrade le service.

## Lot 6 : Instagram et TikTok

Premier lot éditorial natif du 14 novembre 2026 : studio propriétaire
`/settings/system/social`, en français et en anglais. Objectif et public choisis,
pistes distinctes par réseau, accroche, quatre cartes Instagram ou déroulé vidéo
TikTok, texte d’accompagnement, consignes de réalisation, références et indicateurs
à observer. Les cinq pistes de départ sont relues avec cette version ; leur ordre
est une règle éditoriale explicite, pas une tendance mesurée ou une prédiction.

Le blog est facultatif : une seule piste peut renvoyer au guide du même sujet,
si sa page existe réellement dans la langue demandée. Aucun article le plus récent
n’est transformé par défaut. Aucune donnée SAV, privée ou de projet client utilisée.
Les repères officiels doivent rester actuels et correspondre aux résumés relus ;
une modification des résumés ou de la documentation produit met les pistes
concernées à l’écart jusqu’à relecture du contenu. L’export revérifie la version.

Ce lot prépare du texte et un dossier téléchargeable sans appel IA, création de
média, décision Telegram, envoi ou publication. Aucun accord de diffusion n’est
créé par une consultation ou un export. Aucun calendrier durable ni collecte de
performances sociales n’est activé. Prochaines étapes : production/validation des
médias, comptes et connecteurs, décisions Telegram puis observation des résultats.
Détails, audit et validations : [SOCIAL_EDITORIAL_NOYARU.md](SOCIAL_EDITORIAL_NOYARU.md).

Les comptes n’existent pas encore selon Tony. Les connexions sont à choisir avec
lui. Capacités officielles relues le 14 novembre 2026 :

- [Instagram Content Publishing](https://developers.facebook.com/docs/instagram-platform/instagram-api-with-instagram-login/content-publishing) :
  comptes professionnels, autorisations de publication et accès appropriés requis.
- [TikTok Content Sharing Guidelines](https://developers.tiktok.com/docs/en/content-sharing-guidelines) :
  Direct Post exclut les utilitaires destinés aux seuls comptes de leur équipe.
  Pour les comptes Noyaru, prévoir une remise de contenu ou un service compatible,
  dont les capacités et confirmations seront vérifiées avant connexion. Aucune
  autopublication par une app interne n’est promise.

Aucun démarchage en masse, achat publicitaire ou budget réel activé par ce lot.

## Mandats et supervision communs

| Action | Mode initial | Condition d'autonomie ultérieure |
| --- | --- | --- |
| Lire des données déjà accessibles | Autorisé dans le périmètre du compte/projet | Droits vérifiés à chaque exécution |
| Diagnostiquer, préparer un brouillon | Automatique avec quota existant applicable | Sources et preuves identifiables, coût borné |
| Modifier un site client | Parcours de validation existant conservé | Mandat explicite par site/type d'action, journal et contrôle |
| Publier sur Noyaru ou un réseau | Brouillon à relire | Canal, contenu admissible, fréquence et arrêt configurés |
| Répondre à un client par email | Brouillon à relire | Connexion validée et pilote SAV limité approuvé |
| Dépenser, changer un abonnement/droit, rembourser | Aucune délégation créée | Autorisation distincte et plafond technique |
| Réécrire/fusionner/déployer le code de Noyaru | PR et vérifications | Politique de livraison distincte, jamais décidée par un prompt reçu |

Le superviseur doit montrer les actions récentes, décisions, règles actives,
versions, coûts, interruptions, reprises et validations en attente. Prévoir un arrêt
global et par capacité, appliqué côté serveur même si le modèle demande le contraire.

## Conditions d'acceptation par lot

1. Mémoire : contrôle réel, doublons/relectures, référence manquante, contrôles
   incomplets, résultats retardés, modification/restauration, notes forgées,
   séparation des comptes/équipes, suppression, migration et annulation transactionnelle.
2. Veille : indisponibilité, contenu changé, redirection inattendue, source ancienne,
   date d'effet future, instruction malveillante dans une page, promotion et retour arrière.
3. Exécution : double lancement, budget épuisé, tentative répétée, résultat inconnu,
   retrait d'un mandat pendant un travail, version distante modifiée et arrêt d'urgence.
4. SEO propre : cible noyaru.com prouvée, refus d'une propriété étrangère, brouillon
   relisible, contrôle de publication et mesure sans affirmation causale abusive.
5. SAV : doublon d'email, boucle, expéditeur non vérifié, demande sensible, panne
   d'envoi, escalade et résolution humaine. Recette avec des messages de test.
6. Social : comptes de test/autorisés, prévisualisation par canal, confirmation de
   publication, panne/réponse inconnue, limite et annulation sans publication en double.

Chaque lot doit avoir sa PR, ses preuves et ses limites. Aucune recette avec des
emails clients, une publication ou un budget réel n'est implicitement autorisée par
les tests locaux. Le chantier d'autonomie est distinct des PR de refonte déjà autorisées.
