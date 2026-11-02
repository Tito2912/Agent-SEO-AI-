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

Implémenté dans cette branche, à valider avant activation sur une instance :

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

Sources initiales examinées le 2 novembre 2026 :

- [Google Search Essentials](https://developers.google.com/search/docs/essentials) :
  exigences techniques, politiques antispam et contenu utile.
- [Google : optimisation pour les fonctions d'IA de Search](https://developers.google.com/search/docs/fundamentals/ai-optimization-guide) :
  les fondamentaux SEO restent pertinents pour la recherche générative ; ne pas
  proposer un balisage prétendument obligatoire ou une garantie de citation.
- [Bing Webmaster Guidelines](https://www.bing.com/webmasters/help/bing-webmaster-guidelines-30fba23a) :
  accessibilité, indexation, contenu exact, pertinence pour la recherche et ses
  réponses génératives.
- [Bing AI Performance](https://blogs.bing.com/webmaster/2026/2/Introducing-AI-Performance-in-Bing-Webmaster-Tools-Public-Preview/) :
  suivre les citations/indicateurs disponibles dans leur périmètre, sans les
  confondre avec les positions classiques ou toutes les surfaces IA.

Ce sont des sources d'amorçage, pas une veille déjà opérationnelle.

À construire : registre de sources officielles autorisées, collecte quotidienne
bornée en lecture, version et empreinte du contenu retenu, date de consultation,
date d'effet éventuelle, synthèse avec liens et échéance de révision. Le contenu
externe reste une donnée non fiable en tant qu'instruction d'exécution. Les
redirections et destinations réseau doivent être contrôlées ; une URL fournie
par un client ou un email ne doit pas devenir une source privilégiée.

Un changement crée une proposition de règle. Il passe un jeu d'évaluation avant
promotion : cas SEO existants, refus légitimes, fraîcheur des sources, fidélité des
citations et absence de régression. Versionner puis permettre le retour à la règle
précédente. Une source indisponible conserve sa dernière version en la signalant
comme ancienne ; elle ne devient jamais une preuve fraîche.

## Lot 3 : moteur de décisions et autocorrection bornée

Séparer trois objets : constat, décision et exécution. La décision porte la règle
et ses versions, les preuves, le projet, l'action ciblée, le coût maximal et les
autorisations nécessaires. L'exécution réutilise les verrous, journaux, quotas et
vérifications existants ; pas de second chemin d'écriture qui les contournerait.

- Diagnostic automatique des défauts persistants : publication réellement reçue,
  contenu source, URL cible, périmètre de l'analyse, puis stratégie alternative.
- Compter des applications distinctes, pas des contrôles répétés. Un changement
  sans résultat connu n'est pas relancé aveuglément.
- Maximum de tentatives et budget explicitement configurés, délais croissants pour
  les erreurs transitoires, arrêt et dossier de reprise après épuisement.
- Résultat externe inconnu après interruption : réconciliation en lecture avant
  toute nouvelle écriture, jamais effacement d'un verrou pour « réessayer ».
- Correction du code de Noyaru : reproduction, test pertinent, branche/PR et CI.
  Pas d'automodification du service en cours d'exécution.
- Une restauration exige une preuve de compatibilité de la version courante ; ne
  pas écraser une modification plus récente pour revenir à une ancienne version.

## Lot 4 : référencement propre et blog Noyaru

Créer ou sélectionner un projet du compte propriétaire, exclusivement lié à
`https://noyaru.com`, après vérification des droits. Les données des clients ne
servent ni de contenu marketing ni de mémoire globale sans un dispositif distinct.

Cycle : audit technique → opportunités Search Console/Bing → recherche sourcée →
brouillon éditorial → contrôle factuel et visuel → publication autorisée → indexation
et suivi. Réutiliser les reçus pour les défauts techniques ; les effets éditoriaux
se mesurent sur des périodes adaptées et ne sont pas déclarés « vérifiés » par un crawl.

Avant le pilote, traiter le constat antérieur de sélection Bing lorsqu'aucun site
de la connexion ne correspond au projet : refuser la substitution par un autre
domaine du même compte. Valider aussi les données mises en cache, pas seulement
les nouveaux appels. Ce correctif n'est pas livré dans le lot 1.

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

Les comptes n'existent pas encore. Tony choisira/créera les comptes et connectera
les accès officiels appropriés. Vérifier alors les capacités disponibles des API,
les types de comptes, les autorisations, quotas et contraintes de publication.
Ne pas simuler une intégration disponible avant cette vérification.

Vérification du 2 novembre 2026 : les règles TikTok Direct Post excluent les outils
réservés à ses propres comptes ou à une équipe interne ; un client non audité ne
dispose pas de publication publique par cette voie. Pour les seuls comptes Noyaru,
prévoir un service de publication compatible ou une remise de contenus à publier,
puis valider le connecteur choisi. Ne pas promettre l'autopublication par une app
interne dédiée. Les exigences de prévisualisation et de consentement restent à
respecter dans le parcours retenu.
[Règles officielles TikTok](https://developers.tiktok.com/docs/en/content-sharing-guidelines).

Établir une charte de marque, les sujets/publics cibles, les preuves acceptées,
le calendrier et les objectifs. Adapter les articles validés en scripts et médias
propres à chaque plateforme. Contrôler les faits, la lisibilité et les droits des
médias ; conserver les variantes, validations et identifiants de publication.

Commencer par des propositions prêtes à relire. L'autopublication viendra par
canal, fréquence et mandat explicites, avec file d'attente et arrêt immédiat.
Aucun démarchage en masse, achat publicitaire ou budget réel n'est activé par ce plan.

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
