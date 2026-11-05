# Référencement et préparation éditoriale de Noyaru — lot 4A

Implémentation du 4 novembre 2026, depuis `main` `8e6647e`.
L'espace privé est `/settings/system/own-seo`, accessible depuis Système.
Claude conserve les intégrations CMS, les catalogues de traduction et les captures.
Ce lot ne modifie aucun article public ni fichier de traduction.

## Audit de départ et périmètre vérifié

| Élément | Vérification | Constat / décision |
| --- | --- | --- |
| Accueil Noyaru | HTTP public, 4 novembre | 200, H1 présent, canonical `https://noyaru.com/` |
| `/ressources-seo` | HTTP public + code du chargeur Markdown | 200, canonical correct, neuf ressources publiées ; conserver ce catalogue et ses traductions |
| `/robots.txt` | HTTP public | 200, sitemap déclaré ; cette lecture ne prouve pas l'indexation |
| Contenu | Code | Modes article exportable et PR brouillon déjà présents, quotas et validations conservés |
| Bing web / crawler | Code + cas reproduits par tests | En l'absence de propriété correspondante, le premier site du compte pouvait être choisi ; un site configuré étranger pouvait aussi être utilisé |
| Rapports enregistrés | Code + tests | Les anciennes données Bing n'étaient pas validées contre l'adresse du rapport ; elles doivent être exclues du calcul si leur périmètre manque ou diffère |
| Espace propre | Chromium local avec sources simulées | Parcours propriétaire, états vide/peuplé/pause/erreur, FR/EN, cinq largeurs ; aucune recette payante ou publication réelle |

Les trois lectures publiques ci-dessus ne sont pas une analyse exhaustive, ni une
vérification des propriétés privées Google/Bing. Les scénarios peuplés de l'espace
propriétaire sont testés avec des relevés locaux et des transports bloqués.

## Sélection des données

- Une liaison explicite par le propriétaire système, vers un projet **qu'il possède**
  et dont l'adresse normalisée est exactement `https://noyaru.com/`.
  Les partages d'équipe, projets clients, autres sous-domaines/protocoles/chemins,
  paramètres, identifiants d'URL et ports étrangers sont refusés.
- La migration additive `20261104_0024` ne choisit ni ne crée un projet et n'active
  aucune analyse, IA, automatisation ou publication. Les limites habituelles de
  création de projet restent dans le parcours Projets existant.
- Droits et cible relus pour chaque consultation, préparation, relecture et export.
  Un changement de liaison génère un nouveau jeton ; les anciens formulaires et
  briefs ne peuvent agir sur le nouveau projet. Un changement de propriétaire ou
  d'adresse rend l'ancienne liaison inutilisable.
- Dernier rapport terminé parmi les 20 analyses récentes du seul projet lié. Adresse
  exacte exigée, date exploitable de moins de sept jours, absence de blocage/limite
  de temps. Une analyse partielle ou ancienne ne produit pas de propositions mesurées.
- Google : propriété `sc-domain:noyaru.com` ou préfixe exact du site.
  Bing : propriété exacte du site. Les pages proposées appartiennent aux routes
  publiques connues de Noyaru ; les URL privées, étrangères ou avec paramètres sont
  exclues. Les recherches constituent des questions à étudier, pas la preuve qu'un
  contenu manque sur le site.
- Périodes source datées : fin non future et de moins de 45 jours, début antérieur,
  durée maximale d'un an. Mesures numériques finies, clics non supérieurs aux
  impressions. Filtre éditorial initial : au moins 20 impressions et position de
  1 à 20. Ce seuil est une heuristique de sélection, pas une estimation de gain.
- Échantillons `top` enregistrés par le crawler, dix lignes par source/dimension,
  vingt propositions au maximum. Une même page/question n'est proposée qu'une fois,
  avec le relevé échantillonné ayant le plus d'impressions comme source nommée.
  Les volumes de Google et Bing ne sont pas additionnés et leur évolution ne prouve
  aucune causalité. Les limites d'échantillonnage sont affichées.

## Correctif Bing partagé

Le web et le crawler autonome appliquent le même contrat de périmètre, avec tests
communs : protocole, hôte, port et chemin correspondent exactement, hormis la barre
finale et les ports par défaut. Aucun remplacement automatique par `www`, HTTP ou
un autre site du compte. Le crawler portable conserve son propre petit prédicat
standard-library, testé contre celui du web, sans dépendre du serveur web.

Une configuration étrangère est refusée avant lecture des accès ou statistiques.
Sans correspondance dans `GetUserSites`, aucun appel de statistiques n'est lancé.
À la lecture des rapports, les métadonnées et anomalies Bing d'une propriété
étrangère ou non confirmée sont mises à l'écart. Le fichier d'audit original est
conservé ; il n'est pas réécrit comme s'il avait été analysé à nouveau.

Les données explicitement importées mais sans preuve de propriété ne nourrissent
pas le plan autonome. Une propriété correspondante doit être choisie et un relevé
valide obtenu par le parcours habituel. Aucun nouvel appel fournisseur n'est
réalisé par la page de préparation.

## Brief, relecture et suite du parcours

1. Relier le projet Noyaru, puis consulter ses priorités techniques et ses sources.
   Les anomalies gardent leur analyse. Le lien « Voir le constat enregistré » ouvre
   les preuves du brief : projet, page/recherche, source, dates et mesures exactes.
   Il ne relance pas les statistiques en direct sur une période glissante ; un
   relevé plus récent ne remplace pas l'observation utilisée pour le brief.
2. Préparer un brief local à partir d'une proposition reconstruite côté serveur.
   Le formulaire transmet uniquement son empreinte et la liaison ; aucun relevé ou
   texte de client fourni par un champ caché n'est accepté. Les doubles clics et
   préparations concurrentes renvoient le même brief.
3. Une idée de guide SEO/GEO est aussi possible quand les repères Google IA et Bing
   AI Performance sont **actuels et validés** et que son slug n'existe pas dans le
   catalogue. Ce n'est pas un sujet prétendument mesuré : l'interface le distingue.
   Une simple nouvelle lecture du même document ne crée pas de nouveau guide ; un
   retrait, changement de contenu ou de consigne, ou une source ancienne invalide
   sa relecture. Les nouveaux documents externes ne sont jamais approuvés ici.
4. Le brief contient les preuves datées, le lecteur visé, un plan proposé et les
   vérifications de rédaction. C'est un document de travail, **pas un article**.
   Il n'invente ni volume, fonctionnalité, cas client, résultat ou gain de visibilité.
5. Relecture explicite ou abandon, avec journal minimal sans requêtes, URLs ou
   contenu de page dans les événements d'audit. Une preuve changée interdit la
   validation. L'export historique garde ses dates et invite à revérifier les sources.
6. Pour une recherche/idée relue, Contenu s'ouvre avec le sujet prérempli. Ce GET
   n'appelle aucune IA. Le brief reste disponible à côté ; les droits, quotas,
   confirmations et modes de rédaction existants s'appliquent. Une proposition pour
   améliorer une page renvoie à son signal existant plutôt qu'à une nouvelle page.

L'espace est suspendable ; lecture et abandon restent possibles. Les tables
`own_seo_workspace` et `own_seo_briefs` sont protégées par les mêmes droits système
que la veille. Le verrou court de préparation n'encadre aucun appel IA ou écriture
externe. Une suppression de projet retire ses briefs et rend la liaison inutilisable.

Les conseils de rédaction se fondent sur l'utilité et des preuves produit réelles,
conformément aux [principes de contenu utile de Google](https://developers.google.com/search/docs/fundamentals/creating-helpful-content)
consultés le 4 novembre 2026. Cette référence de conception n'est pas ajoutée ni
promue automatiquement dans la veille. Une analyse technique ne permet pas de
mesurer le succès éditorial ou les citations IA.

## Étape suivante

Le [lot 4B1](REDACTION_SOURCEE_NOYARU.md) apporte la rédaction privée sous budget et
autorisation, avec sources par paragraphe et relecture explicite du texte. Le lot 4B2
reste la publication propre au blog de Noyaru avec ses preuves de déploiement.
Le mandat 3C limité aux titres/descriptions n'est pas étendu
à des articles entiers. L'observation des clics, de l'indexation et des citations
nécessitera ses fenêtres et sources réelles ; aucun résultat n'est fabriqué ici.
