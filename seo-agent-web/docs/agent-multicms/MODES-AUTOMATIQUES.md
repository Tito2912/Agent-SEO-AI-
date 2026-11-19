# Autorisations des modes existants — complément C084

État du 19 novembre 2026 : préparation en branche et PR #95 en brouillon.
Aucun mode client activé, aucune fusion ni publication réelle sur un site externe.
Les CGU/confidentialité restent hors de ce complément, en coordination avec Claude.

## Parcours livré

Deux modes existants sont couverts : fusion GitHub des familles déterministes
éligibles et rédaction planifiée dans le dépôt connecté. Cette dernière conserve
ses choix « vérification » (proposition à relire) et « publication automatique »
(fusion après contrôles du dépôt). Aucune nouvelle autopublication CMS native ni
autocorrection continue n’est introduite. Le mode de préparation autonome déjà
présent reste un accord par tâche, limité à 24 heures et à son périmètre antérieur.

Une activation exige une case non cochée par défaut. L’accord porte sur le
propriétaire, le site, le dépôt/branche et la version de l’avertissement ; pour
le contenu, il inclut rubrique, publication, langues, taille/ton et calendrier.
Sauvegarder le même périmètre ne renouvelle ni accord ni échéance. Élargir ou
changer ce périmètre demande un nouvel accord. Réduire la publication automatique
à la vérification humaine, sans autre changement, est possible sans nouvelle case :
le retrait/restriction est enregistré et l’accord d’origine n’est pas antidaté
ni prolongé. Désactiver reste possible sans quota ou abonnement d’écriture.

Les anciens réglages `auto`/`enabled` et audits historiques ne deviennent pas des
preuves inventées : ils affichent « Accord à confirmer » et aucune prochaine
exécution n’est promise avant une activation explicite. Une ancienne proposition
non liée à une activation actuelle ne peut pas être fusionnée automatiquement.

## Exécution et retrait

L’autorisation fonctionnelle est une ligne `AutomationMandate`, distincte du
registre des preuves cinq ans. Une réactivation crée une nouvelle identité : elle
ne réautorise pas les tâches de l’ancienne activation. Une signature serveur lie
le projet, cette activation, le SHA, le numéro/nœud GitHub, le droit de fusion et
le contrôle de sitemap éventuel. Modifier le JSON d’une note ne peut ni signer
une autre PR ni transformer une proposition à relire en permission de fusion.
L’origine des brouillons planifiés est également relue dans le registre serveur.
La clé de signature reste dans l’autorisation fonctionnelle et est exclue de
l’export des données du compte.

Le planificateur relit les réglages sous verrou avant de réserver le créneau et
de poser sa date. Une sélection ancienne ne peut pas réécrire un réglage qui a
été désactivé entre-temps. Les transports GitHub et IA relisent l’activation et
son périmètre avant chaque appel, y compris dans les contextes explicitement
copiés vers les fils de traduction. Aucun verrou de base n’est tenu pendant HTTP.

Une réservation unique précède les appels IA/écritures ; les lectures répétées
de CI ne consomment pas cette réservation. Un envoi commencé mais non confirmé,
ou un processus interrompu, n’est pas rejoué automatiquement. La même route
planifiée reste bloquée après redémarrage/réactivation tant qu’une tentative
reste en cours ou incertaine ; une nouvelle note ou le type JSON du numéro de PR
ne contourne pas l’unicité. L’écran Automatisation montre ces tentatives avec
la route et le lien de PR lorsqu’il est connu. Il n’offre pas de bouton qui
réessaierait aveuglément une écriture.

Le retrait empêche les prochains appels. Il ne rappelle pas une requête déjà
partie chez GitHub ou le fournisseur IA et ne garantit pas une annulation atomique
entre systèmes. Un opérateur doit vérifier le résultat d’une tentative incertaine ;
ce complément n’ajoute pas de résolution automatique de ces tentatives.

La PR distante est relue avant sa promotion et sa fusion : dépôt, branche de
base, numéro, nœud et SHA doivent encore correspondre. La relecture de la branche
reste distincte de la requête de fusion ; aucun verrou inter-systèmes ne la rend
atomique. La fusion emploie le SHA attesté et n’est annoncée réussie que sur une réponse
`merged: true`. Le contrat GitHub permet de refuser la fusion si le SHA ne
correspond plus à la tête : [REST — fusion d’une pull request](https://docs.github.com/en/rest/pulls/pulls#merge-a-pull-request).
Les quotas, contrôles de dépôt et interdictions existantes des corrections IA
non éligibles ne changent pas.

## Preuves et opérations manuelles

Activation, retrait et restriction enregistrent avant effet une preuve minimale
dans le registre C083 : acteur et copie d’e-mail, propriétaire/site, date,
empreinte, périmètre et échéance cinq ans après chaque événement. Les preuves
survivent aux suppressions du projet/compte, avec export isolé par propriétaire
et purge déjà livrés ; aucune copie d’article, d’image ou de secret dans ces reçus.
L’autorisation fonctionnelle active n’est pas prolongée par la purge d’une preuve.

Les demandes manuelles restent par tâche. Les accords de préparation autonome,
de publication de son aperçu et de création de contenu GitHub ont désormais un
reçu durable avant l’effet. La validation de fusion du contenu demande une case
nommée et contrôlée côté serveur ; elle conserve la cible, le SHA et les contrôles.
Un simple drapeau `valide_par_client` dans une note ne remplace pas ce reçu.
Une panne de stockage empêche l’envoi et laisse le mode/réglage initial intact.

## Migration et contrôles

Migration additive `20261119_0035` : autorisations et réservations ; aucune
activation historique reconstituée. Montée répétable depuis 0034 et base déjà
créée par les modèles. Descente refusée si elle retirerait des autorisations ou
des exécutions ; suppression du projet retire ces données fonctionnelles et
conserve les reçus. Vérification réelle sur SQLite et PostgreSQL 16.15 jetable.

Les tests de retrait entre deux transports remplacent seulement HTTP par un
fournisseur fictif : les verrous, modèles, signatures, réservations, vérification
et transports Noyaru sont exécutés. Les tests couvrent notamment retrait après
sélection, retrait au retour IA, retrait avant fusion, réactivation, altération
du SHA/droit/contrôle, preuve absente, réponse ambiguë et absence de rejeu.
Les fixtures des tests hérités créent une vraie activation lorsqu’elles veulent
représenter un mode autorisé ; les assertions de refus restent présentes.

Rapport local et empreintes : `MODES-LOCAL-20261119.json`. Les groupes de tests
se recoupent ; ne pas additionner leurs totaux. Les résultats de CI et du pilote
WordPress sur le dernier candidat sont ceux des checks de la PR #95.
La recette Chromium FR/EN, 1440×1100 et 390×844 emploie un compte fictif et ne
fait que des GET locaux ; aucune requête externe, erreur JS ou débordement.
Les quatre captures locales sont archivées avec la note de reprise du complément.
Le point d’entrée éprouvé est `backend.app:app`. L’ancien lancement `app:app`
rencontre déjà une importation relative dans `db.py` sur la base C083 ; il n’est
pas présenté comme validé par ce complément.
