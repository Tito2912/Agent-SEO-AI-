# Conservation des accords CMS

Complément C083 du 19 novembre 2026, en branche de travail de la PR #95.
La durée et son point de départ sont confirmés par Tony : **cinq ans à compter de
chaque accord**. Le code de ce lot est distinct de sa mise en production et des
textes publics, qui restent à coordonner avec Claude.

## Preuve conservée

Avant toute écriture manuelle créer/publier et avant un transfert d’image,
Noyaru enregistre l’auteur authentifié, une copie de son e-mail, l’adresse publique
du site, la date, l’échéance, l’opération, les cibles minimales et l’empreinte du
contenu. La preuve ne conserve ni le texte de l’article ni les octets de l’image.
Les libellés des collections, clés, cookies et secrets de prévisualisation ne
font pas partie de cette copie durable. Les références publiques d’une page
PrestaShop peuvent conserver controller=cms et ses identifiants numériques.

Le propriétaire du projet et l’auteur de l’accord sont deux liens distincts. Un
membre d’équipe reste identifié comme l’auteur réel ; son action chez un autre
propriétaire ne lui donne pas accès à l’export de ce propriétaire.

L’accord est validé dans la même transaction que l’intention d’écriture. Une
panne lors de l’enregistrement empêche l’envoi au CMS. L’identité retournée par le
CMS peut être ajoutée ensuite, datée comme **observation**, sans réécrire les cibles
approuvées ni la date d’accord. Une adresse absente peut être complétée lors d’une
relecture ; une identité/adresse déjà connue ne doit pas être remplacée par une
modification ultérieure du CMS. Ce reçu n’est pas une preuve de visibilité publique
ou de résultat SEO.

## Suppression, export et purge

Les liens vers le projet, la publication, l’auteur et le compte propriétaire
passent à NULL lors de leur suppression. Les copies minimales demeurent dans le
registre jusqu’à l’échéance initiale ; les contenus et connexions opérationnels
continuent d’être supprimés normalement. Un transfert média possède sa propre
preuve durable, indépendante de la ligne opérationnelle de transfert.

L’export « Mes données » retrouve les preuves du propriétaire encore actif,
y compris après suppression du projet. Il ne fait jamais de rapprochement par
e-mail et ne suit pas l’auteur vers les projets d’un autre compte. Après suppression
du compte, une nouvelle inscription avec le même e-mail n’hérite pas des preuves.

La purge s’exécute au démarrage du service web puis à chaque cycle de rétention
(par défaut six heures), même si le DPA est masqué et les rétentions de rapports
désactivées. Elle retire les seules preuves arrivées à échéance. Le 29 février
expire le 28 février cinq ans plus tard. Une panne est journalisée sans contenu
privé et sera réessayée au cycle suivant ; elle n’est pas comptée comme une purge
réussie. Les preuves devenues échues peuvent donc rester jusqu’au prochain cycle,
ou plus longtemps en cas de panne à traiter.

## Migration 0034

La migration conserve les preuves existantes, change les deux suppressions en
cascade en SET NULL et ajoute les copies, le lien propriétaire et l’échéance.
Les copies récupérées depuis le profil/site **actuels** portent snapshot_source=
backfilled ; elles ne sont pas présentées comme des copies prises à l’heure de
l’ancien accord. Les dates et empreintes initiales ne sont pas renouvelées.
Les anciennes preuves médias sont reconstituées seulement à partir des valeurs
connues : leur empreinte d’intention est explicitement distinguée d’une empreinte
des octets de l’image. Aucun auteur, date ou contenu manquant n’est inventé.

Une descente qui retirerait des preuves est refusée avant modification. La
descente puis remontée restent possibles sur un registre vide. Un retour vers
un ancien code doit être préparé avec une sauvegarde et une stratégie explicite,
sans effacer les preuves pour contourner ce refus.

## Validation et limites

Les tests contrôlent les contraintes SQLite et PostgreSQL, les copies historiques,
la migration depuis 0033 et depuis les modèles déjà créés, la survie aux suppressions,
l’export isolé, l’absence d’envoi si la preuve ne peut pas être enregistrée, le
29 février, la purge et sa reprise après erreur. Le parcours Ghost réel local
vérifie séparément l’accord média et les deux accords créer/publier.
Voir VALIDATION.md et les rapports C083 pour les résultats réellement obtenus.

Ce lot conserve les accords du parcours CMS natif manuel. Les mandats d’activation
des modes automatiques (fusion, autocorrection, publication planifiée), leur retrait
et les preuves des anciens parcours GitHub nécessitent leur complément distinct.
Aucun automatisme CMS ou réseau social n’est activé par cette migration. Les CGU,
la confidentialité et la politique de restauration des sauvegardes ne sont pas
modifiées ici ; aucune annonce de conformité juridique globale n’est déduite des
tests de code.
