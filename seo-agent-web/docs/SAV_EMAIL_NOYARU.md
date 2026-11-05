# SAV email de Noyaru : périmètre et parcours

Audit établi le 5 novembre 2026 depuis `main` `033648d`. Lecture du code ; les recettes
réelles et simulées sont distinguées dans la PR. Claude conserve les intégrations CMS.

| Écran / étape | Constat de départ | Conséquence | Évolution P1 |
| --- | --- | --- | --- |
| Contact public | Lien mailto vers contact@noyaru.com | Aucun suivi SAV dans Noyaru | Conserver cette adresse et raccorder une réception dédiée |
| Connexions Google | Connexions clientes pour Search Console | Elles ne constituent pas un mandat de lecture du SAV | Identifiants OAuth et jetons SAV séparés, compte Gmail attendu vérifié |
| Réception | Aucun ticket ni curseur entrant | Doublons, demandes perdues, absence de supervision | Import borné des nouveaux messages d'un libellé SAV, journal et dédoublonnage |
| Réponse | Envoi transactionnel commun sans reçu de fil SAV | Une réponse réseau perdue ne prouve ni échec ni livraison | Brouillon sourcé, accord versionné, intention d'envoi durable et reçu fournisseur |
| Telegram | Connaissances, rédaction et blog | Aucune décision SAV raccordée | Dossier de rédaction puis réponse exacte à approuver/refuser |

Le parcours cible : connecter le compte Gmail dédié → choisir et confirmer son libellé SAV
→ importer les nouveaux messages → examiner le dossier réduit et ses sources → autoriser
la rédaction → relire la réponse et le destinataire dans Telegram → autoriser un envoi
unique → distinguer acceptation du fournisseur et livraison → suivre ou escalader.

La permission Google `gmail.readonly` porte sur la boîte ; un libellé est un filtre imposé
par Noyaru, pas une restriction OAuth fournie par Google. L'écran l'explique avant accord.
Un compte Gmail dédié évite de donner accès à une boîte personnelle. Ni Search Console,
ni les jetons Google des clients, ni la boîte de test ne sont réutilisés implicitement.

L'import ne parcourt pas les anciens messages. Il part d'un curseur confirmé à l'activation,
contrôle le compte et le libellé, lit d'abord une structure sans données de pièces jointes
et n'importe que des emails texte admissibles. Les autres demandes passent en examen humain.
Les messages, expéditeurs et liens entrants sont des données non fiables, pas des mandats.
Aucune lecture de site client, remboursement, modification de compte ou envoi à une adresse
choisie par un modèle n'est disponible depuis le SAV.

Les dossiers conservés sont réduits et chiffrés ; les secrets courants et l'historique cité
sont retirés avant IA. Cela ne garantit pas l'anonymat : le dossier exact reste à relire.
Les sources sont la documentation publique actuelle, jamais des faits de compte inventés.
Les coûts de rédaction concernent Noyaru et pas le quota SEO du correspondant.

Les écrans reprennent les cartes, états textuels, focus visibles, détails et cibles de 44 px
de l'espace propriétaire. Prévoir FR/EN, petits écrans, pause, état vide, refus, panne,
réponse incertaine et accès client interdit. Les changements partagés de traduction et les
écrans CMS restent hors de ce lot.

Références : [permissions Gmail](https://developers.google.com/workspace/gmail/api/auth/scopes),
[filtres de messages](https://developers.google.com/workspace/gmail/api/guides/filtering),
[OAuth serveur](https://developers.google.com/identity/protocols/oauth2/web-server).

## Mise en service

1. Propriétaire : Système → SAV email de Noyaru. Une migration ne crée aucune connexion.
2. Utiliser une Gmail dédiée recevant les demandes à `contact@noyaru.com`, créer son libellé
   `Noyaru-SAV` et un filtre pour ces demandes. Aucun filtre Gmail n'est écrit par Noyaru.
3. Dans un projet Google Cloud **distinct** de celui des clients, activer Gmail API et créer
   un client OAuth Web. L'adresse de retour en production est
   `https://noyaru.com/settings/system/support/oauth/callback`. Le formulaire explique l'accès
   à toute la boîte autorisé par Google et la restriction de requêtes appliquée par Noyaru.
   Le compte doit être autorisé dans la configuration du projet Google ; la validation Google
   et les durées de jetons dépendent de cette configuration. Aucun secret dans le chat.
4. Saisir les identifiants dans le formulaire, choisir cette Gmail dans OAuth, puis activer
   explicitement la réception. Le premier curseur part de maintenant, sans importer le passé.
5. Le transport Brevo, Resend ou SendGrid existant convient si son expéditeur est exactement
   `contact@noyaru.com`. Sinon, enregistrer une clé SAV séparée ; les paramètres transactionnels
   existants restent intacts. Vérifier le domaine/adresse chez le fournisseur. Pas de SMTP de repli.
6. Avant le pilote client, envoyer une demande témoin convenue à `contact@noyaru.com`,
   examiner son dossier Telegram, autoriser éventuellement l'unique rédaction, puis approuver
   la réponse exacte. Vérifier sa réception dans la boîte témoin.

Plafonds glissants : 20 messages importés, 2 demandes IA, 10 intentions d'envoi sur 24 h.
Les accords en attente réservent aussi le budget. Un refus d'envoi conserve le brouillon,
à modifier explicitement avant une nouvelle carte. Une interruption après intention d'envoi
reste incertaine : contrôler le fournisseur/Gmail, aucun second essai ni repli automatique.
L'acceptation API ne confirme ni livraison, ni résolution du dossier ; pas de suivi de
livraison par webhook dans ce lot. Brevo ne garantit pas le regroupement dans le fil via
cette API. Les demandes sensibles, pièces jointes et formats hors périmètre passent à un humain.

## Vérifications de ce lot

- Recette locale Chromium : 110 cas FR/EN, 1440/1024/768/390/360 px, 3 soumissions natives
  (révision manuelle, pause, reprise), sans débordement, erreur JS ou requête extérieure.
- Les contrôles SAV couvrent OAuth/PKCE et accord à usage unique, mauvais compte/permission,
  dédoublonnage/pagination, pièces jointes, secrets, changements avant envoi, refus Telegram,
  intentions durables, résultat incertain, plafonds et conservation de 30 jours.
- Migration testée sur base vierge et tables précréées ; répétition sans activation,
  retour à la migration précédente en conservant utilisateurs, blog et lien Telegram.
- Gmail, IA, Telegram et emails **simulés** dans les tests. Aucun email SAV réel lu ou envoyé
  pour cette validation ; connexion réelle et réception témoin restent à vérifier.
