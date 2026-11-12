# Procédure en cas de violation de données personnelles

Document interne d'E-Com Shop pour le service Noyaru. Version du 12 novembre 2026. Responsable :
le dirigeant d'E-Com Shop. Ce dépôt est public : ce document ne contient aucun secret, et le
registre des violations n'est jamais rangé dans le dépôt.

## Quand l'appliquer

Une violation de données, c'est toute perte de confidentialité, d'intégrité ou de disponibilité de
données personnelles, accidentelle ou non. Exemples :

- accès non autorisé à un compte d'administration : Render, AWS, GitHub, Stripe, Google, Anthropic,
  OpenAI, Brevo, Sentry, Telegram ou le compte propriétaire de Noyaru ;
- clé, jeton ou cookie de session exposé : capture d'écran, message, dépôt, journal ;
- base de données, sauvegarde ou fichiers d'analyse accessibles à un tiers ;
- défaut du code qui montre à un compte les données d'un autre ;
- e-mail envoyé au mauvais destinataire ;
- données effacées par erreur sans sauvegarde utilisable ;
- alerte d'un prestataire, signalement d'un client ou d'un tiers.

Dans le doute, appliquer la procédure : on peut conclure ensuite qu'il n'y avait pas de violation,
et cette conclusion s'inscrit aussi au registre.

## Les délais

Ils courent à partir du moment où E-Com Shop **a connaissance** de la violation, pas de sa
correction.

| À qui | Quand | Pour quelles données | Texte |
| --- | --- | --- | --- |
| Registre interne | Toujours, même sans aucune notification | Toutes | RGPD, art. 33.5 |
| Client professionnel concerné | Dans les meilleurs délais ; objectif 48 heures | Données traitées pour le client : pages de ses sites, dépôt, site connecté, données de recherche | RGPD, art. 33.2 ; DPA, clause 9.2 et clause complémentaire B4, une fois le DPA publié |
| CNIL | 72 heures au plus, sauf si la violation est peu susceptible de créer un risque pour les personnes | Données dont E-Com Shop est responsable : comptes, facturation, support, sécurité | RGPD, art. 33.1 |
| Personnes concernées | Dans les meilleurs délais, si le risque est élevé | Données dont E-Com Shop est responsable | RGPD, art. 34 |

Pour les données traitées pour un client, c'est le client qui notifie la CNIL et informe les
personnes. E-Com Shop lui donne les éléments pour le faire (clause 9.1 du DPA).

Si tout n'est pas connu dans les 72 heures, notifier quand même ce qui l'est, puis compléter :
c'est prévu par l'article 33.4.

## Les étapes

### 1. Noter

Noter la date et l'heure de la découverte, et ouvrir une ligne au registre. C'est cette heure qui
fait courir les délais.

### 2. Contenir

Agir dans l'heure si possible. Selon le cas :

- **Clé ou jeton exposé** : le révoquer chez le prestataire, en créer un nouveau, le remplacer dans
  Render (Environment du service concerné), puis vérifier que le service redémarre.
- **Compte d'administration compromis** : changer le mot de passe, fermer les sessions ouvertes,
  vérifier la double authentification et lire le journal des connexions du prestataire.
- **Cookie de session Noyaru exposé** : une déconnexion ne l'annule pas. Une session est un jeton
  signé, valable 30 jours, dont le serveur ne garde pas trace. Pour toutes les fermer :
  1. obtenir l'heure actuelle en secondes Unix (PowerShell : `[DateTimeOffset]::UtcNow.ToUnixTimeSeconds()`) ;
  2. la mettre dans `SEO_AGENT_SESSIONS_NOT_BEFORE` sur le service web Render.

  Chacun se reconnecte, rien d'autre ne change.
  **Ne pas changer `SEO_AGENT_SECRET_KEY` ni `SEO_AGENT_ENCRYPTION_KEY`** : elles chiffrent aussi
  les jetons confiés par les clients, qui deviendraient illisibles.
- **Défaut du code** : revenir au déploiement précédent (Render → service → Events → Rollback), ou
  couper la fonction concernée, puis corriger.
- **Incident chez un prestataire** : suivre ses consignes. La date de connaissance est celle de
  la réception de son avis.

### 3. Évaluer

- Quelles données ? Combien de comptes, de clients et de personnes, au moins approximativement ?
- Depuis quand, et l'accès est-il fermé ?

Sources : journal d'audit (Exploitation), journaux Render, Sentry, journaux du prestataire.

Apprécier ensuite le risque pour les personnes :
- les mots de passe ne sont stockés que sous forme d'empreinte ;
- les jetons des clients sont chiffrés, sauf si la clé de chiffrement a fuité aussi ;
- le contenu des sites analysés est le plus souvent public ;
- une adresse e-mail seule présente un risque faible ; associée à d'autres données, il augmente.

### 4. Prévenir

- **Clients concernés** : e-mail à l'adresse du compte propriétaire. Il contient au moins :
  - la nature de la violation, avec si possible les catégories et le nombre approximatif de
    personnes et d'enregistrements concernés ;
  - le contact de suivi : contact@noyaru.com ;
  - les conséquences probables ;
  - les mesures prises ou proposées.

  Si des jetons ou des clés confiés par le client ont pu être lus, lui demander de les révoquer
  chez GitHub, Google, son CMS, etc.
- **CNIL** : téléservice https://notifications.cnil.fr/notifications/index. Guide :
  https://www.cnil.fr/fr/notifier-une-violation-de-donnees-personnelles
- **Personnes concernées**, si le risque est élevé : e-mail clair, avec les conséquences
  possibles et ce qu'elles peuvent faire, par exemple changer leur mot de passe.

### 5. Corriger et vérifier

Corriger la cause, la couvrir par un test, déployer, puis vérifier en production que l'accès est
fermé.

### 6. Clore

Compléter la ligne du registre : faits, effets, mesures, décision de notifier ou non, et pourquoi.

## Le registre

À tenir dans un fichier privé, jamais dans le dépôt, même quand aucune notification n'est faite.
Une ligne par incident :

| Colonne | Contenu |
| --- | --- |
| Découverte | Date et heure ; qui ou quoi l'a signalée |
| Incident | Date et heure de début, si connues ; description |
| Données et personnes | Catégories ; nombre approximatif de personnes et d'enregistrements ; clients concernés |
| Conséquences | Probables et constatées |
| Mesures | Prises pour contenir et corriger ; date de fermeture de l'accès |
| Clients informés | Date de l'e-mail, ou « non concerné » |
| CNIL | Date et numéro de notification, ou raison de ne pas notifier |
| Personnes informées | Date et moyen, ou raison de ne pas les informer |
| Clôture | Date ; ce qui a changé pour éviter que cela se reproduise |
