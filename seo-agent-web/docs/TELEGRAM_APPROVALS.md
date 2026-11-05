# Validations du propriétaire dans Telegram — 5 novembre 2026

## Bilan de départ (code examiné, aucune recette Telegram réelle à ce stade)

| Écran / étape | Problème | Conséquence | Changement | Priorité |
| --- | --- | --- | --- | --- |
| Système / Telegram | Notifications sortantes uniquement | Le propriétaire doit revenir dans Noyaru pour décider | Conversation privée liée au propriétaire et boutons de décision | P1 |
| Veille SEO/GEO | Résumés FR/EN à approuver dans l’administration | Une source modifiée peut rester sans relecture | Présenter la version et les deux résumés dans le bot | P1 |
| Référencement de Noyaru / briefs | Relecture limitée à l’application | Décisions dispersées | Brief complet et preuves, validation ou abandon | P1 |
| Brouillons privés | Autorisation IA et relecture sur deux écrans | Coût ou portée de l’accord faciles à confondre | Deux demandes distinctes : autoriser la rédaction, puis relire le résultat | P1 |
| Publications, SAV, réseaux | Exécuteurs propriétaires encore à construire | Une notification ne suffit pas à rendre ces actions autonomes | Réutiliser le registre de validations lors de leur livraison | Étapes suivantes |

## Parcours et règles

Système → Validations Telegram → activer la conversation privée déjà configurée → recevoir
une demande datée → lire toutes ses pages → approuver ou refuser → confirmer l’action précise
→ obtenir le résultat. Une validation de brouillon ne publie rien. Une autorisation de rédaction
autorise un appel IA payant au maximum, avec le plafond existant de deux appels sur 24 heures.

Le jeton du bot reste dans la configuration serveur. Le lien est invalidé si le bot, le chat ou
le propriétaire change. Un groupe Telegram ne peut pas approuver. Les boutons expirent après
24 heures et sont liés à la version relue. Les actions sont revérifiées et enregistrées dans
la même transaction que la décision ; les doubles clics ne répètent pas l’effet.

La réception utilise getUpdates et un bail en base partagé entre processus. Elle ne remplace
pas un webhook existant. Le curseur avance après traitement ; une reprise peut relire une mise
à jour sans répéter la décision. Les messages externes et les commandes libres ne deviennent
jamais des instructions d’exécution. Les demandes et refus restent consultables dans Noyaru.
Une livraison dont le résultat réseau est inconnu n’est pas renvoyée automatiquement.

Les cartes sont paginées, les libellés FR/EN sont explicites, les actions positives ont une
confirmation propre et la page web utilise les composants et protections CSRF existants.
Les droits des clients sur leurs sites restent inchangés : ce canal concerne le propriétaire
de Noyaru et ses seuls objets internes. Aucune donnée de ticket SAV client n’y est transmise
par ce premier lot.

Références techniques : [Bot API](https://core.telegram.org/bots/api#getupdates),
[boutons et réponses aux clics](https://core.telegram.org/bots/api#callbackquery),
[FAQ officielle](https://core.telegram.org/bots/faq). Telegram limite callback_data à 64 octets ;
le bot doit répondre aux clics et getUpdates ne fonctionne pas avec un webhook actif.

## Activation et exploitation

Les migrations ne donnent aucun mandat et n’activent pas le canal. Le propriétaire l’active
dans Système avec le bot existant, puis peut envoyer une demande de test sans effet métier.
`SEO_AGENT_DISABLE_TELEGRAM_RECEIVER=true` arrête la réception (notamment en tests). La pause
depuis l’interface invalide les boutons ; les validations restent accessibles sur le web.
Les erreurs affichées sont des codes maîtrisés, sans URL API ni secret.

La recette réelle doit distinguer réception d’une carte, clic humain et décision persistée.
Les simulations locales ne prouvent pas la livraison Telegram réelle.

## Vérifications locales

- Régressions initiales : 187 tests réussis (Telegram, veille, briefs, rédaction, anciennes notifications).
- Cas Telegram supplémentaires : refus persistant, rollback commun, bail et curseur, reprise d’un envoi
  interrompu, conflit de récepteur, changement de bot, changement de preuves entre lecture et accord,
  reprise d’une rédaction déjà autorisée après arrêt du processus.
- Navigateur Chromium local : 60 combinaisons FR/EN, 1440/1024/768/390/360 px, états en attente,
  livraison inconnue, approuvé, refusé, expiré et pause ; quatre formulaires natifs avec CSRF.
  Aucun débordement horizontal, erreur JavaScript ou appel externe réel. Captures inspectées.
- Migration aller/retour sur base vierge et précréée ; aucun canal actif ni demande créée.
- La recette Telegram réelle et la vérification de production sont consignées séparément après fusion.
