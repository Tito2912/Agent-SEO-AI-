# Lot 28 — Recette Stripe isolée et langue des écrans hébergés

Base initiale : `1058bb7`, après les lots 26/27. Branche et worktree isolés.
Aucun catalogue, moteur de langues ou capture de Claude modifié.

## Audit avant changement

| Écran / parcours | Preuve | Problème / conséquence | Proposition | Priorité |
| --- | --- | --- | --- | --- |
| Abonnement anglais → Stripe Checkout | Navigateur réel, copie locale de Noyaru et API Stripe de test | Noyaru est en anglais, mais Stripe et son HTML restent en français (`locale=fr`). Rupture de compréhension au paiement. | Transmettre la langue courante de la requête à Stripe. | P2 |
| Gérer l'abonnement → portail Stripe | Code uniquement au constat initial | La langue est aussi imposée au français. | Même règle pour le portail, le parcours de résiliation et son repli. | P2 |
| Choisir Solo sans consentement | Navigateur réel local | La soumission est bloquée avant tout envoi, comme attendu. | Conserver la confirmation obligatoire. | Conforme |
| Annuler Checkout | Navigateur réel Stripe test → Noyaru local | Le retour vers Abonnement confirme l'annulation ; le compte reste Free. | Conserver les droits et le retour explicite. | Conforme |

## Environnement réellement utilisé

Le propriétaire confirme que les essais antérieurs utilisaient les variables
Stripe de test dans l'environnement Noyaru existant. La présente recette utilise
une copie locale de `main`, une base SQLite neuve, un compte fictif, aucun worker,
aucun fournisseur IA et aucun envoi SMTP. Les variables de production sont intactes.

Un bac à sable Stripe temporaire indépendant, sans paiement réel, fournit les
produits/tarifs fictifs et les webhooks signés relayés au serveur local. Le CLI
officiel 1.55.6 est téléchargé dans `/tmp` et son SHA-256 vérifié. Aucun compte
Stripe de production ni donnée client n'est réutilisé. Clés, cookies et URLs de
sessions restent privés et sont exclus des preuves publiables.

Le premier Checkout est bien un abonnement de test, non payé, de 49 EUR. Le
passage en anglais est confirmé dans Noyaru, tandis que le fournisseur conserve
`locale=fr` et son HTML français. Les préférences de facturation du client Stripe
et les montants ne sont pas modifiés par le correctif de langue de l'interface.

## Réalisation et validation

Le changement applicatif se limite à `backend/billing.py` : la langue courante
de Noyaru remplace les trois valeurs françaises imposées, pour Checkout, le
portail et le repli de la résiliation. Aucun tarif, consentement, droit, document
de facturation ou traitement de webhook n'est modifié.

**77 tests ciblés réussis**, dont huit nouveaux cas FR/EN couvrant les trois
appels, le repli du parcours de résiliation, la langue choisie différente de
celle enregistrée sur le compte et les paramètres de paiement conservés.
Les tests existants de consentement, synchronisation et droits passent aussi.

Recette Chromium réelle sur la copie locale, code applicatif `25d94e2` :

| Scénario | Résultat observé |
| --- | --- |
| Noyaru anglais → Checkout | API `locale=en`, HTML anglais, tarif fictif inchangé de 49 EUR. |
| Noyaru français → Checkout | API `locale=fr`, HTML français, consentement initialement décoché et soumission bloquée tant qu'il n'est pas accepté ; quatre largeurs 1 440 / 768 / 390 / 360 px sans débordement. |
| Paiement de la carte de test officielle | Checkout `complete` / `paid`, retour sur Abonnement anglais, formule Solo et quotas de 3 sites, 20 000 pages, 100 corrections et 400 messages. Aucun montant réel prélevé. |
| Carte de test refusée pour fonds insuffisants | Erreur française explicite, session ouverte et impayée, retour Abonnement ; formule Free et quotas gratuits conservés. |
| Événements signés relayés par Stripe CLI | `invoice.paid`, `checkout.session.completed`, `customer.subscription.created`, puis `customer.subscription.updated` reçus ; réponses HTTP 200. Le plan passe à Solo. |
| Abonnement Noyaru FR/EN | Huit rendus à 1 440, 768, 390 et 360 px, aucun débordement. Deux retours du portail vers Noyaru préservent la langue. |
| Portail et résiliation FR/EN | HTML `fr-FR` / `en-US`, date et confirmation dans la langue choisie. Aucun changement avant confirmation. La relecture visuelle repère une limite du portail pendant un redimensionnement, détaillée ci-dessous. |
| Confirmation de résiliation de test | Annulation programmée au 2 décembre ; Noyaru affiche « Cancellation recorded » et « Subscription ends », conserve Solo et ses quotas jusque-là. |
| Nettoyage de l'abonnement fictif | Suppression explicite via API Stripe de test, événement signé `customer.subscription.deleted` accepté ; Noyaru revient à Free (1 site, 800 pages, 2 corrections). L'ancien abonnement reste visible comme annulé. Ce nettoyage ne simule pas l'horloge d'expiration. |

### Contrôle complémentaire de l'affichage du portail

La première mesure de `document.scrollWidth` ne révélait aucun débordement, mais
les captures du portail déjà ouvert à 1 440 px puis réduit à 390 px montrent du
contenu tronqué. L'absence de défilement horizontal ne suffit donc pas à déclarer
ce cas conforme. Ce comportement concerne l'écran hébergé par Stripe ; le
correctif de langue ne modifie pas sa mise en page. Point P2 à conserver pour la
recette de redimensionnement du fournisseur.

Un contrôle indépendant est ensuite exécuté avec un abonnement fictif de test,
la taille fixée **avant** chaque navigation : huit entrées dans le portail FR/EN
à 1 440, 768, 390 et 360 px, puis quatre entrées dans sa confirmation de
résiliation à 390 et 360 px. **12 rendus réussis**, toutes les actions visibles
dans la largeur de l'écran, captures mobiles relues visuellement. Le contenu
chargé et son tarif sont attendus explicitement avant mesure. Ces observations
complémentaires concernent Stripe seul ; les retours et droits Noyaru sont
attestés par la première recette complète ci-dessus. Les ressources fictives et
clés temporaires des contrôles sont nettoyées après chaque essai.

Les preuves sans clés ni URLs de session sont conservées dans le dossier de
recette local. La CI complète et la revue du commit final sont consignées dans
la PR ; aucun déploiement manuel n'est déclenché.

## Limites et coordination avec Claude

- La recette porte sur Noyaru local et un fournisseur Stripe réel en mode test ;
  elle ne prouve pas la configuration Stripe de production, ni le retour depuis
  une autre instance hébergée.
- SMTP est désactivé et l'adresse est fictive. La réception d'une confirmation
  de paiement ou de résiliation n'est donc pas validée. Le journal signale
  explicitement l'absence d'envoi lors de la résiliation.
- Les préférences françaises des factures Stripe sont conservées, conformément
  au comportement existant couvert par les tests. La présente correction
  concerne la langue des écrans hébergés.
- Deux textes résiduels sont réellement visibles au retour anglais du paiement :
  « Paiement confirmé. » (`app.py`, `billing_page`) et « 49€/mois » (libellé du
  catalogue des formules). Ils sont signalés au propriétaire pour le chantier
  de traduction de Claude ; aucun de ses fichiers n'est retouché ici.
- L'expiration à la date future n'est pas accélérée avec une horloge Stripe.
  Le maintien des droits après programmation est vérifié dans le navigateur.
