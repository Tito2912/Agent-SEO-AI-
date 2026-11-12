# SAV : reprendre un message et renouveler la connexion Gmail

Lot du 12 novembre 2026, base initiale `main` `6cdd60d`, actualisée sans conflit
sur `0dede57` avant publication. Les nouveaux contrôles utilisent
des données fictives et des fournisseurs simulés. Aucune session administrateur
de production n'est utilisée, conformément à la décision de Tony relayée en C002
du journal partagé. Les fichiers juridiques et de sauvegarde de Claude sont préservés.

## Bilan avant modification

| Écran / étape | Constat et preuve | Conséquence | Évolution P1 |
| --- | --- | --- | --- |
| Réception d'un message classé spam | Pilote réel des 6–7 novembre, puis code : le même identifiant Gmail reste dédoublonné après reclassement | Une demande légitime écartée ne revient pas dans la file | Relecture explicitement autorisée d'un message connu, avec les mêmes contrôles d'admissibilité |
| Motif de refus | Code : `outside_label` couvre aussi Spam, Trash et les dossiers sortants | « Hors du libellé » donne une explication incomplète | Motif compréhensible et étapes de tri dans Gmail |
| Accès Google expiré | Reproduction locale d'un `invalid_grant` : erreur générique de fournisseur | La réception peut rester affichée active et retenter un accès refusé | Pause, motif précis, conservation des demandes et du point de suivi, reconnexion visible |
| Compte enregistré | Code : présence d'un refresh token assimilée à « Compte Google relié » | Un accès enregistré peut être pris pour une connexion effectivement utilisable | Distinguer accès enregistré et réception activée ; vérifier l'accès à chaque relevé |
| Aide à la connexion durable | Configuration initiale en mode Google Testing rapportée pendant le pilote, statut actuel non vérifié | Renouvellements manuels potentiellement fréquents | Explication du statut Google et des étapes opérateur, sans déduire le statut depuis un jeton |

## Parcours de reprise

1. Le propriétaire examine l'original dans Gmail et corrige son classement s'il
   s'agit d'une vraie demande SAV. Aucun filtre universel anti-spam n'est proposé.
2. Dans la carte écartée, il coche une confirmation puis demande la relecture.
   Cette action ne porte que sur l'identifiant déjà enregistré, jamais une recherche
   dans l'historique ni un identifiant transmis librement par le formulaire.
3. Le serveur vérifie le propriétaire, la liaison active, la conservation, la copie
   initiale vide et l'absence de brouillon, d'appel IA ou de tentative d'envoi.
4. Le lecteur Gmail habituel vérifie compte, permission readonly exacte, libellé,
   exclusions Spam/Trash/Sent/Draft/Chat, destinataire SAV, expéditeur et anti-boucle,
   période initialement autorisée, taille et structure MIME. Le corps n'est lu
   qu'après les contrôles de métadonnées ; aucune pièce jointe n'est récupérée.
5. Les preuves et la liaison sont relues après les appels réseau. Le même ticket
   reçoit sa copie réduite chiffrée, sa vraie conversation et une nouvelle révision.
   Sa date de création n'est pas modifiée : la conservation n'est pas prolongée.
6. Une demande admissible revient à « Rédaction à autoriser ». Une pièce jointe ou
   une demande sensible exige toujours une reprise humaine. Aucun appel IA ni
   email n'est exécuté par cette reprise ; les décisions Telegram suivantes restent
   distinctes et explicites.

Le bail Gmail existant sérialise relevé et reprise. Un double clic, un état changé,
une pause, une ancienne liaison ou une tentative d'envoi interdisent le rejeu.
Les reprises réussies sont auditées sans coordonnées ou contenu d'email et partagent
le plafond de 20 imports/reprises sur 24 h glissantes. Les lectures utilisent aussi
le plafond Gmail existant. Un message disparu n'invalide pas le suivi des autres
messages ni le curseur d'historique.

## Connexion expirée et intervention opérateur

- Un refus `invalid_grant` du renouvellement demande une nouvelle autorisation.
  Un client OAuth refusé demande de vérifier la configuration du projet dédié.
  La réception est suspendue ; aucune boucle de reconnexion n'est déclenchée.
- Les erreurs transitoires ne sont pas présentées comme une révocation.
- Le passage d'une réception active à une pause pour un problème de connexion
  est audité. Une alerte Telegram sans données d'email est tentée une fois, via le
  transport existant, seulement pour la conversation propriétaire déjà activée et
  liée à la configuration courante. C'est un envoi best effort : ni une preuve
  de réception, ni une file de notification avec reprise après crash.
- La carte ouvre directement la reconnexion au lieu de proposer une reprise avec
  un ancien accès invalide. Après consentement sur le même compte, le propriétaire
  réactive la réception. Le suivi précédent est conservé si son curseur est encore
  valide ; la procédure existante d'expiration du curseur reste applicable.

Google indique qu'un projet externe en **Testing** délivre pour Gmail un refresh
token expirant après **7 jours**. Le statut de publication n'est pas déductible du
jeton reçu par Noyaru. Voir la [documentation Google sur l'expiration](https://developers.google.com/identity/protocols/oauth2#expiration).

Pour terminer la configuration réelle, le propriétaire doit vérifier le statut du
projet Google Cloud SAV et les exigences Google applicables avant une nouvelle
connexion. Aucun changement de ce statut, aucune permission supplémentaire, aucune
rotation ou suppression de secret Google n'est réalisé par ce lot. Le code ne peut
pas réactiver un accès déjà expiré en changeant son seul affichage.

## Validation et limites

- 184 tests ciblés SAV/Telegram réussis, dont 47 nouveaux cas : contrôles des
  permissions et formulaires HTTP/CSRF, absence d'envoi/IA, budgets, concurrence,
  pause et changement de révision pendant la lecture, conservation, exceptions
  MIME, demandes sensibles, messages manquants, erreurs Google, rotation d'accès,
  reconnexion et alerte propriétaire unique.
- Chromium local, vrais gabarits/routes et formulaires natifs, base SQLite fictive
  et fournisseurs simulés : 34 observations FR/EN, 360/390/768/1024/1440 px,
  consentement requis, erreur de classement, reprise réussie, reconnexion et longs
  intitulés ; aucun débordement ni erreur JavaScript. Rapport :
  [browser.json](sav-reliability/browser.json). Recette reproductible :
  [browser_smoke.py](sav-reliability/browser_smoke.py).
- Deux rendus examinés visuellement : reconnexion FR sur ordinateur et reprise EN
  sur mobile. Les captures sont des données de démonstration, pas des comptes clients.
- Compilation, Ruff F/E9, Bandit medium/high et diff-check locaux réussis. La CI
  complète et la relecture du diff doivent réussir avant fusion.
- Cette recette ne prouve ni l'état actuel du projet Google réel, ni la réception
  de l'alerte dans le bot réel, ni le classement futur de tous les emails clients.
  Ces vérifications opérateur restent distinctes ; aucune recette authentifiée de
  production n'est revendiquée.

Références techniques : [lecture d'un message Gmail](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/get),
[historique et curseur Gmail](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.history/list).
