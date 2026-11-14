# Studio éditorial natif — premier lot

14 novembre 2026, base `main 4cfb821`. Tony souhaite une stratégie propre aux réseaux,
sans transformer systématiquement les articles du blog. Ce lot est une préparation
éditoriale privée ; les comptes et transports sociaux restent à connecter.

## Constat et parcours

| Écran / état examiné dans le code | Problème | Conséquence | Réponse du lot | Priorité |
| --- | --- | --- | --- | --- |
| Système | Le marketing social n’a pas d’entrée dédiée | Le propriétaire passe par le blog même pour un sujet natif | Lien « Studio réseaux sociaux » | Haute |
| Référencement propre | Un brief prépare surtout du texte long | Risque d’adaptation systématique, sans objectif social | Accès distinct au studio, sans projet client ni brief de blog requis | Haute |
| Choix éditorial | Aucun public, objectif ou format social explicite | Propositions difficiles à comparer | Réseau, objectif et public dans l’URL ; petit ensemble de pistes avec motifs | Haute |
| Production | Un article ne fournit pas un carrousel ou un déroulé vidéo | Réécriture et réalisation implicites | Accroche et quatre cartes Instagram ou trois scènes TikTok, texte d’accompagnement et consignes | Haute |
| Sources périmées ou différentes | Les exemples figés peuvent survivre à leurs preuves | Affirmations dépassées | Mise à l’écart, motif visible ; export refusé pour une ancienne version | Haute |
| Actualités / témoignages / tendances | Aucun relevé ou droit établi | Contenu inventé ou opportunisme sans rapport avec le public | Raisons explicites de ne pas publier ces sujets | Haute |
| Résultats sociaux | Aucun compte ni mesure sociale reliés | Confusion entre vues et acquisition | Indicateurs à observer proposés, aucune statistique inventée | Moyenne |

Parcours : **intention → idée native → dossier de réalisation**. Les liens entre
la liste, le dossier et l’export gardent réseau, objectif, public et langue. Aucun
formulaire ne demande un article. Le blog reste une lecture facultative pour le
seul sujet correspondant, si sa traduction existe réellement.

Les cinq pistes de départ sont des choix éditoriaux relus avec le code, pas une
analyse automatique des tendances ou de la demande. L’ordre est déterministe et
expliqué. Le public adapte le cadrage ; il ne repose pas sur un profilage d’utilisateur.
Ce lot ne prétend ni explorer les réseaux, ni apprendre de performances encore absentes.

## Périmètre et fiabilité

- Écran et téléchargement réservés au propriétaire système, y compris face à un
  autre administrateur. Ni liaison projet, ni contenu client, ni donnée de SAV/Gmail.
- Lecture de la documentation publique de cette version et des seuls états de la
  veille officielle. Aucun appel distant, modèle IA, média ou publication.
- Les exemples produit correspondent aux empreintes des six éditions FR/EN relues.
  Une modification de document exige la relecture du texte proposé et l’actualisation
  de son empreinte dans une livraison. La présence d’une page ne certifie pas son propos.
- Les contenus SEO/GEO exigent des sources actuelles, relues, avec le résumé exact
  utilisé lors de la conception des messages. Un nouveau résumé exige une relecture
  des messages, même si la nouvelle source est approuvée dans la veille.
- L’export recalcule la disponibilité et l’empreinte du dossier ; changement de
  réseau, objectif, public, langue, message ou référence → nouvelle version requise.
  Une simple nouvelle date de consultation de la même source ne l’invalide pas.
- Aucune nouvelle table, migration, tâche de fond, dépense ou validation Telegram.
  L’export est un dossier daté de préparation, pas un accord de publication.
- Aucune programmation durable, production de médias, mesure sociale ou intégration
  n’est activée. Les dossiers restent à produire et à relire avant diffusion.

## Présentation commune

Identité sombre Noyaru, accent vert, trois étapes, formulaires avec labels explicites,
actions d’au moins 44 px et focus visible. Les détails de preuve et motifs secondaires
sont repliés. Les déroulés restent lisibles à quatre largeurs ; aucune couleur seule
ne porte l’état. Navigation et téléchargement fonctionnent sans JavaScript.

## Plateformes, vérification du 14 novembre 2026

Ces contraintes servent à choisir le futur transport, pas à prétendre qu’il est connecté :

- [Instagram Content Publishing](https://developers.facebook.com/docs/instagram-platform/instagram-api-with-instagram-login/content-publishing) :
  publication sur comptes professionnels avec accès et autorisations appropriés.
- [TikTok Content Sharing Guidelines](https://developers.tiktok.com/docs/en/content-sharing-guidelines) :
  Direct Post exclut les utilitaires pour les seuls comptes de leur propre équipe.
  Choisir une remise de contenus ou un service compatible ; revérifier ses capacités
  et les exigences de prévisualisation/consentement avant de connecter les comptes.

Les recommandations de format de ce studio sont des choix éditoriaux de départ,
pas des exigences d’algorithme ni des promesses de portée. Les plafonds et specs
d’un futur transport devront être lus depuis sa documentation et ses réponses.

## Validation

Tests ciblés : droits, absence d’effets, sélection par objectif, formats natifs,
sources anciennes/modifiées/retirées, éditions produit, langues, contexte, exports
et indisponibilité de la base : 99 cas ciblés réussis, dont 21 nouveaux. Après les
ajustements du raccordement et de la récupération d’export, les 21 cas du studio
ont été repassés avec succès. Compilation, Ruff F/E9 et Bandit medium/high verts.
CI complète à constater dans la PR du lot.

Recette Chromium locale reproductible :
`docs/ux-social-editorial/browser_smoke.py`. Comptes fictifs, services bloqués,
scènes normales, références anciennes, absence de pistes et erreur ; FR/EN,
1 440/768/390/360 px. Résultats dans `ux-social-editorial/browser-results.json`.
50 observations réussies, dont deux parcours sans JavaScript ; captures relues
localement, aucun débordement global ni erreur JavaScript. Aucun accès privé de production
ou compte Instagram/TikTok réel utilisé ; la recette réelle reste à effectuer
par le propriétaire avec les futurs médias/comptes.
