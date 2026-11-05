# Rédaction sourcée de Noyaru — lot 4B1

Base : `main` `b8a83b0`, 4 novembre 2026. Claude conserve les CMS, traductions et captures.
Ce lot livre la préparation privée et la relecture ; la publication native du blog
reste une étape distincte, avec son propre mandat et ses preuves de déploiement.

## Bilan avant implémentation

| Écran/parcours | Constat dans le code | Conséquence | Proposition / priorité |
| --- | --- | --- | --- |
| Référencement de Noyaru | Brief relu et preuves enregistrés ; Contenu reçoit seulement le sujet | Les preuves ne suivent pas le rédacteur | Dossier serveur fermé de sources validées, P1 |
| Contenu | Génération pour les projets clients, aperçu exportable ou PR brouillon | Pas de mandat dédié au blog de Noyaru | Brouillon privé séparé, aucun droit GitHub, P1 |
| Budget de rédaction | Administrateurs exemptés des quotas commerciaux | Une tâche système doit avoir ses propres bornes | Deux demandes fournisseur sur 24 h glissantes, une par autorisation, P1 |
| Réponse IA | Objet JSON ne garantissant pas la structure ou la preuve des affirmations | Références inventées, contenu non vérifié | Validation des champs et références, rendu échappé ; relecture factuelle explicite, P1 |
| Interruption/reprise | Pas d'opération éditoriale durable propre | Un redémarrage pourrait doubler un appel | File durable, bail, idempotence, aucun repli après appel incertain, P1 |

Ces constats sont un examen du code. Les recettes locales utilisent des fournisseurs
simulés ; aucune génération réelle n'est autorisée par l'installation du lot.

## Parcours et limites

Brief actuel relu → dossier des sources → autorisation d'un brouillon → rédaction
en arrière-plan → contrôle de forme et des identifiants de sources → relecture
des affirmations → export historique daté. Pause, révocation et invalidation de
la liaison sont relues avant l'appel et avant de retenir son résultat.

Le dossier transmet le sujet et la langue, les résumés SEO/GEO actuellement approuvés
et quelques faits produit publics sélectionnés côté serveur. Il ne transmet pas les
mesures brutes Google/Bing, un rapport client, des clés ou les identifiants du compte.
Une recherche observée n'est pas une preuve publique à citer dans l'article.

Les données restent dans le message de données ; les consignes d'exécution restent
séparées. La sortie décrit des paragraphes et leurs références dans un schéma fermé,
validé côté serveur. Aucun outil, HTML, lien libre, image ou commande issus du modèle
n'est exécuté. Une citation syntaxiquement valide n'est **pas** un contrôle factuel :
la personne doit vérifier la portée et le soutien de chaque affirmation avant relecture.
Ces choix s'appuient sur la documentation officielle
[OpenAI Docs — sécurité des agents](https://developers.openai.com/api/docs/guides/agent-builder-safety),
consultée le 4 novembre 2026 ; ils ne constituent pas une garantie contre toute erreur.

Une autorisation vaut pour un dossier précis et expire après 24 heures. Elle ne
permet qu'une requête fournisseur de texte, entrée et sortie bornées ; aucune
relance ou substitution automatique après un résultat incertain. Un plafond serveur
de deux requêtes par propriétaire sur 24 heures glissantes s'applique aussi aux
administrateurs ; les appels commencés restent comptés même si leur résultat est
rejeté. Ce plafond porte sur des appels et tokens, pas sur une somme en euros.
Les tarifs et le fournisseur configurés déterminent le coût réel.
Le contexte du suivi de coût est fixé explicitement au propriétaire et à
`own_seo_writing`, puis restauré. Une reprise par cron ne dépend pas d'une session
web et ne doit jamais imputer ce coût à un autre compte.

La reprise ne prend que les opérations déjà autorisées ; aucun brief n'est sélectionné
ou autorisé par le cron. Une réponse interrompue après le début de l'appel est
signalée comme incertaine et n'est pas régénérée automatiquement. Les erreurs de
forme ou sources insuffisantes demandent une intervention, pas une boucle d'appels.
Un export périmé conserve son état et ses dates et ne vaut pas permission de publier.

## Règles visuelles

Réutiliser le cadre Référencement de Noyaru, boutons de 44 px, focus visible,
cartes et paragraphes lisibles, références à côté de chaque bloc, FR/EN, aucune
couleur seule pour un statut. Séparer « références présentes » et « affirmations
relues ». États initial, sources indisponibles, file, rédaction, brouillon, rejet,
résultat incertain, preuves changées, annulé et relu doivent indiquer la suite possible.

## Vérifications locales

- 39 nouveaux cas : transport Anthropic/OpenAI simulé, idempotence, concurrence,
  révocation, source/liaison changée, bail expiré, plafond glissant, schéma, relecture,
  CSRF/droits, cron et migration additive `20261104_0025` avec retour arrière.
- 146 régressions briefs, veille, publication et accès ; inventaire des sept nouvelles
  routes et tests des tâches planifiées également exécutés.
- Recette sur serveur HTTP local et Chromium FR/EN, 1440/1024/768/390/360 px ;
  formulaires natifs jusqu'à la relecture et l'abandon. Les fournisseurs sont simulés,
  les accès externes bloqués. Les captures et résultats restent hors du dépôt.
- Compilation, Ruff F/E9 et Bandit medium/high. La CI complète et les preuves de
  consultation en production sont consignées dans la PR de livraison.

La migration n'autorise ni ne lance une rédaction. Une consultation réelle de
l'état initial en production ne vaut pas recette d'un appel IA payant ; aucun
crédit, nouvelle analyse, email ou publication réelle n'est utilisé pour cette recette.
