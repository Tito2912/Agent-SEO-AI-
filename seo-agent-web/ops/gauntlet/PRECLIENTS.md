# Recette avant les premiers clients

Etat du 08/10/2026 : pas de certification globale ni de deploiement de cette
branche. Un inventaire, un build ou une baisse de compteur ne suffit pas a
prouver toutes les corrections sur tous les sites.

## Perimetre propose

L'inventaire courant contient 203 cles, dont 131 sans correcteur automatique.
Les 62 familles revendiquees se repartissent entre 47 avec apercu et 15 dont
les corrections exigent un plan de pages verifie et la route etendue.
Les variantes ne sont pas des familles supplementaires. Les conseils manuels,
les erreurs brutes et l'historique des PR doivent rester accessibles.

`python ops/gauntlet/preclients_audit.py --output NEW_TEMP_REPORT.json` genere
la matrice courante dans une base temporaire, sans IA ni acces GitHub. Les
references aux bilans sont des mentions exactes, pas des certificats de succes.
L'inventaire ne teste pas lui-meme les API ou un agent deploye.

## Preuves Acquises

- Politique individuelle : les routes FastAPI authentifiees et la fiche utilisent
  la meme autorisation. Refus sans IA/debit pour les diagnostics manuels, anciens
  apercus et plans forges ; reprise de recus sous une cle non normalisee sans
  second debit. Ce sont des tests SQLite avec GitHub/IA simules et des controles
  navigateur locaux, pas une recette des services deployes. Voir
  [le bilan](validation-correction-capability-policy-2026-10-08.json).
- Verrous et quota : PostgreSQL reel isole, six processus et perte de session
  testes le 02/10. Voir [le bilan](validation-postgres-et-ia-2026-10-02.json).
- Reprise : effets GitHub reels et PostgreSQL isole testes ensemble le 04/10.
  Une PR acceptee est retrouvee sans second debit. Une branche partiellement
  ecrite sans PR est retenue et exige une decision operateur, pas une reprise
  automatique destructive. Voir [le bilan](validation-lots-et-reprise-2026-10-04.json).
- Corrections SEO : builds et recrawls possedes ont mesure des sous-ensembles
  precis. Les cadres partages, sources calculees et cas non prouves ne deviennent
  pas certifies par cumul des bilans. Les descriptions courtes, ancres et groupes
  hreflang gardent notamment des occurrences volontairement refusees.

Le demarrage commun web/worker est couvert par les tests de politique locale
et un banc de vrais processus Linux : deux cycles, SQLite migre et six taches
synthetiques de type inconnu, prises une seule fois. Le worker refuse une cle
stricte invalide avant ses threads ; les schedulers PR/contenu restent sur le
web. SIGTERM intervient apres ces taches, pas pendant un crawl reel. Voir
[le bilan](validation-service-startup-policy-2026-10-08.json). Ce n'est pas une
recette Docker/Render ou une mesure de charge PostgreSQL. Un override de plan
disponible localement n'est pas pour autant partage entre deux machines : leur
coherence et leur persistance doivent etre verifiees sur l'hebergement cible.

La preproduction n'existe pas encore. Une configuration de **nouveau** Blueprint
et [son guide de creation](PREPRODUCTION.md) sont prepares, avec base/secrets
dedies, integrations initialement vides et boucles PR/contenu desactivees.
Le schema officiel Render et les garde-fous ont ete controles localement ;
[le bilan](validation-preproduction-config-2026-10-08.json) distingue ces preuves
d'une creation payante, d'un build Docker et d'une recette sur l'hebergeur.

Les derniers tests SQLite ignores ne retirent pas les preuves PostgreSQL
historiques. Ils ne constituent pas non plus une nouvelle verification de la
configuration, du pool ou de la charge du futur hebergement.

## Conditions Avant Pilote

1. Revoir la branche et valider sa version exacte sur une preproduction isolee :
   demarrage, workers, scheduler, redemarrage, pool PostgreSQL et charge bornee.
   Ne pas fusionner ou deployer sur la seule base de cet inventaire.
2. Fixer la retention des recus, la sauvegarde et la procedure operateur :
   retrouver une PR ou un debit, traiter une branche partielle, revocation de
   jeton, erreur reseau et perte de worker. Ne pas effacer les preuves pour
   debloquer une correction.
3. Faire une recette de compte non administrateur avec les services en mode
   test : connexion GitHub a un depot possede, permissions insuffisantes,
   creation de projet, paiement de test, webhook retarde ou repete, renouvellement,
   changement de plan et indisponibilite du fournisseur. Les stubs actuels ne
   sont pas cette recette integree.
4. Choisir explicitement les frameworks, types de sources et familles admis
   au pilote. Pour chaque parcours : anomalie observee avant, fichier/PR relus,
   build reussi, recrawl apres et controle des autres pages. Limiter les appels
   Claude et les ecritures ; conserver refus, resultats partiels et residus.
5. Lancer un petit pilote supervise avec PR en brouillon, sans fusion automatique,
   journal et surveillance de couts. Ne promettre que le perimetre mesure,
   pas la correction de toutes les anomalies corrigeables.

Aucune preproduction, recette Stripe/GitHub integree, retention en exploitation
ou acceptation par un client reel n'est attestee par cette phase. Aucun site
client ni main ne doit etre modifie par ce bilan.
