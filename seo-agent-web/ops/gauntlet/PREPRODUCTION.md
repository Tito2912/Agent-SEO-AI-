# Creer la preproduction isolee

Ce guide prepare une nouvelle infrastructure. Aucun service n'est cree par le
commit ; les ressources Render proposees sont payantes. Valider leur cout dans
le tableau de bord avant tout lancement. Une validation YAML n'est pas une
recette de l'application hebergee.

## Premier lancement

1. Dans Render, creer un **nouveau** Blueprint depuis ce depot et la branche
   `codex/corrector-recovery-20261003`. Choisir le chemin
   `render.preproduction.yaml`, pas `render.yaml`.
2. Relire la liste : seulement `noyaru-preproduction-web`,
   `noyaru-preproduction-worker`, `noyaru-preproduction-db` et le groupe
   `noyaru-preproduction-shared`. Refuser toute modification d'un service
   existant. Ne pas importer de dump, disque, compte ou groupe de production.
3. Fournir `BOOTSTRAP_ADMIN_EMAIL` avec l'adresse du proprietaire de recette.
   Les secrets de session, chiffrement, cron, invitation et HTTP Basic sont
   generes pour cette infrastructure. Ne pas reutiliser ceux de production.
4. Avant de confirmer, verifier le cout des deux services, du disque et de la
   base. La base propose PostgreSQL 16, comme les preuves locales historiques :
   verifier la version voulue avant creation, elle n'est pas modifiable ensuite.
   Cette configuration ne garantit pas la capacite ni la parite avec la base
   de production dont la version n'a pas ete mesuree ici.
5. Apres une creation explicitement validee, mettre **Auto Sync = No** dans les
   reglages du Blueprint. `autoDeployTrigger: "off"` coupe les deploiements sur
   commit des services, pas la synchronisation automatique du Blueprint ni le
   premier deploiement. Les futures mises a jour restent manuelles.
6. Relever la version Git effectivement deployee sur le web **et** le worker.
   Ne pas remplacer cette verification par un simple HTTP 200.

Les proprietes et plans sont ceux du
[schema Render](https://render.com/docs/blueprint-spec). Le choix d'un chemin
personnalise et la desactivation d'Auto Sync sont documentes dans
[les Blueprints](https://render.com/docs/infrastructure-as-code).

## Etat initial volontairement ferme

- Meme base **neuve** et meme cle de chiffrement via un groupe dedie aux deux
  services. Aucune reference a la base ou au groupe de production.
- Inscriptions fermees, sauf le premier compte correspondant a l'adresse de
  bootstrap. Recuperer le mot de passe HTTP Basic et le code d'invitation dans
  Render, sans les mettre dans un journal, une URL ou cette conversation.
- `SEO_AGENT_DISABLE_SCHEDULERS=true` empeche le demarrage des boucles PR/contenu
  du web. Il ne suspend pas une boucle deja lancee : redemarrer apres changement.
  Il ne coupe ni le worker, ni les routes cron manuelles, ni une tache deja en file.
  Aucun service cron Render n'est declare ; ne connecter aucun workflow externe.
- `SEO_AGENT_NOINDEX=true` ajoute `X-Robots-Tag: noindex, nofollow` aux reponses
  traitees par l'application. Ce n'est pas une protection d'acces : les pages
  publiques restent lisibles, les inscriptions et HTTP Basic ont leurs roles
  propres. Les robots non conformes peuvent ignorer cette instruction.
- Cles IA, paiement, OAuth, emails et stockage externe initialement vides.
  La recette de ces integrations n'est donc pas possible au premier lancement.
- Aucune purge automatique de jobs/runs. Fixer une retention apres la recette,
  en conservant les recus de correction et leur procedure de reprise.

## Ouvrir la recette par etapes

1. Verifier migrations, logs de demarrage, arret/redemarrage et les deux
   instances. `/healthz` est de la presence HTTP, pas une preuve PostgreSQL ou
   de consommation de file. Mesurer une tache possedee puis une charge bornee.
2. Configurer un bucket S3 **dedie**, avec des droits IAM limites a celui-ci et
   le prefixe `noyaru-preproduction/seo-runs`. Sans S3, le worker sans disque et
   le web ne partagent pas leurs fichiers ; un crawl qui finit ne suffit pas.
   Verifier depuis le web les artefacts produits par le worker, puis apres
   redemarrage. Ne jamais saisir le bucket ou les credentials de production.
3. Configurer les integrations seulement avec des comptes de recette : Stripe
   en mode test, prix et webhook de test ; application OAuth GitHub separee et
   depot possede sans donnees client ; destinataires email limites aux testeurs.
   Pour Claude, utiliser des credentials dedies et autoriser explicitement un
   budget d'appels borne. Rien ici n'active ou ne limite automatiquement ce budget.
4. Creer un second compte **non administrateur**. Ouvrir les inscriptions de
   facon nominative et temporaire avec invitation/liste blanche, puis les fermer.
   Tester droits du projet, permission GitHub insuffisante, paiement, webhook
   repete/retarde, changement de plan et indisponibilite du fournisseur.
5. Garder `PLAN_CONFIG_JSON` dans le groupe de preproduction pour les deux
   services. Mettre a jour/deployer les deux ensemble et comparer les plans
   effectifs apres redemarrage. Un changement dans l'UI ou sur le disque du web
   n'est pas distribue automatiquement au worker.
6. N'activer les schedulers qu'apres cette recette, avec donnees possedees,
   publication manuelle et revue des PR. Mesurer aussi leur comportement et les
   migrations concurrentes ; les tests locaux ne valident pas l'hebergeur.

Conserver SHA deployee, configuration non secrete, resultat avant/apres, debit,
refus et residus. Suivre ensuite [les conditions avant pilote](PRECLIENTS.md).
Ne pas fusionner `main` ou promettre toutes les corrections sur la seule base
de ce fichier. Arreter les ressources de recette devenues inutiles par une
operation explicite dans Render ; supprimer leur YAML ne les supprime pas.
