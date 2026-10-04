# Parcours d'obstacles du correcteur

Un banc de non-régression : **31 pages, une anomalie chacune**, dans le dépôt fixture
`pployeraffiliation-a11y/noyaru-stack-static-html` (branche `main`).

Il existe parce que 34 familles revendiquées par le correcteur ne s'étaient produites sur aucun
site réel : elles ne reposaient que sur des tests unitaires — exactement le statut qu'avaient les
familles dans lesquelles huit défauts ont été trouvés le 08/09/2026. Attendre qu'un client les
porte n'était pas un plan.

## Règle absolue

**Une branche de correction ne doit JAMAIS être fusionnée dans `main`.** Le parcours doit garder
ses défauts pour resservir. Les PR ouvertes pour obtenir une prévisualisation Netlify se ferment
sans fusion.

## Rejouer le banc

```
# 1) crawl de reference (le parcours, defauts intacts)
python skills/public/seo-autopilot/scripts/seo_audit.py \
  https://noyaru-stack-static-html.netlify.app/ --max-pages 60 --workers 3 \
  --output-dir <workdir>/before

# 2) le correcteur, toutes familles, sur UNE branche
GAUNTLET_WORKDIR=<workdir> FIXTURE_TOKEN=<PAT> python seo-agent-web/ops/gauntlet/run.py

# 3) ouvrir une PR pour obtenir la preview, crawler la preview, comparer, PUIS FERMER la PR
```

## Deux pieges de mesure, verifies

- **Netlify injecte `X-Robots-Tag: noindex` sur toute prévisualisation.** Comparer un crawl de
  production a un crawl de preview fausse toutes les familles d'indexabilite (`noindex_page`,
  les variantes `_not_indexable`). Comparer les familles de base, ou preview contre preview.
- Le crawl de reference et celui de verification doivent utiliser le **meme `--max-pages`** :
  un plafond different change les comptes et ressemble a une regression.

## Cycle Controle

`live_cycle.py` utilise le meme correcteur que les routes produit et se limite aux neuf depots
fixtures. Il ouvre deux PR brouillon, attend le succes du build Netlify pour leur commit exact,
crawle la preview temoin et la preview corrigee, puis ferme les deux PR **sans fusion**.
La fermeture est aussi tentee en cas d'echec. `cycle.json` conserve le resultat et le nettoyage.

Depuis `seo-agent-web`, avec l'environnement Python du projet :

```sh
python ops/gauntlet/live_cycle.py --workdir <dossier-temporaire> --stacks astro next-app
```

Par defaut, seules les corrections mecaniques sont appliquees. `--ai-families` permet de nommer
explicitement un petit lot a exercer avec Claude ; `--ai-max-files` vaut 2 fichiers par famille.
Ce plafond est un budget de banc, pas une certification de toutes les occurrences du site.
Les appels IA sont traces dans `claude.log`, jamais avec leurs cles.
`--ai-max-calls` ajoute un plafond strict de 12 requetes Claude par stack par defaut,
relances comprises. Les echecs du fournisseur comptent aussi, les preparations paralleles
partagent ce plafond et aucun repli OpenAI n'est autorise. Le passage mecanique dispose
de zero appel. Un budget epuise rend le cycle `unverified`.

`--ai-families` selectionne des cles exactes. Demander `missing_meta_description` ne teste
pas automatiquement les descriptions trop courtes ou trop longues : il faut aussi nommer
une cle de longueur, qui regroupe ensuite ses deux variantes. `gauntlet_run.json` conserve
les cles demandees et exercees, le budget, les URL impactees, les fichiers cibles/modifies,
les refus et les fichiers ecartes par le plafond du passage courant. Le diagnostic sur
le contenu d'origine est reserve aux reecriveurs mecaniques ; il n'appelle jamais Claude.

Une famille absente de la preview temoin reste `unverified`, meme si son compteur vaut zero
apres correction. Les settings, les pages HTML effectivement servies, les cibles revisitees et
l'ajout de `noindex` sont controles. Les hausses d'autres familles et les nouvelles URL touchees
restent visibles, meme quand une correction deplace une anomalie sans changer son compteur.
Trois familles sensibles a l'indexabilite disposent aussi de controles directs du HTML observe :
canonical vers une 4xx, liens internes HTTP et langue du HTML servi. Ces preuves sont rapportees
separement des compteurs du crawler et seulement sur les pages effectivement observees.
Le banc impose une base SQLite temporaire et n'utilise jamais la base client.
Les descriptions disposent aussi d'un controle du HTML observe : le defaut doit etre present
dans le temoin, toutes les occurrences doivent etre revisitees, et chaque page corrigee doit
servir une seule balise description de 100 a 160 caracteres. Une mesure du nombre de balises
absente ou inconnue reste `unverified`, meme si le compteur d'anomalies vaut zero.

## Bilans Conserves

- `validation-2026-10-01.json` : HTML statique, verification et interface.
- `validation-multistack-2026-10-01.json` : Astro et Next App, appels Claude limites ;
  la premiere regression sitemap reste conservee avec sa contre-verification.
- `validation-preclients-2026-10-02.json` : six autres stacks, builds et recrawls reels
  sans nouveaux appels Claude ; tests des quotas, confirmations et demandes concurrentes.
- `validation-postgres-et-ia-2026-10-02.json` : vraie base PostgreSQL temporaire, panne de
  connexion, pool et dernier credit partage par six processus ; Claude sur Hugo/Nuxt et
  disparition du reliquat Open Graph sur Hugo/Gatsby/Nuxt apres builds et recrawls reels.
- `validation-reprise-2026-10-03.json` : journal durable des corrections, apercus payes
  recuperables, debit atomique et reprise d'une PR deja acceptee ; migration SQLite/PostgreSQL.
- `validation-reconciliation-2026-10-03.json` : inspection et abandon explicite des operations
  impayees interrompues ; conservation des branches, audit operateur et tests SQLite/PostgreSQL.
- `validation-github-recovery-2026-10-03.json` : ecritures GitHub reelles, arrets de processus
  et coupure de socket ; PR retrouvee sans doublon, puis fermee sans fusion. Suite finale :
  2 800 tests reussis sur SQLite, avec les cas PostgreSQL deja exerces dans le bilan precedent.
- `validation-descriptions-2026-10-03.json` : familles de longueur et balises description
  multiples, Claude reel sur Hugo/Nuxt, builds et recrawls avec temoins positifs. Les reliquats
  passent de 4 a 0 sur Hugo et de 3 a 0 sur Nuxt, sans regression observee.
- `validation-hreflang-retours-2026-10-03.json` : retour manquant corrige sans IA sur Hugo/Nuxt,
  avec conservation du conflit de langue non arbitrable et controle direct du HTML.
- `validation-hreflang-ia-2026-10-04.json` : codes `fr_FR` et `x-default` manquants, Claude reel
  sur Hugo/Nuxt. Un mauvais choix de destination detecte au premier passage est conserve ;
  le correcteur reprend maintenant les choix explicites du groupe observe et les verifie
  avant ecriture. Suite finale : 2 927 tests reussis, sans deploiement de l'agent.

Ces bilans ne certifient pas toutes les anomalies ni tous les clients. Les corrections locales
ne sont pas deployees. Le bilan de reconciliation conserve 2 780 tests reussis sur SQLite et
166 tests cibles reussis, dont 103 sur PostgreSQL reel. Ses cas PostgreSQL ignores dans le
lancement SQLite ont ete exerces separement. Le serveur temporaire a ensuite ete arrete.

Le journal permet de retrouver une PR acceptee avant une perte de reponse ou un echec du debit,
sans reecrire les fichiers ni facturer deux fois. Une branche partielle ou une PR impossible a
confirmer reste bloquee : aucune relance automatique destructive n'est activee. L'outillage de
reconciliation operateur est decrit dans `../CORRECTION_RECOVERY.md`. La conservation des recus reste a cadrer.
La capacite du deploiement reel, les familles masquees par le noindex des previews et les lots IA
incomplets restent a valider.

Le bilan PostgreSQL/IA du 2 octobre corrige aussi l'explication historique du reliquat Open Graph : le canonical
avait deja ete passe en HTTPS, mais son slash final differait encore d'og:url. Les anciens
rapports restent conserves ; la nouvelle preuve et sa contre-verification sont dans ce bilan.

## Reprise Apres Interruption

`recovery_cycle.py` utilise uniquement `noyaru-stack-static-html`, une base SQLite temporaire
et des comptes synthetiques. Sept processus API distincts exercent trois interruptions :
apercu paye avant un arret brutal, processus tue apres creation de branche, puis socket ferme
apres acceptation d'une PR par GitHub. La reprise doit retrouver cette PR, reconstruire sa
trace, refuser un doublon et conserver un seul debit. Les ecritures GitHub sont reelles ;
les pannes sont provoquees par un relais HTTP sur loopback, pas par une panne du service GitHub.

```sh
python ops/gauntlet/recovery_cycle.py --workdir <dossier-vide-temporaire>
```

Le budget est strict : deux branches, un commit sur `gauntlet/missing-title.html`, une PR
brouillon, aucune fusion et aucun appel Claude. Un generateur deterministe remplace l'IA :
ce banc teste la reprise, pas la qualite des corrections SEO. Les branches sont conservees,
la PR du banc est fermee sans fusion, son identite est reverifiee avant fermeture et `main`
doit rester identique. Un resultat de nettoyage inconnu n'est jamais compte comme reussi.
Les workers du banc ne demarrent pas les schedulers ni le lifespan de production.

`recovery.json` conserve les effets distants, les sept codes de sortie/reponses API, le
commit exact et les controles de nettoyage. Aucun jeton n'y est inscrit. Le bilan du
passage reel est `validation-github-recovery-2026-10-03.json`.

## Retours Hreflang

`hreflang_cycle.py` valide uniquement `missing_reciprocal_hreflang` sur Hugo/Nuxt.
Il part de deux commits fixtures deja controles et refuse une origine qui aurait change.
Trois traductions sont ajoutees sur des branches QA, avec un vrai retour manquant sur
l'edition anglaise. Le sitemap statique et l'index du parcours sont etendus uniquement
sur ces branches : les liens relatifs assurent leur decouverte sur les previews.

```sh
python ops/gauntlet/hreflang_cycle.py --workdir <dossier-vide-temporaire>
```

Le correcteur produit est exerce avec un plafond d'un fichier et de zero appel IA.
Il doit cibler la traduction anglaise, pas la page francaise signalee. Le conflit
preexistant (code `fr` deja utilise pour une autre URL) reste intact et est explique.
Les quatre PR brouillon sont fermees sans fusion, meme si la verification echoue.

Les rapports bruts sont conserves. Ils ne prouvent PAS la correction par leur compteur
zero : le `noindex` injecte par Netlify masque cette famille. Un rapport distinct,
explicitement contrefactuel, remplace le domaine preview dans les champs URL structures
par le domaine de production et retire seulement l'en-tete exact `noindex` des pages
du domaine preview. Les meta robots et autres directives sont conserves. Le moteur
d'audit d'origine recalcule les anomalies avec le sitemap XML effectivement servi.
Cette projection ne reconstitue pas un crawl de production : les classifications de
liens observees restent celles du crawl preview. Elle ne certifie pas l'indexabilite.

Le temoin doit contenir la preuve precise du retour manquant. Apres build et recrawl,
toutes les pages HTML doivent rester accessibles, cette preuve doit disparaitre et
aucune hausse ou nouvelle route d'anomalie ne doit etre observee dans la comparaison.
Le compteur total reste `partial` (2 vers 1), car le conflit non arbitrable subsiste ;
l'occurrence corrigeable passe de 1 a 0. Le HTML brut confirme directement la nouvelle
balise et la preservation du canonical et des annotations existantes.

`validation-hreflang-retours-2026-10-03.json` conserve les limites, builds, empreintes,
resultats et tentatives initiales refusees faute de pages effectivement visitees.
Les autres familles hreflang, notamment `x-default` et les codes invalides, ne sont
pas certifiees par ce passage.

## Hreflang Avec Claude

`hreflang_ai_cycle.py` exerce exactement `hreflang_annotation_invalid` puis
`x_default_hreflang_missing` sur Hugo/Nuxt. Il part des deux commits de correction
du banc precedent, verifies par SHA et ascendance de `main`. Une branche QA retire
uniquement le `x-default` anglais du groupe FR/EN/DE : les deux autres traductions
conservent leur choix explicite de la page francaise. Les anciens cas `fr_FR` et
`x-default` manquants restent aussi des temoins positifs.

```sh
python ops/gauntlet/hreflang_ai_cycle.py --workdir <dossier-vide-temporaire> --ai-max-calls 6
```

Le plafond inclut les relances et echecs fournisseur ; sa valeur par defaut et maximale
est de 12 appels par stack. Chaque stack utilise un processus distinct, une base SQLite
temporaire et aucune IA de ciblage. Les fichiers attendus sont resolus par les routes,
avec au plus six fichiers par famille. Le banc exige leur correction complete ; aucun
fichier omis par le plafond ne peut etre compte comme valide. Il s'arrete apres le premier
stack non valide. Les deux PR brouillon par stack sont fermees sans fusion, y compris
en cas d'echec ; aucune branche client, aucun scheduler et aucune base client ne sont utilises.

Les builds doivent correspondre aux commits exacts. Les rapports bruts, sitemap XML,
rapports contrefactuels, limites, fichiers et nettoyage sont conserves. Les compteurs
bruts a zero restent exclus des preuves : la preview est toujours `noindex` en realite.
La projection suit les memes limites que le banc de retours hreflang ci-dessus.
`usage.json` ne contient que fournisseur, modele, tokens, budget et cout estime par le
code : ce cout n'est pas une facture du fournisseur. Les cles et prompts n'y sont pas inscrits.

Le controle HTML exige `fr_FR` vers `fr-FR` sans changer ses destinations, une seule
annotation `x-default`, et la preservation des autres annotations, canonical, titres,
descriptions, robots et metadonnees sociales. La page anglaise QA doit reprendre
precisement le choix FR de son groupe, pas sa propre URL. Les anciennes fixtures sans
politique explicite autorisent la racine ou leur edition FR existante, toutes observees,
HTTP 200, canoniques et sans `noindex` dans le rapport contrefactuel. Ces choix de fixture
ne constituent pas une recommandation universelle pour les clients.

Le premier passage avait fait disparaitre les deux compteurs tout en choisissant le mauvais
`x-default` pour la page anglaise. Il reste `unverified` dans les preuves. Le correcteur
produit reprend desormais les choix coherents des traductions directement liees et
reciproques, lorsque la destination est observee et utilisable. Un choix contradictoire
ou une destination observee non indexable refuse la preparation. Sur les fichiers par
page, une sortie IA ne respectant pas ce choix est refusee avant le PUT GitHub.
Le controle de source couvre les balises HTML et les objets Nuxt litteraux bornes ;
les generateurs partages/dynamiques et les groupes sans politique observable ne sont
pas universellement certifies. Les corrections proposees restent a relire avant fusion.

Le bilan conserve l'echec initial et les deux passages finaux : codes invalides 1 vers 0
sur chaque stack, `x-default` 5 vers 0 sur Hugo et 4 vers 0 sur Nuxt. Les six PR sont
fermees sans fusion et les trois `main` restent inchanges. Cela ne certifie ni tous les
codes de langue possibles, ni les autres familles IA, ni une indexation en production.
La [documentation Google](https://developers.google.com/search/docs/specialty/international/localized-versions)
decrit le role de destination de repli de `x-default` ; le choix concret du client reste
une decision de son groupe de traductions.

## Reconstruire les pages

`build_pages.py` (les 31 pages) puis `build_scaffold.py` (index, ressources, redirection,
sitemap). Chaque page porte en tete un commentaire nommant la famille visee, pour qu'un lecteur
du depot distingue une fixture d'une erreur.
