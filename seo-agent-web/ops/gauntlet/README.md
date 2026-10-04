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
- `validation-titres-et-doublons-2026-10-04.json` : titres manquants, multiples et hors
  limites, puis paire de titres/descriptions dupliques, Claude reel sur Hugo/Nuxt. Les bugs
  de lecture et de placement du `raw_head` Hugo sont corriges ; les autres occurrences des
  grands groupes de doublons restent hors perimetre. Huit PR fermees sans fusion et
  2 980 tests reussis sur SQLite temporaire ; aucune correction deployee sur l'agent.
- `validation-lots-et-reprise-2026-10-04.json` : lots simules de 14 pages, echec partiel,
  puis trois fichiers GitHub reels avec PostgreSQL temporaire. Une branche interrompue
  est retenue explicitement, une PR et un debit retrouves sans doublon. Le resume du lot
  reste disponible apres reprise ; un ancien recu sans resume reste de perimetre inconnu.
  Suite finale : 3 014 tests reussis ; 159 tests reussis sur PostgreSQL reel, ensuite arrete.
- `validation-doublons-par-lots-2026-10-04.json` : huit pages Hugo corrigees avec Claude
  en lots plafonnes, puis build et recrawl sans regression observee. Nuxt reste NON VALIDE :
  deux descriptions de 169 caracteres ont revele un plafond absent des objets meta nommes,
  corrige et rejoue sur ces sources ; une nouvelle passe refuse ensuite deux propositions
  de titres au controle de syntaxe. Les echecs sont conserves, pas comptes comme reussites.
  Suite finale : 3 088 tests reussis ; quatre PR fermees sans fusion, sans deploiement de l'agent.
- `validation-nuxt-syntaxe-2026-10-04.json` : propositions Nuxt capturees et controlees
  independamment par Node. Un repli borne vers le fichier complet recupere deux editions
  ciblees invalides. Huit titres et huit descriptions valides apres build et recrawl,
  sans regression observee ; 3 122 tests reussis et deux PR fermees sans fusion.
- `validation-sitemaps-et-inventaire-2026-10-04.json` : creation et reparation du sitemap,
  recrawl intermediaire puis declaration robots, sans IA sur le depot statique. Deux
  cycles de 44 routes HTML et 37 entrees, quatre PR fermees sans fusion et 3 200 tests
  reussis. Un defaut hreflang preexistant revele par le sitemap reste explicite ;
  l'inventaire des 66 familles ne les certifie pas toutes.

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

## Titres Et Doublons

`title_cycle.py` exerce les titres manquants, multiples et hors limites, puis les titres
et descriptions dupliques des seules pages `duplicate-a` et `duplicate-b`, sur Hugo/Nuxt.
Il part des commits verifies du banc hreflang IA. Les deux branches QA restent separees
de `main`, les PR sont des brouillons et se ferment sans fusion. Les temoins titre
sont propres a chaque stack : une famille absente reste explicitement
`not_exercised`, notamment les balises titre multiples que Nuxt deduplique deja au build.

```sh
python ops/gauntlet/title_cycle.py --workdir <dossier-vide-temporaire> --ai-max-calls 12
```

Le budget est de 12 appels Claude au maximum par stack, relances et erreurs comprises.
Le ciblage n'utilise pas d'IA ; une famille doit modifier exactement ses fichiers resolus
par les routes, avec au plus six fichiers. Le banc travaille dans une base SQLite
temporaire, sans comptes clients ni schedulers. Les familles sont appliquees sur une
meme branche de correction : la verification porte sur ce passage cumulatif, pas sur
chaque famille executee independamment. `usage.json` conserve les tokens et le cout
estime par le code, pas une facture du fournisseur, sans cles ni prompts.

Les familles de longueur regroupent les titres trop courts et trop longs, y compris
le temoin natif `noindex`, dont la directive doit rester intacte. Le HTML doit contenir
une seule balise titre de 15 a 70 caracteres ou une seule description de 100 a 160
caracteres. Ce sont les seuils du crawler, pas une recommandation universelle de longueur.
Les deux pages du groupe selectionne doivent devenir distinctes aussi des autres pages
observees. Canonical, robots, langues, hreflang, titres de contenu, images sociales et
metadonnees non ciblees sont controles sur toutes les pages HTML revisitees. Une copie
sociale du texte cible peut suivre exactement sa nouvelle valeur ; les autres changements
ne sont pas acceptes. Une mesure absente, un temoin absent ou une page perdue ne vaut pas
une correction, meme quand un compteur tombe a zero.

Les gros groupes de doublons volontaires du parcours ne sont PAS tous reecrits par ce
budget : `unselected_impacted_urls` conserve chaque occurrence hors de la paire. Un
resultat `measured_selected_occurrences` certifie uniquement les controles selectionnes ;
les compteurs globaux des doublons restent `partial`. Les rapports bruts, le sitemap
servi, les rapports contrefactuels et les limites de la projection sont conserves comme
dans le banc hreflang. Les previews restent reellement `noindex`, sans preuve d'indexation.

Le statut Netlify doit correspondre au domaine de la PR exacte, pas a une ancienne PR
au meme SHA. Le banc attend aussi que la racine et le sitemap XML soient servis avant
le crawl : un statut vert au meme SHA ne garantit pas une nouvelle preview disponible.
Les premiers essais interrompus restent conserves : preview pas encore prete,
lecture du titre front matter au lieu de `raw_head`, puis remplacement de la premiere
copie du texte dans le fichier sans corriger la balise effectivement servie.

Le correcteur lit desormais le head litteral d'un `raw_head` TOML valide, avec abstention
si son literal source est ambigu. Le raccourcisseur remplace le literal du champ cible
plutot qu'une copie quelconque du texte et refuse un texte qui nomme aussi sa syntaxe.
L'anti-doublon compare les valeurs apres
raccourcissement et echappement, prefere une relance conforme meme plus courte, reserve
les valeurs observees hors des fichiers cibles ainsi que celles du cache du lot, puis
reverifie le contenu final avant PUT GitHub. Cela ne remplace pas un build et un recrawl
pour des templates clients arbitraires, dynamiques ou comportant des suffixes.

## Lots Et Reprise

`bulk_recovery_cycle.py` exerce la vraie route `github/bulk-fix` avec session, CSRF,
compte synthetique non administrateur, quota de trois corrections et journal durable.
Le fournisseur est simule : cela ne reteste pas la qualite des reponses Claude.
Les effets GitHub, arrets de processus et deconnexions du relais sont reels.

```sh
python ops/gauntlet/bulk_recovery_cycle.py --workdir <dossier-vide-temporaire>
python ops/gauntlet/bulk_recovery_cycle.py --workdir <autre-dossier-vide> --postgres-url postgresql+psycopg://fixture@127.0.0.1:<port>/seo_corrector_test_bulk_<id>
```

Sans URL, SQLite reste strictement dans le dossier du banc. PostgreSQL doit etre une
base NEUVE sans tables, dans un cluster temporaire sur loopback, sous le role `fixture`,
sans mot de passe ni options de connexion. Une base existante ou un autre hote est refuse
avant les effets GitHub. Le banc n'initialise ni ne demarre un service PostgreSQL.

Le relais n'autorise que `pployeraffiliation-a11y/noyaru-stack-static-html`, deux nouvelles
branches `seo-fix/bulk-*` depuis le `main` capture, quatre commits de contenu exact et une
PR brouillon. Les seuls fichiers modifiables sont `gauntlet/duplicate-a.html`,
`gauntlet/duplicate-b.html` et `gauntlet/missing-title.html`. Toute fusion, suppression,
ecriture de `main` ou autre contenu est refusee. Le rapport limite a ces deux familles
est synthetique et derive des sources capturees, pas d'un nouveau crawl de production.

Le premier worker est tue apres l'acceptation du premier fichier, avant son accuse de
reception et avant la PR. La relance repond 503 sans modele, debit ou nouvelle ecriture.
L'operateur doit reconnaitre les changements non publies et conserver explicitement la
branche avant de liberer la demande impayee. Cela ne constitue PAS une reprise automatique
des fichiers restants : le lot suivant repart du `main` intact, sur une autre branche.

Le lot suivant ecrit ses trois fichiers. Le relais perd ensuite la reponse a la PR acceptee.
Un nouveau worker retrouve la PR et est arrete apres la transaction du debit de trois
unites. Le processus suivant restaure les deux taches de famille et la trace du commit
exact, sans nouveaux fichiers, appel modele ou debit. Une nouvelle demande identique est
refusee avec 409. La PR est fermee sans fusion ; les deux branches restent conservables.

Le resume du lot est maintenant fige dans l'intention PR, avant publication : compteurs,
resultats, plafonds, fichiers exclus et drapeau partiel restent presents a la reprise.
Une ancienne intention sans resume exploitable retourne `partial` et `scope_unknown` ;
elle ne certifie pas un perimetre complet. Le resume ne peut pas ecraser l'identite verifiee
de la PR. Aucun nouveau schema de base n'est requis.

Les tests unitaires exercent aussi 14 pages en trois lots plafonnes a 6, 6 et 2 fichiers,
avec une collision contre une valeur d'un lot precedent. Les valeurs observees et celles
du cache restent reservees, MEME sur les fichiers selectionnes : un refus de source ou un
PUT en echec ne doit pas permettre de reprendre leur ancienne valeur sur une autre page.
Cela n'est pas une preuve de reecriture Claude de tous les grands groupes des fixtures.

Le bilan conserve les recus et empreintes, ainsi que la contre-verification independante
de la PR fermee, du `main`, des trois contenus, de la branche partielle et du seul debit
PostgreSQL. Le quota interne est teste ; aucun paiement Stripe, site client, deploiement,
build ou recrawl SEO n'est exerce dans cette passe.

## Doublons Par Lots

`duplicate_batch_cycle.py` reprend les commits valides du banc titres sur Hugo/Nuxt.
Il selectionne huit autres pages du grand groupe commun : `link-http`, `missing-alt`,
`missing-h1`, `mixed-css`, `mixed-image`, `mixed-js`, `multiple-h1` et `redirected-css`.
Les titres puis descriptions sont reecrits avec Claude reel sur la meme branche QA,
en deux lots plafonnes a six puis deux fichiers pour chaque famille.

```sh
python ops/gauntlet/duplicate_batch_cycle.py --workdir <dossier-vide-temporaire> --ai-max-calls 24
```

Le plafond de 24 appels par stack inclut les relances et erreurs ; aucun ciblage IA ni
repli OpenAI n'est permis. La base SQLite et les dossiers sont temporaires, sans
comptes clients, startup de production ou schedulers. Seuls seize PUT de contenu sur
les huit fichiers resolus sont autorises, sur une nouvelle branche de correction.
`main`, les configurations, les autres fichiers et depots ne sont pas modifiables.
Les branches partent de commits QA dont l'identite et l'ascendance sont verifiees.
Les deux PR par stack sont des brouillons fermes sans fusion, y compris apres echec.

Chaque lot doit partager exactement les fichiers restants entre `patched` et
`deferred_files`, sans exclusion silencieuse ni nouvelle cible. Le cache de contenu
reste partage entre lots et familles. Les valeurs finales lisibles sont controlees
sur tous les fichiers deja traites, avec leurs bornes, SHA de blob et empreinte source.
Ces observations de source ne remplacent pas la mesure HTML du dernier commit.

Le banc attend le build de la PR exacte puis son HTML et sitemap, conserve le crawl
brut et applique la meme projection contrefactuelle que les bancs hreflang/titres.
Toutes les routes doivent etre revisitees. Chaque valeur cible doit avoir un temoin
dupliquant positif, changer, respecter les bornes, n'avoir qu'une balise et devenir
unique aussi contre les pages non selectionnees. Les metadonnees non ciblees et les
champs observes de contenu, liens, images et donnees structurees sont compares.
Les compteurs de toutes les familles ne doivent pas augmenter ni introduire de
nouvelles routes en anomalie. Les ressources generees a nom de fichier variable ne
font pas l'objet d'une comparaison octet pour octet entre builds.

Le resultat `measured_selected_occurrences` ne vaut que pour ces huit pages et ce
passage cumulatif, pas pour tous les membres du groupe ni pour chaque famille
executee independamment. `unselected_impacted_urls` garde les autres occurrences ;
`all_group_occurrences_certified` reste faux. Les previews restent `noindex` en
realite : aucune indexation, fusion ou mise en production de l'agent n'est certifiee.
`usage.json` mesure les tokens et le cout estime du code, sans facture fournisseur,
cles ou prompts. La reprise HTTP/quota, les paiements et les templates clients
arbitraires ne sont pas retestes par ce banc.

Le premier passage Nuxt est conserve comme ECHEC : apres huit titres et six
descriptions ecrits, deux descriptions nommees faisaient 169 caracteres. Le plafond
lisait `description: '...'`, pas l'objet `name: 'description', content: '...'`.
Il couvre maintenant ce literal, ses apostrophes et les copies sociales exactes
ecrites par le patch, sans couper une expression ou une ancienne valeur non modifiee.
La mesure est refaite apres reparation des guillemets, et une valeur cible qui
redeviendrait trop longue apres un autre garde-fou est refusee avant PUT.
Cette branche partielle n'est pas certifiee : l'essai suivant repart du meme commit
initial sur une NOUVELLE branche, sans reprise automatique du contenu interrompu.

Ce nouvel essai Nuxt est lui aussi un ECHEC : sur six propositions de titres, quatre
fichiers sont ecrits et deux sont refuses par le controle de litteraux non termines
(`mixed-css`, `mixed-js`). Les deux fichiers refuses restent inchanges. Le banc
s'arrete avant les descriptions et avant une PR de correction ; le temoin est ferme.
Pour cet essai historique, les propositions refusees n'avaient pas ete conservees :
leur cause precise n'etait donc pas diagnostiquee. Le bilan precedent garde cet echec,
le succes Hugo et le rejeu de source du correctif de plafond distincts.

## Diagnostic Nuxt

Le passage suivant conserve les sources fixtures d'origine, les propositions ciblees,
le repli complet eventuel et le contenu soumis au refus dans `source_diagnostics/`.
Les empreintes et refus sont dans `sources.json`, sans cles, prompts ni reponses brutes
du fournisseur. La capture est limitee aux huit sources Hugo/Nuxt du banc et a leur
hostname fixture ; les fonctions temporaires sont restaurees meme en cas d'exception.

Une sonde Claude sans ecriture distante a reproduit une ligne `title` avec un guillemet
orphelin apres sa virgule. Node confirme que la proposition etait deja invalide avant
les garde-fous : le refus protegeait correctement le depot. Une edition qui trouve son
texte n'est donc pas necessairement une edition syntaxiquement valable.

Le generateur essaie maintenant les reparations mecaniques existantes sur une copie
de la proposition ciblee. Si le parseur de litteraux de tete constate encore une erreur
certaine, il utilise UNE fois le repli complet existant, a partir du fichier original
et avec ce diagnostic. Une expression que le parseur ne sait pas lire n'est pas un
motif de relance. Les refus finaux, plafonds et controles d'unicite avant PUT restent
obligatoires ; une seconde proposition invalide n'est pas acceptee pour finir le lot.

Le nouveau cycle reel a reproduit ce defaut sur `mixed-css` et `mixed-js`, puis livre
seize commits sur huit fichiers apres deux replis complets. Les builds exacts et le
recrawl des 43 pages passent : huit doublons cibles de chaque champ deviennent zero,
sans regression observee. Les descriptions finales font 147 a 159 caracteres.
La contre-verification sans IA relit GitHub, les sources et le HTML ; Node valide les
huit scripts et les templates restent identiques au commit de depart.
Les PR 52/53 sont des brouillons fermes sans fusion, `main` est inchange.
Les 19 autres titres et 20 autres descriptions en doublon restent hors perimetre.
Le bilan `validation-nuxt-syntaxe-2026-10-04.json` ne certifie ni tous les groupes,
ni tous les templates clients, ni le demarrage ou le deploiement de l'agent.

## Sitemaps Et Inventaire

`sitemap_lifecycle_cycle.py` exerce deux temoins sur le seul depot fixture statique :
un sitemap absent, puis un XML volontairement casse. Chaque cas part du `main`
inspecte sur une nouvelle branche QA. Seuls `sitemap.xml` et `robots.txt` peuvent
etre ecrits ; une suppression du sitemap sur le temoin absent est reservee au setup.
Le budget modele est zero, les donnees et la base sont temporaires, les deux PR
par cas sont des brouillons fermes sans fusion et `main` reste inchange.

```sh
python ops/gauntlet/sitemap_lifecycle_cycle.py --workdir <dossier-vide-temporaire>
python ops/gauntlet/acceptance_inventory.py --output <nouveau-fichier-json>
```

La selection des URL exige maintenant une reponse HTML 200 observee, sans erreur,
blocage ni noindex, et un canonical absent ou identique a l'URL finale. Le schema,
la casse du chemin et des parametres et le slash final restent significatifs.
La reparation relit le blob courant : un XML redevenu lisible, un contenu inconnu
ou un XML refuse par le parseur securise ne sont jamais ecrases. Un encodage
inconnu est refuse avec une note, sans exception non geree. Un blob vide
explicitement confirme reste reparable ; le SHA courant protege le PUT concurrent.

Apres la correction du sitemap, un vrai recrawl intermediaire fournit le rapport
qui debloque sa declaration dans robots. Les trois builds doivent porter le SHA
exact attendu. La mise a jour differee du head de PR ne permet plus d'accepter le
build du commit precedent. Le premier essai arrete avant le crawl final reste
conserve comme echec, avec ses PR fermees, et ne vaut pas preuve de correction.

Les crawls bruts restent noindex. Le rapport controle projette seulement le host
fixture et le header injecte exact ; son XML vient du corps HTTP sauvegarde de
LA preview concernee. Le crawler brut peut suivre la declaration robots absolue
vers le domaine de production : cette decouverte n'est pas presentee comme celle
du sitemap modifie de la preview. L'indexation de production n'est pas certifiee.

Les deux nouveaux cycles observent 44 routes HTML preservees et 37 entrees de
sitemap admissibles. Le XML et la declaration robots sont corriges, mais le
compteur `missing_reciprocal_hreflang` passe de 0 a 1. Une contre-verification
sans ecriture reproduit cette hausse sur le HTML INCHANGE du temoin avec le
nouveau sitemap : ce defaut preexistant devient mesurable, il n'est pas repare
par cette passe. Il n'y a donc pas de verdict global "tous les compteurs verts".

`acceptance-inventory-2026-10-04.json` recense 203 cles et 66 familles declarees
corrigibles, dont 65 exposees ; `missing_canonical` est un ancien cas non propose.
Les variantes d'indexabilite sont groupees, pas les familles editoriales distinctes.
Une reference nommee dans un bilan, meme en echec, reste une simple reference,
JAMAIS une certification. Apres ce bilan, 22 familles n'ont aucune reference
nommee exacte ; ce chiffre ne prouve pas qu'elles sont toutes non testees.
L'inventaire garde les empreintes des bilans et la certification clients a faux.
Le bilan `validation-sitemaps-et-inventaire-2026-10-04.json` distingue les preuves
de ce perimetre statique des sitemaps generes, multisitemaps et templates clients.

## Reconstruire les pages

`build_pages.py` (les 31 pages) puis `build_scaffold.py` (index, ressources, redirection,
sitemap). Chaque page porte en tete un commentaire nommant la famille visee, pour qu'un lecteur
du depot distingue une fixture d'une erreur.
