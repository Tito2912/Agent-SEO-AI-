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
- `validation-canonical-et-redirections-2026-10-04.json` : deux pages partageant
  une cible 404 retrouvent chacune leur propre canonical, sans toucher aux hreflang.
  Une boucle forcee revient en HTTP 200 ; les autres redirections sont conservees.
  Trois PR fermees sans fusion, dont le premier temoin incomplet refuse, zero IA
  et 3 240 tests reussis. Des defauts de liens et de langues deviennent visibles
  avec les pages accessibles/auto-canoniques ; leurs hausses restent explicites.
- `validation-destinations-canonical-2026-10-04.json` : canonical vers une
  redirection et vers une page non canonique, sans IA sur le depot statique.
  Les deux temoins sont corriges, les 49 routes HTML et la redirection legitime
  restent intactes. Une occurrence preexistante non selectionnee demeure ;
  les lots mixtes conservent les refus et les paires valides separement.
- `validation-canonical-residuel-2026-10-04.json` : le cas preexistant
  `canonical-other` est ensuite corrige sur une branche issue du lot precedent.
  Un seul attribut canonical change dans un seul fichier, sans IA. La famille
  non canonique passe de 1 a 0 ; les anciennes corrections, les 49 routes HTML
  et la vraie 301 sont conservees. Aucun compteur n'augmente dans ce controle.
- `validation-http-canonical-conseil-2026-10-04.json` : une page servie en HTTP
  avec canonical HTTPS est un probleme d'hebergement, pas de balise a remplacer.
  Le correcteur refuse ce cas, y compris via un ancien apercu ; l'anomalie reste
  visible. Un temoin local HTTP/TLS reste a 1 apres le refus puis passe a 0
  uniquement apres une 301 ajoutee manuellement au serveur, sans changer le HTML.

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

## Canonical Et Redirections

`canonical_redirect_cycle.py` cree trois pages QA sur une nouvelle branche du
seul depot statique : deux canonical vers la meme cible disparue et une regle
301 forcee redirigeant une page vers elle-meme. Les liens relatifs font visiter
la cible 404 au crawler ; une simple observation HTTP externe ne suffit pas
a autoriser sa correction. Le premier essai refuse pour cette raison est conserve.

```sh
python ops/gauntlet/canonical_redirect_cycle.py --workdir <dossier-vide-temporaire>
```

Six fichiers de setup et trois PUT correctifs sont permis, uniquement sur les
branches QA. La correction doit modifier les deux pages et `_redirects`, sans
IA. Le canonical de chaque page est rattache a son propre fichier, pas a la
premiere page partageant cette cible. Un fichier partage, une identite ambigue
ou un statut inconnu, contradictoire, protege ou temporairement en erreur est
refuse. Schema, casse du chemin/parametres et slash restent significatifs.
Les balises alternate/hreflang et les liens ordinaires sont conserves. Les variantes
relatives du canonical gardent leurs parametres et fragments : une URL sans
parametre ne peut pas correspondre a une cible mesuree avec parametre.

Les deux builds portent leur SHA exact. Les 46 routes HTML initiales restent
presentes, une 47e devient accessible, les canonical fautifs passent de 2 a 0
et la boucle passe de 301 a 200. Les deux autres redirections sont legitimes
et restent en place. Les deux PR brouillon sont fermees sans fusion ; `main`
reste identique. Le HTML source de la page auparavant en boucle est inchange.

La comparaison independante conserve le noindex natif et les corps HTTP bruts.
Netlify ajoute une barre de preview aux pages de controle : la comparaison
au source identifie seulement ce fragment exact, sans ignorer d'autre contenu.
Les deux canonical servis et les alternate sont aussi verifies par un parseur
HTML independant. Les corps HTTP relus sont identiques aux captures conservees.

Les pages desormais accessibles/auto-canoniques revelent des liens casses,
des hreflang incomplets et des liens entrants uniques. Le rejeu du rapport
initial avec seulement ces deux canonical et la page redevenue accessible
reproduit chaque hausse et exemple. Ces defauts restent non corriges ici :
ce passage ne certifie ni tous les compteurs verts ni toutes les redirections.
Le refus d'une cible 503 est teste sur une entree synthetique, pas en HTTP reel.
Le dernier garde-fou sur les URL relatives est couvert par trois tests unitaires
et la suite complete. Un rejeu du code final capture localement les trois PUT,
a partir des sources du temoin : ils sont identiques aux corrections mesurees.
Ce rejeu n'est pas un nouveau build/recrawl ni un temoin HTTP de canonical a parametres.

Le bilan `validation-canonical-et-redirections-2026-10-04.json` conserve les
empreintes et les limites. Un nouvel inventaire temporaire compte 19 familles
sans reference nommee, contre 22 avant ce bilan. Cela inclut une preuve de
refus et une famille exposee mais non reparee : ce n'est pas un score de reussite.
L'ancien inventaire suivi dans Git est conserve comme photographie historique.

## Destinations Canonical

`canonical_target_cycle.py` exerce `canonical_points_to_redirect` et
`non_canonical_page_specified_as_canonical_one` sur le seul depot fixture statique.
Cinq nouvelles pages QA portent deux temoins, un relais, une destination et
un controle. Huit PUT de setup et deux PUT correctifs sont permis sur les seules
branches QA. Aucune ecriture sur `main`, aucune IA, aucune fusion.

```sh
python ops/gauntlet/canonical_target_cycle.py --workdir <dossier-vide-temporaire>
```

La preuve doit etre corroboree par les pages observees : canonical actuel de
la source, relation de redirection ou canonical declare par l'ancienne cible,
destination HTML 200 sans erreur, blocage ou noindex, sans canonical vers
une autre adresse. Les identites d'URL restent exactes. Une absence de preuve
ne permet plus une correction guidee uniquement par une consigne IA.
Les fichiers ambigus ou non resolus ne declenchent pas de ciblage IA non plus.

Les paires de canonical sont rattachees a leur propre fichier. Une valeur
appartenant a une autre page n'est pas reecrite. Pour une URL relative, le
protocole et le domaine de la source restent significatifs : une destination
sur un autre domaine est ecrite en absolu, pas en chemin relatif local.
Les parametres, fragments, hreflang et liens ordinaires sont conserves.
Dans un lot mixte, les paires verifiees restent corrigeables et les fichiers
des autres pages sont explicitement ecartes, sans PUT ni appel modele.

Les deux builds portent leur SHA exact ; les deux crawls observent les memes
49 routes HTML. Le canonical vers la 301 passe de 1 a 0. La famille non canonique
passe de 2 a 1 : le nouveau temoin passe bien de 1 a 0, mais l'ancien cas
`/gauntlet/canonical-other` reste hors de ce lot. Il n'est pas compte comme repare.
La vraie redirection 301 est utile et reste en place, ainsi que `_redirects`.
Aucun compteur n'augmente dans cette comparaison controlee.

Une contre-verification relit les PR fermees sans fusion, leurs builds, les
sources et le HTML servi. Les corps HTTP bruts conservent le noindex et la
barre Netlify ; seul son fragment exact est distingue du source. Le parseur
HTML confirme les canonical et les alternate. Les propositions du correcteur
final sont capturees sans ecriture distante et egalent les commits mesures.
Le refus d'une destination noindex et les lots mixtes sont testes separement.
Une assertion initiale exigeant a tort un refus global malgre d'autres paires
valides est corrigee dans la mesure, sans changement du comportement produit.

Le bilan `validation-destinations-canonical-2026-10-04.json` distingue cette
preuve statique des templates partages, URL calculees et autres schemas de
canonical. La projection des previews reste contrefactuelle, pas une preuve
d'indexabilite en production. Le nouvel inventaire temporaire compte 17 familles
sans reference nommee ; l'ancien inventaire Git reste une photographie historique,
et les references ne deviennent jamais un score de corrections certifiees.

## Canonical Residuel

`canonical_completion_cycle.py` repart du SHA mesure du lot precedent, pas de
`main`, et verifie sa branche, sa PR fermee sans fusion, son build et son ascendance.
Le seul fichier correctible est `gauntlet/canonical-other.html`. Un seul PUT
est permis, avec le SHA du blob et le contenu exact attendus : seul l'attribut
canonical peut changer. Le banc n'ajoute aucune page ni regle, et n'utilise pas
de modele. Les 32 nouveaux tests couvrent la portee des ecritures, les temoins,
la conservation des routes et corrections, les compteurs et le nettoyage des PR.

```sh
python ops/gauntlet/canonical_completion_cycle.py --workdir <dossier-vide-temporaire>
```

La source declare le relais `canonical-relay`, qui declare `missing-h1` comme
maitresse. Cette derniere est observee HTML 200 avec son propre canonical.
Le correcteur conserve cette intention : il remplace le relais par la maitresse,
il ne rend pas arbitrairement `canonical-other` auto-canonique. Le H1 manquant
de la destination et toutes ses autres anomalies restent hors de cette correction.

Deux builds et deux vrais crawls portent les SHA exacts. La famille
`non_canonical_page_specified_as_canonical_one` passe de 1 a 0 dans les 49 routes
observees ; `canonical_points_to_redirect` reste a 0. Les cinq pages QA du lot
precedent restent intactes, tout comme la vraie 301 et `_redirects`. Aucune autre
valeur observee ne change, aucun compteur n'augmente. Les PR 57 et 58 sont fermees
sans fusion, et `main` reste identique.

Une nouvelle contre-verification en lecture seule controle les sources et les
16 corps HTML servis, en distinguant uniquement le fragment exact de la barre
Netlify. Les fins de ligne reelles, les alternate et le noindex natif sont
conserves. Un rejeu local capture la proposition du correcteur sans PUT distant :
elle egale le commit mesure. Une destination synthetique noindex est refusee.
Les assertions initiales du controle sur les fins de ligne et les champs JSON
optionnels sont corrigees et consignees, sans changement du produit.

Le bilan `validation-canonical-residuel-2026-10-04.json` complete le bilan
precedent sans le reecrire. Il n'apporte aucun nouveau correctif au moteur :
celui-ci savait deja traiter ce cas, qui etait simplement hors du lot selectionne.
La suite finale compte 3 322 tests reussis et 42 ignores sur SQLite isolee ;
131 tests cibles passent. L'inventaire reste a 17 familles sans reference nommee.
La projection des previews est toujours contrefactuelle. Ce resultat ne certifie
ni l'indexation en production, ni toutes les piles, ni toutes les anomalies avant
les premiers clients. Aucun deploiement de l'agent ni site client n'est modifie.

## HTTP Et Canonical HTTPS

`canonical_from_http_to_https` se leve quand la page reste servie en HTTP et
declare HTTPS comme canonical. Le canonical HTTPS n'est pas a remplacer pour
cette famille ; il faut verifier la destination et le certificat, puis corriger
l'acces HTTP au niveau de l'hebergement, du serveur ou du proxy. Reecrire les
liens ou le sitemap ne prouve pas que l'ancienne URL HTTP cesse de servir en 200.

Le produit proposait pourtant cette famille au correcteur generique de tete,
avec des candidats comme un layout partage et sans preuve du fichier qui pilote
le serveur. La consigne au modele disait de conserver le canonical, mais ce
n'etait pas un refus garanti. Le cas devient donc consultatif, sans supprimer
sa detection ni son affichage. Son conseil demande aussi de revisiter l'ancienne
URL HTTP, pas de la retirer du controle pour obtenir artificiellement un zero.

Le refus s'applique a la preparation et au garde-fou commun des operations, apres
l'autorisation du projet mais avant le journal, les anciens apercus, le quota ou
GitHub. Il protege les corrections individuelles, les apercus par URL/GitHub et
les confirmations, meme avec les variantes d'indexabilite, la casse et les espaces.
Le lot automatique exclut cette famille. Le cas inverse,
`canonical_from_https_to_http`, reste propose : sa valeur de canonical est bien
le probleme a traiter, mais sa preuve de reparation sera validee separement.

`http_canonical_advice_cycle.py` ne modifie aucun depot. Il cree uniquement deux
serveurs loopback sur des ports ephemeres, dont un TLS avec certificat temporaire.
Requests verifie ce certificat ; le vrai extracteur Playwright et le scorer de
l'agent observent HTTP 200 et canonical HTTPS, puis le refus laisse le compteur
a 1. L'operateur ajoute ensuite une 301 dans le serveur du temoin : l'ancienne
URL redirige vers HTTPS 200 et le compteur passe a 0. Les trois corps HTML sont
identiques et leur canonical reste inchange. Cette intervention manuelle n'est
jamais comptabilisee comme une reparation par le correcteur.
Les compteurs de redirection deviennent visibles, ainsi qu'une notice de page
orpheline indexable dans ce temoin minimal : ce controle manuel n'est pas tout vert.

```sh
python ops/gauntlet/http_canonical_advice_cycle.py --workdir <dossier-vide-temporaire>
```

Les serveurs et navigateurs sont fermes, la cle privee generee est supprimee,
et aucun fichier existant n'est ecrase. Le controle loopback n'assouplit pas la
politique SSRF de production. Une contre-verification relit les rapports et les
corps servis avec un autre parseur, verifie la fermeture des ports et teste les
quatre refus d'API avant toute consultation d'apercu ou de quota. La fixture
publique Netlify confirme HTTP 301 et HTTPS 200 : elle ne peut pas fournir le
temoin positif HTTP 200 que cette famille exige.

Le bilan `validation-http-canonical-conseil-2026-10-04.json` conserve ce resultat
negatif et le changement de promesse : 66 vers 65 familles revendiquees, 65 vers
64 exposees comme corrigibles. Le nombre de familles revendiquees sans reference
nommee passe de 17 a 16 uniquement par ce reclassement, pas par une reparation
reussie. Les inventaires et preuves historiques restent inchanges. Aucune IA,
aucune PR distante, aucun site client ni deploiement de l'agent n'est implique.
La suite finale compte 3 370 tests reussis, 42 ignores, et 190 tests cibles reussis.
Les 48 nouveaux cas couvrent aussi le refus avant toute relecture d'ancien apercu.

## Canonical HTTP sur une page HTTPS (04/10/2026)

`https_canonical_cycle.py` teste maintenant directement
`canonical_from_https_to_http`, sur la fixture statique deja corrigee par les
cycles precedents. Le crawler fournit une proposition `http` vers `https`, mais
ce changement de protocole ne prouve pas a lui seul que la destination existe.
Le correcteur exige des observations HTML 200 coherentes pour la source et la
destination, conserve exactement l'hote, le port, le chemin et la query, et
refuse une destination non canonique ou `noindex`. Quand la destination est la
source elle-meme, son ancien canonical HTTP est justement la valeur a reparer.
Un maitre HTTP observe qui contredit cette destination interdit aussi l'upgrade.

Les paires validees sont liees a leur fichier de page par le resolveur existant.
Seule la balise canonical litterale change : ni alternate/hreflang, ni og:url,
ni lien de navigation. Un composant partage, une route inconnue ou une valeur
calculee restent sans repli modele. Les apercus libres par URL/GitHub et leur
confirmation sont refuses avant la relecture d'un ancien apercu, l'IA ou le
quota ; la correction etendue et le lot restent disponibles avec ces preuves.
Les anciens recus et historiques de facturation ne sont ni supprimes ni migres.

```sh
python ops/gauntlet/https_canonical_cycle.py --workdir <dossier-vide-temporaire>
```

Le cycle epingle la fixture `pployeraffiliation-a11y/noyaru-stack-static-html`
sur `8f7877b0ce4ad49202677e804944e50069bd1d85`, apres la PR #58 fermee sans
fusion. Il autorise un seul PUT, sur `gauntlet/canonical-http.html`, avec la
branche QA, le blob SHA et les octets attendus. Deux builds Netlify reussis et
deux vrais crawls comparent les memes 49 routes. L'anomalie passe de 1 a 0 ;
le commit de fixture `dd9cea5389a4ce6c1242696d63eb2f2bcae66ff8` ne change que
le protocole d'un attribut canonical. Les PR #59 et #60 sont fermees sans fusion,
la branche principale de la fixture reste inchangee, et aucun fournisseur IA
n'est appele. Le contre-controle frais est strictement en lecture seule.

Ce resultat n'est pas un site tout vert : `duplicate_titles` et
`duplicate_meta_descriptions` passent chacun de 32 a 33, et
`page_has_only_one_dofollow_incoming_internal_link_indexable` de 34 a 35.
Chaque nouveau temoin est cette seule page, desormais canonique dans le scoring
controle ; les textes et liens etaient deja presents et restent identiques.
La comparaison des corps servis conserve aussi l'injection native Netlify et
neutralise uniquement son identifiant de build observe, qui varie entre previews.

Les previews brutes gardent leur `X-Robots-Tag: noindex`. Leurs observations
sont reprojetees comme dans les cycles precedents pour tester les familles
indexables : ce n'est pas une preuve d'indexation en production. Le canonical
HTTP disparait egalement des observations brutes, sans retirer la page du crawl.
Cette preuve renforce une famille deja mentionnee dans un ancien bilan ; elle
ne ferme pas une nouvelle lacune de l'inventaire des references nommees.
Les limites, hashes, essais et resultats sont conserves dans
`validation-canonical-https-2026-10-04.json`. Aucune certification universelle,
aucun site client et aucun deploiement de l'agent ne sont inclus.
La suite finale compte 3 444 tests reussis et 42 ignores ; les 279 tests cibles
passent aussi. Les 74 nouveaux cas incluent les refus et les garde-fous du banc.

## Consolidation de copies sans canonical (05/10/2026)

`duplicate_pages_without_canonical` est une detection heuristique : le crawler
signale aussi des pages qui partagent seulement leur titre ou leur description.
Ces metadonnees seules ne permettent plus de choisir un maitre. Le correcteur
exige des observations HTML 200 coherentes, sans `noindex`, avec les memes
empreinte de contenu, H1, langue, images et liens. Il refuse les groupes
incomplets, les observations contradictoires et les ponts ambigus de metadonnees.
Un unique maitre deja auto-reference et observe est prioritaire ; sinon le choix
stable de l'URL la plus courte reste une convention soumise a revue humaine.
Cette famille ne peut pas etre fusionnee automatiquement, meme sans appel IA.

Avant le premier PUT, tous les fichiers du groupe et son eventuel maitre
existant doivent etre resolus sans ambiguite vers des pages HTML litterales.
Les corps actuels et leurs langues sont compares, les canonicals sont relus,
et le plafond doit permettre la correction complete. Scripts, templates,
`noindex`, base URL, attributs ambigus et sources devenues incompatibles sont
refuses sans repli IA. Les ecritures GitHub restent des operations par fichier :
une panne reseau peut laisser une branche partiellement modifiee, jamais une
transaction atomique garantie ni une fusion automatique.

La correction ajoute uniquement le canonical et aligne l'og:url litteral deja
present. Elle preserve les corps, les autres metadonnees, alternate/hreflang,
navigation et fins de ligne. Les apercus libres par URL/GitHub et leur
confirmation sont bloques avant ancien apercu, modele ou quota ; les parcours
etendu et groupe utilisent les memes preuves et ne facturent aucun appel IA.

```sh
python ops/gauntlet/duplicate_canonical_cycle.py --workdir <dossier-vide-temporaire>
```

Le cycle epingle `pployeraffiliation-a11y/noyaru-stack-static-html` sur
`dd9cea5389a4ce6c1242696d63eb2f2bcae66ff8`. Il ajoute deux vraies copies
et une page de controle sur une branche QA, avec navigation et sitemap : cinq
PUT de preparation strictement bornes. Il conserve les anciennes pages
`no-canonical-a` et `no-canonical-b` comme temoins negatifs car leurs contenus
different. Seuls `qa-dupe-a.html` et `qa-dupe-b.html` recoivent ensuite le
correctif. Deux builds et deux crawls reussis conservent les memes 52 routes.
Les occurrences selectionnees passent de 2 a 0 ; la famille globale passe de
4 a 2, les deux refus restent visibles. Les PR #61 et #62 sont fermees sans
fusion ; main, les redirections et les temoins negatifs restent inchanges.

Le resultat n'est pas tout vert : `sitemap_non_canonical_page` passe de 7 a 8
et `page_has_only_one_dofollow_incoming_internal_link_not_indexable` de 5 a 6.
Le nouveau temoin est `qa-dupe-b`, desormais consolide vers `qa-dupe-a` mais
encore liste dans le sitemap. Son retrait du sitemap est une correction
separee a valider, pas une reparation obtenue dans ce cycle.
Les previews brutes restent `noindex` et leur compteur de cette famille vaut
0 avant comme apres ; seule la projection controlee mesure 4 vers 2.
Les corps bruts prouvent bien l'ajout des canonicals, sans preuve d'indexation
en production. Le contre-controle frais est strictement en lecture seule et
neutralise uniquement l'identifiant natif du build Netlify pour comparer les
corps servis. Le bilan `validation-canonical-doublons-2026-10-05.json` conserve
les preuves, hashes et limites. Aucun client ni deploiement de l'agent n'est
inclus ; une reference nommee n'est pas une certification universelle.
La suite finale compte 3 557 tests reussis et 42 ignores ; les 283 tests cibles
passent aussi, avec 113 nouveaux cas. L'inventaire garde 203 cles, 65 familles
revendiquees et 64 proposees. Les familles sans reference nommee passent de
16 a 14 : la consolidation est mesuree, mais la seconde nouvelle mention
concerne justement le sitemap restant a corriger. Ce chiffre ne mesure donc
ni les reparations reussies ni la preparation aux premiers clients.

## Nettoyage des alias canonical du sitemap (05/10/2026)

`sitemap_non_canonical_page` dispose maintenant d'un chemin sans IA borne aux
sitemaps XML litteraux uniques. Chaque alias et chaque etape de sa chaine doivent
avoir des observations HTML 200 coherentes ; le maitre final doit etre canonique
et sans `noindex`. Boucles, observations absentes ou contradictoires, changement
d'hote et downgrade HTTPS sont refuses. Avant le PUT, les heads HTML actuels sont
relus via des routes non partagees, et leurs canonicals doivent encore correspondre
au rapport. Sources calculees, scripts, templates et sitemap engendre restent
sans repli IA. Les apercus libres ne contournent pas ces preuves.

Le parseur XML securise identifie les vrais noeuds et leurs offsets UTF-8, sans
toucher les commentaires ni confondre casse, query ou slash des chemins. Une
entree du maitre deja presente conserve son bloc complet, meme si l'alias apparait
avant elle. Si le maitre manque, seule une entree minimale est creee : les dates,
images et priorites de l'alias ne lui sont pas transferees. Les tests couvrent ce
second cas ; le cycle distant mesure uniquement des maitres deja presents.
Sitemap index, DTD, sources multiples ou ambigues et fichiers trop grands sont
refuses. Les autres familles de reecriture du sitemap restent inchangees.

```sh
python ops/gauntlet/canonical_sitemap_cycle.py --workdir <dossier-vide-temporaire>
```

Le cycle epingle la fixture sur `c31d4b1370735f7938b96bc45e841d81cb0b7fc8`,
apres la consolidation des copies. Un seul PUT modifie `sitemap.xml`, qui passe
de 55 a 49 entrees. Deux builds et deux vrais crawls conservent les memes 52
routes HTML et leurs champs observes. Les six occurrences selectionnees passent
de 6 a 0 ; la famille globale passe de 8 a 2. `blog` et `canonical-404` restent
listes faute de destination finale saine observee : leur refus n'est pas une
reparation. Les trois maitres, les autres entrees, les redirections et les
temoins du cycle precedent sont preserves. Aucun compteur ne progresse dans ce
cycle controle. Les PR #63 et #64 sont fermees en brouillon sans fusion, et main
reste inchangee. Le contre-controle frais ne fait aucune ecriture distante.

Les previews brutes restent `noindex` : leur compteur vaut 0 avant comme apres.
La projection controlee avec le sitemap servi mesure 8 vers 2, pas l'indexation
en production. Le bilan `validation-sitemap-canonical-2026-10-05.json` conserve
les hashes, essais et limites. Aucun appel IA, site client ni deploiement de
l'agent n'est implique. La suite finale compte 3 651 tests reussis et 42 ignores,
les 323 tests cibles passent aussi, avec 94 nouveaux cas. L'inventaire reste a
203 cles, 65 familles revendiquees, 64 proposees et 14 sans reference nommee :
cette famille etait deja mentionnee comme avertissement dans le bilan precedent.
Une reference nommee ne certifie ni toutes les occurrences ni tous les stacks,
et la preparation aux premiers clients reste a valider.

## URL redirigees dans le sitemap (05/10/2026)

`sitemap_3xx_redirect` utilise maintenant le meme parseur XML borne que le
nettoyage canonical. Une proposition ne suffit plus : sa source doit avoir une
chaine de redirection observee, complete et coherente, vers une destination HTML
200 observee independamment, canonique et sans `noindex`. Boucles, contradictions,
sauts externes, downgrade HTTPS et codes non suivables restent refuses sans IA.
Une destination deja listee conserve son entree complete ; les metadonnees de
l'ancienne URL ne la remplacent jamais. La creation minimale d'une entree absente
reste couverte par les tests synthetiques, pas par ce cycle distant.

Le chemin est volontairement limite au sitemap litteral unique et a un fichier
`_redirects` unique dans le meme dossier servi. Avant le PUT, chaque regle actuelle
doit encore correspondre aux sauts et aux codes observes. Une route ayant un
fichier statique exige une regle forcee. Cette precaution suit le comportement
de shadowing [documente par Netlify](https://docs.netlify.com/manage/routing/redirects/redirect-options/#force-redirects).
Les heads actuels des destinations sont aussi relus. Regles conditionnelles,
wildcards, placeholders, queries non prouvees, configuration concurrente et
sitemap engendre sont refuses ; ce n'est pas un correcteur universel de routing.
Les apercus libres et anciens apercus ne contournent pas ces preuves. Les
parcours etendu et groupe ne facturent aucun token et ne modifient que le XML.

```sh
python ops/gauntlet/sitemap_redirect_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `ab31afaddafdf86c26649a1f3e908647b22045ed` apres le nettoyage
canonical. Trois PUT de preparation ajoutent navigation, cinq entrees de sitemap
et trois regles de test, sans ajouter de page HTML. Un seul PUT correctif retire
les trois entrees prouvees : deux redirections simples et une chaine 302 puis
301. Le sitemap passe de 54 a 51 entrees, les occurrences selectionnees de 3 a
0, et la famille globale de 5 a 2. Les deux temoins negatifs restent listes :
`qa-sitemap-noindex` aboutit a une page `noindex`, et `qa-sitemap-noncanonical`
aboutit a une page dont le canonical designe un autre maitre. Leur refus ne
compte pas comme une reparation. Les deux maitres deja presents sont preserves.

Deux builds et deux crawls conservent les memes 52 routes HTML, tous les temoins
de redirection restent observes grace a la navigation, et aucun compteur
n'augmente entre ces deux rapports controles. Les regles, les contenus et les
temoins des cycles precedents restent inchanges par la correction. Les PR #65 et
#66 sont fermees en brouillon sans fusion, et main reste inchangee. La
contre-verification fraiche relit aussi les sources, les PR, les builds, les
reponses HTTP et les blocs XML conserves, sans ecriture distante ni appel IA.

Les previews natives gardent `noindex`. Leur sitemap sert des URL dans l'espace
de noms de production, avec zero seed dans le crawl brut : son compteur vaut
0 avant comme apres et ne prouve pas la correction. La projection controlee
avec le XML effectivement servi mesure 5 vers 2 ; les reponses HTTP et les octets
du XML sont contre-verifies independamment. Ce n'est pas un deploiement de
l'agent ni une preuve d'indexation en production. Le bilan est conserve dans
`validation-sitemap-redirections-2026-10-05.json`.

La suite finale compte 3 766 tests reussis, 42 ignores, et 440 tests cibles reussis,
avec 115 nouveaux cas. L'inventaire reste a 203 cles, 65 familles revendiquees et
64 proposees ; les familles sans reference nommee passent de 14 a 13, par une
nouvelle preuve dediee. Les references ne certifient ni toutes les occurrences
ni tous les stacks. Aucun site client, deploiement, merge main ou migration de
recus n'est implique. La prochaine validation portera sur les pages `noindex`
presentes dans le sitemap, puis sur les autres familles et l'onboarding client.

## Pages noindex dans le sitemap (05/10/2026)

`sitemap_noindex_page` exige maintenant une instruction generique meta robots
`noindex` explicite, unique et coherente dans des observations HTML 200. Le fichier
HTML actuel est relu avant toute ecriture : son head litteral doit encore porter
la meme instruction. Un en-tete seul ne suffit pas a ce chemin source, meme si
[Google prend aussi en charge le noindex HTTP](https://developers.google.com/search/docs/crawling-indexing/block-indexing).
Les faux marqueurs dans un commentaire, attribut, body, titre, template ou contenu
inerte, les robots contradictoires et les sources calculees sont refuses sans IA.

Le correcteur retire seulement les blocs XML exacts, en conservant les pages et
leur choix d'indexation. Casse, slash et query significatifs ne sont plus confondus
dans cette famille. Les autres entrees, metadonnees, extensions, commentaires et
octets restent intacts. Le sitemap doit etre litteral, unique et dans un dossier
statique connu. Les sitemaps engendres ne declenchent plus d'exclusion IA devinee.
Une entree redirigee exige une cible noindex observee independamment et des regles
actuelles litterales prouvees. Les refus restent nommes pour un sous-ensemble
partiel, et la note de premise impose une revue humaine meme en mode automatique.
Les parcours individuel et groupe partagent ces gardes ; les apercus libres ou
anciens ne les contournent pas, et aucun token IA n'est facture.

```sh
python ops/gauntlet/sitemap_noindex_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `751fafff93a5def142562f1a3d1bed6ba8930f85`, sans preparation
des sources. Un seul PUT retire `noindex-long`, `noindex-no-description` et
`qa-sitemap-noindex`, dont la 302 aboutit a `noindex-long`. Le sitemap passe de
51 a 48 entrees et la famille controlee de 3 a 0. Les deux builds et les deux
crawls conservent 52 routes HTML et 60 lignes de rapport, toutes les pages noindex
restent observees avec leurs balises intactes. La famille des URL redirigees dans
le sitemap passe aussi de 2 a 1 ; son autre temoin negatif reste liste. Aucun
compteur n'augmente, et les temoins canonical et doublons anterieurs restent
intacts. Les PR #67 et #68 sont fermees en brouillon sans fusion ; main ne change
pas. La contre-verification fraiche relit les SHA, builds, sources, reponses HTTP
et entrees XML, puis reproduit la preparation, sans ecriture distante ni appel IA.

Les previews natives gardent leur en-tete `noindex`. Le XML servi utilise les URL
de production, avec zero seed sitemap dans les crawls bruts : leurs compteurs
0 vers 0 ne prouvent rien. La projection controlee de l'hote/en-tete, avec le XML
reellement servi et les meta noindex explicites conservees, mesure 3 vers 0.
Cela ne prouve pas l'indexation en production et ne deploie pas l'agent. Le bilan
est dans `validation-sitemap-noindex-2026-10-05.json`.

La suite finale compte 3 881 tests reussis et 42 ignores, avec 636 tests cibles
reussis et 115 nouveaux cas. L'inventaire reste a 203 cles, 65 familles revendiquees
et 64 proposees ; les familles sans reference nommee passent de 13 a 12. Une
reference ne certifie ni toutes les occurrences ni tous les stacks. Les cas
header-only, Googlebot seul, generes, multi-sources ou calcules restent limites,
et les effets possibles sur les partenaires hreflang sont des avertissements,
pas une reparation multilingue atomique. Aucun site client ni deploiement n'est
implique. La prochaine validation porte sur les pages en erreur du sitemap.

## Pages en erreur dans le sitemap (05/10/2026)

`sitemap_4xx_page` ne retire plus toute URL signalee aveuglement. Seules les
404/410 directes HTML, observees sans contradiction, erreur de fetch ou blocage,
peuvent etre preparees. Les 401/403, 429, autres 4xx, 5xx et redirections finissant
en erreur restent intactes. Ce sous-ensemble prudent n'est pas la totalite de la
famille suivie. [Google distingue aussi le 429 des autres erreurs client](https://developers.google.com/crawling/docs/troubleshooting/http-status-codes).

Le sitemap doit etre litteral, unique et dans un dossier statique connu, sans
generateur. Une source de page resolue dans l'index du depot interdit le retrait,
tout comme une regle configuree sur cette URL, un routing ambigu ou concurrent.
Juste avant le PUT, le correcteur relit le statut HTTP avec GET, sans suivre les
redirections ni consommer le corps, en reutilisant la garde de cible publique.
La meme 404/410 directe HTML doit encore etre observable, sans `Retry-After`.
Une URL redevenue accessible ou un fetch impossible bloque toute l'ecriture.
Le parseur XML exact des phases precedentes conserve les autres entrees et leurs
octets ; aucune page n'est creee, aucun contenu, lien ou routing n'est modifie.
Le chemin etendu et le parcours groupe ne font aucun appel IA, les apercus libres
ne contournent pas les preuves, et une note de premise impose la revue humaine :
une absence observee ne prouve pas l'intention d'abandonner la page.

```sh
python ops/gauntlet/sitemap_error_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `86837f38ec7298bfff5a66659dcb765a46a9cc24`. Deux PUT de
preparation ajoutent au sitemap et a la navigation deux URL absentes, sans creer
de page. Les statuts 404 sont revalides avant un seul PUT correctif dans le XML.
Le banc traduit uniquement ces deux URL controlees vers leur hote de preview
observe, puis utilise la sonde HTTP du produit sans fabriquer de reponse. Le
sitemap passe de 50 a 48 entrees et la famille controlee de 2 a 0, avec deux builds
et deux crawls. Les memes 52 routes HTML saines et 62 lignes de rapport restent
observees ; les deux URL absentes restent en 404 et dans la navigation apres le
retrait. Les corrections et temoins negatifs anterieurs restent intacts, sans
hausse de compteur. Les PR #69 et #70 sont fermees en brouillon sans fusion et
main ne change pas. La contre-verification fraiche relit les sources, SHA, builds,
statuts HTTP, corps sains et XML servi, sans ecriture distante ni appel IA.

Les previews natives gardent leur politique `noindex`. Leur sitemap sert des URL
de production, avec zero seed sitemap dans le crawl brut : ses compteurs 0 vers 0
ne prouvent pas la correction. La projection controlee avec le XML effectivement
servi mesure 2 vers 0, et les HTTP/sources/octets sont contre-verifies separement.
Ce n'est pas une preuve d'indexation en production ni un deploiement de l'agent.
Le bilan est dans `validation-sitemap-erreurs-2026-10-05.json`.

La suite finale compte 4 002 tests reussis et 42 ignores, avec 757 tests cibles
reussis et 121 nouveaux cas. Les 410 et cas proteges/transitoires sont testes
synthetiquement, pas mesures sur toutes les previews ou stacks. L'inventaire
reste a 203 cles, 65 familles revendiquees et 64 proposees ; les familles sans
reference nommee passent de 12 a 11. Une reference ne certifie pas toutes les
occurrences. Les lectures Git et HTTP ne forment pas une transaction atomique
avec le deploiement ; une future restauration ou une absence temporaire reste
possible. Aucun site client ni merge main n'est implique. La prochaine etape
porte sur les URL HTTP presentes dans le sitemap d'un site HTTPS.

## URL HTTP du sitemap d'un site HTTPS (05/10/2026)

Une reponse HTTPS du domaine ne prouve pas celle de chaque chemin. Le correcteur
exige maintenant une observation directe HTML 200, canonique ou sans canonical,
sans `noindex` ni `none`. Il relit la source HTML litterale et les regles de
routing, puis revalide chaque destination HTTPS avant l'unique PUT XML. Un
temoin devenu indisponible bloque toute l'ecriture. Les apercus IA libres ou
anciens ne contournent pas ces preuves, et aucun appel Claude n'est necessaire.

Le parseur XML conserve les chemins, requetes, fragments distincts, commentaires,
extensions et entrees non visees. En cas de collision, l'entree HTTPS existante
reste entiere, avec ses propres metadonnees ; sans collision, seul son `<loc>`
HTTP change. Les sitemaps engendres, index, sources ambigues et alternates
multilingues non verifies d'une entree deplacee sont refuses.

```sh
python ops/gauntlet/sitemap_https_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `7a29cdf61167d4613d714abae6638b011bfe3716` : un PUT de preparation
et un PUT correctif, tous deux exclusivement dans `sitemap.xml`. Trois entrees
HTTP sont corrigees : deux collisions et une conversion conservant les
metadonnees. Deux temoins HTTP restent intacts, vers une page `noindex` et une
destination HTTPS 404. La famille controlee passe donc de **5 a 2**, avec les
trois occurrences visees de 3 a 0 ; le sitemap passe de 51 a 49 entrees. Deux
builds et deux crawls conservent les 52 routes HTML saines, les temoins manquants,
les corrections anterieures et leurs controles negatifs, sans hausse de compteur.
Les PR #71 et #72 sont fermees en brouillon sans fusion ; main reste intact.

Les previews natives gardent leur `noindex` et leurs URL de production : les
compteurs bruts 0 vers 0 ne prouvent aucune correction. Le banc controle seulement
leur hote et l'en-tete exact injecte par Netlify, sans inventer de statut ni de
corps. Le controle de cette famille, execute hors `_score_issues` par l'auditeur,
est recalcule a partir des vrais `<loc>` servis. La contre-verification relit les
sources, metadonnees, XML, builds et HTTP, et confirme que la sonde du produit
refuse toujours les previews natives. Ce n'est pas une preuve d'indexation en
production ni une validation globale du correcteur. Le bilan est dans
`validation-sitemap-https-2026-10-05.json` ; aucun site client n'est modifie.

## Pages indexables absentes du sitemap (05/10/2026)

`indexable_page_not_in_sitemap` ajoute uniquement les pages HTTPS observees en
HTML 200 direct, canoniques ou sans canonical, sans `noindex` ni `none`. Le
correcteur exige le vrai hote du projet, un sitemap litteral unique, une source
HTML actuelle non ambigue et des regles simples. Chaque page est recontrolee en
HTTP juste avant le PUT ; un temoin devenu non indexable bloque toute l'ecriture.
Les apercus IA ne contournent pas ces preuves. Aucun appel Claude n'est necessaire.

Le parseur ajoute des noeuds `<url><loc>...</loc></url>` minimaux, avec namespace
et echappement XML structures, sans resserialiser les entrees existantes. Aucun
`lastmod`, `priority` ou hreflang n'est invente. Les octets, metadonnees et doublons
non vises restent intacts ; une seconde correction est sans effet. Sitemaps
engendres, index, DTD, XML ambigu, texte mixte et racine autofermee sont refuses.

```sh
python ops/gauntlet/sitemap_add_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `b347b9b98437fd679d0f3c55ea0c1ee2b7ad9e2d` et retire trois
entrees completes, puis le correcteur ajoute seulement leurs loc manquants : le
sitemap passe de **46 a 49** entrees et la famille controlee de **3 a 0**. Les
46 anciennes entrees restent intactes ; les metadonnees retirees par le setup
ne sont pas devinees lors de l'ajout. Deux builds et deux vrais crawls conservent
52 routes HTML saines, les corrections anterieures et les controles negatifs,
sans hausse des autres compteurs. Les PR #75 et #76 sont fermees en brouillon
sans fusion ; main reste intact. Les sondes immediates portent sur les trois pages.

Le premier cycle #73/#74 reste classe en echec : son comparateur refusait les
exemples des sondes HTTP/www dont le numero de preview changeait. Le banc accepte
desormais seulement cette variation d'hote, sur les sondes racine des deux
previews exactes possedees. Protocole, www, chemin, requete, fragment, port,
identifiants, hote tiers et compteurs ne peuvent pas etre masques. Les observations
de production et les rapports bruts restent inchanges. 27 tests encadrent ce
correctif, suivi d'un nouveau cycle complet reussi ; l'ancien echec est conserve.

Les previews natives gardent leur `noindex` : les compteurs bruts 0 vers 0 ne
prouvent aucune reparation. La projection controlee et le XML reellement servi
mesurent 3 vers 0. Une contre-verification en lecture seule relit les sources,
XML, builds et HTTP, et confirme six refus par la sonde normale du produit sur
les previews natives. Ce n'est pas une preuve d'indexation en production.

La suite complete passe **4 281 tests**, avec 42 ignores ; la suite ciblee passe
1 062 tests. 142 cas sont ajoutes dans cette etape. L'inventaire passe de dix a
neuf familles revendiquees sans mention exacte dans un bilan : une mention
n'est pas une reparation prouvee. Le bilan est dans
`validation-sitemap-ajouts-2026-10-05.json`. Aucun site client n'est modifie,
l'agent n'est pas deploye et la mise en service globale n'est pas certifiee.

## Viewport absent (05/10/2026)

`viewport_not_set` ne delegue plus l'ajout d'une balise connue a un modele. La
correction exige des observations HTTPS HTML 200 directes, canoniques ou sans
canonical et indexables, avec un compteur viewport explicitement entier a zero.
Elle relit la source HTML litterale de chaque page, sa canonical et son routing.
Les comptages inconnus, temoins contradictoires, sources calculees, templates,
scripts et routes ambigues sont refuses sans repli IA. Un viewport deja present,
meme vide ou personnalise, n'est pas remplace ni duplique.

Seule la balise `<meta name="viewport" content="width=device-width, initial-scale=1" />`
est ajoutee a la fin du head litteral. Les octets existants, commentaires et fins
de ligne restent intacts ; les sources et sorties sont bornees a 80 000 octets
UTF-8. Toutes les sources et sondes HTTPS du lot sont verifiees avant le premier
PUT. Le plafond doit couvrir tout le lot ; une erreur d'ecriture arrete les PUT
suivants et laisse les fichiers non ecrits explicitement signales. Les ecritures
multifichiers ne sont pas atomiques. Les deux endpoints profonds utilisent ce
chemin gratuit sans IA ; les apercus libres ou anciens ne le contournent pas.

```sh
python ops/gauntlet/viewport_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `e42e0f58eb69128317c2ec912f3ce92d33c7e82a`, sans PUT de setup.
Un seul PUT ajoute une seule balise a `gauntlet/viewport-not-set.html`. Deux
builds et deux vrais crawls mesurent la famille controlee de **1 a 0**, avec les
52 routes HTML saines et 62 lignes de rapport conservees. Sitemap, navigation,
canonical, balises sociales, viewports sains et corrections precedentes restent
intacts, sans hausse d'autre compteur. Les PR #77 et #78 sont fermees en brouillon
sans fusion et main reste intact.

La contre-verification relit les sources, SHA, builds, XML et HTTP reels sans
ecriture. Le HTML servi ne change que par la balise attendue, avec uniquement
l'identifiant de deploy Netlify possede normalise. L'ajout est idempotent et
deux nouvelles sondes normales refusent bien les previews natives `noindex`.
Les compteurs bruts 0 vers 0 ne prouvent aucune reparation : seul le recrawl
controle et ses preuves de source/HTTP mesurent 1 vers 0. Le layout mobile en
navigateur, le DOM JavaScript et l'indexation de production ne sont pas verifies.

La suite complete passe **4 418 tests**, avec 42 ignores ; la suite ciblee passe
1 232 tests. 137 cas sont ajoutes. L'ancien temoin du lot de sept familles fournit
maintenant les observations requises, sans affaiblir son assertion. L'inventaire
passe de neuf a huit familles sans mention exacte dans un bilan ; une mention
n'est pas une reparation prouvee. Le diagnostic historique de canonical absente
reste volontairement non propose : aucune nouvelle tache n'est activee pour
reduire artificiellement ce chiffre.

Le bilan est dans `validation-viewport-2026-10-05.json`. Les frameworks et sources
generees restent a valider separement. Aucun site client n'est modifie, l'agent
n'est pas deploye et sa mise en service globale n'est pas certifiee.

## Twitter Card absente (05/10/2026)

`twitter_card_missing` suit maintenant un chemin mecanique sans IA, partage par
les corrections individuelle et groupee. Chaque page doit avoir des observations
coherentes HTTPS HTML 200 directes, indexables et de l'hote du projet. Les champs
Twitter doivent etre connus, et titre, description et image doivent deja exister
sur cette meme page, directement ou via ses propres balises OG et metadonnees.
L'exemption de l'auditeur pour les pages OG seules est conservee.

Le correcteur relit le fichier HTML litteral et le routing actuels, puis verifie
la reponse HTTPS servie : type de carte toujours absent, memes valeurs sociales,
canonical et indexabilite coherentes. Les temoins contradictoires, champs inconnus,
sources partagees ou non litterales, scripts, templates, doublons, balises vides,
routes ambigues et images relatives, HTTP ou locales sont refuses sans repli IA.
Une image n'est jamais inventee ou empruntee a une autre page. Les valeurs deja
presentes, y compris `twitter:image:src`, ne sont ni remplacees ni dupliquees.

Seules les balises manquantes sont ajoutees : `twitter:card=summary_large_image`
et les champs absents pour lesquels cette page fournit deja une valeur utilisable.
Les attributs sont echappes, les octets et fins de ligne existants sont conserves,
et l'ajout est idempotent. Les limites de 80 000 octets, le plafond couvrant tout
le lot et les verifications avant le premier PUT restent communs au viewport.
Les ecritures sont protegees par SHA, mais un lot multifichier n'est pas atomique.
Les apercus libres ou anciens ne peuvent pas contourner ce chemin.

```sh
python ops/gauntlet/twitter_card_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `21bf75fcbca71d940d8b20291da385b8705bc348`, sans PUT de setup.
Un seul PUT ajoute le type de carte et la description OG deja disponible a
`gauntlet/twitter-missing.html`, en gardant son titre et son image Twitter.
Deux builds et deux vrais crawls mesurent **1 carte absente a 0**, dans les rapports
bruts comme controles : les trois cartes incompletes existantes restent a trois.
Les 52 routes HTML saines et 62 lignes de rapport restent presentes, ainsi que
les cartes saines, viewports, sitemap, canonical, navigation et corrections
precedentes. Les PR #79 et #80 sont fermees en brouillon sans fusion ; main reste intact.

La contre-verification en lecture seule relit les SHA, builds, sources, XML et
HTTP reels, puis reprepare le correctif sur le code final et verifie l'idempotence.
Le HTML servi ne change que par les deux lignes attendues, avec uniquement
l'identifiant de deploy Netlify possede normalise. L'image PNG existante est
verifiee identique sur les deux builds ; ce controle d'asset concerne uniquement
cette fixture, pas toutes les images des futurs sites.

La suite complete finale passe **4 623 tests**, avec 42 ignores ; la suite ciblee
passe **1 491 tests**. Les refus et regressions comprennent 205 nouveaux cas.
L'inventaire passe de huit a sept familles sans mention exacte dans un bilan,
sans promouvoir le diagnostic historique de canonical absente volontairement cache.
Les previews gardent leur `noindex` natif : deux nouvelles sondes
normales les refusent. La projection d'hote/en-tete de ce banc reste strictement
bornee a la fixture possedee. Le rendu reel sur X, les dimensions de chaque image,
le DOM JavaScript et l'indexation de production ne sont pas verifies.

Le bilan est dans `validation-twitter-card-2026-10-05.json`. Les cartes incompletes
et les frameworks restent a valider separement. Une mention dans un bilan n'est
pas une reparation prouvee. Aucun site client n'est modifie, l'agent n'est pas
deploye et sa mise en service globale n'est pas certifiee.

## Langue HTML issue du self hreflang (05/10/2026)

`hreflang_defined_but_html_lang_missing` dispose d'un chemin mecanique sans IA,
commun aux corrections individuelle et groupee. La page doit etre HTML HTTPS 200
directe, indexable et self-canonical sur l'hote du projet, avec des observations
coherentes attestant l'absence de `lang` et `served_lang`. Une seule annotation
`hreflang` litterale non `x-default` doit designer cette URL exacte. Aucune langue
n'est devinee depuis le texte, l'URL, les reglages du projet ou une autre page.

Le correcteur relit le fichier et son routing actuels, puis verifie la reponse
HTTPS servie : langue toujours absente et memes annotations, canonical et
indexabilite. Champs inconnus, observations contradictoires, doublons ou langues
auto-referencees concurrentes, sources non litterales ou partagees, scripts,
templates, routes ambigues et tout attribut `lang` ou `xml:lang` deja present
(meme vide) sont refuses sans repli IA. Le lecteur et la sonde HTTPS precedents
gardent leur refus par defaut pour les documents sans langue : seule cette
nouvelle correction active l'option qui permet de les inspecter.

Un seul attribut est insere dans la vraie balise racine `<html>`, en gardant tous
les autres octets, fins de ligne et liens `hreflang`. Les commentaires contenant
`<html>` ne sont pas des balises actives. Les positions ambigues et les sources
ou sorties au-dessus de 80 000 octets sont refusees. Les controles de tout le lot
finissent avant le premier PUT ; chaque ecriture est protegee par SHA, mais les
lots multifichiers ne sont pas atomiques. Les apercus libres ou anciens ne
contournent pas ce chemin, et une seconde application n'ajoute rien.

```sh
python ops/gauntlet/hreflang_lang_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `1c0c40130306afcf200c0e54a2cca8fce8c480cc`, sans PUT de setup.
Un seul PUT ajoute `lang="fr"` a `gauntlet/hreflang-no-html-lang.html` depuis son
annotation francaise existante. Deux builds et deux vrais crawls mesurent
**1 anomalie de cette famille a 0**, dans les rapports bruts comme controles.
Le compteur general de langue HTML absente passe de **2 a 1** : l'autre page
reste volontairement inchangee faute d'annotation auto-referencee. Les 52 routes,
62 lignes de rapport, 49 entrees sitemap, metadonnees et corrections precedentes
restent presentes. Les PR #81 et #82 sont fermees en brouillon sans fusion ;
main reste intact et tous les liens `hreflang` restent identiques.

La contre-verification en lecture seule relit les SHA, builds, sources, XML et
HTTP reels. Un parseur independant localise la racine malgre le commentaire
contenant du HTML, compare les annotations et le HTML entier, puis reprepare
le correctif et controle son idempotence contre les empreintes du code teste.
Seul l'identifiant de deploy Netlify possede est normalise entre les deux builds.
Les previews conservent leur `noindex` natif : deux nouvelles sondes ordinaires
les refusent. La projection d'hote/en-tete du banc reste strictement bornee au
site de test ; elle ne prouve pas l'indexabilite en production.

La suite complete finale passe **4 825 tests**, avec 42 ignores ; la suite ciblee
passe **1 744 tests**. Les nouveaux controles comprennent 202 cas. L'inventaire
passe de sept a six familles sans mention exacte dans un bilan ; cette mention
n'est pas une certification. Le diagnostic historique de canonical absente reste
volontairement cache et n'est pas active pour reduire ce chiffre.

Le bilan est dans `validation-hreflang-lang-2026-10-05.json`. Le contenu linguistique,
le registre complet des codes, les destinations et la reciprocite des traductions,
les frameworks, le DOM JavaScript et l'indexation de production restent a valider
separement. Aucun site client n'est modifie, l'agent n'est pas deploye et sa mise
en service globale n'est pas certifiee.

## Destination canonical du hreflang (07/10/2026)

`hreflang_to_non_canonical` utilise un chemin mecanique sans IA pour un groupe
bilingue deja declare et observe. Trois URL HTTPS directes du meme hote doivent
etre verifiees : source self-canonical, alias non canonical et destination
self-canonical. Les langues, annotations et retours doivent former le meme
groupe a deux langues ; une canonical seule ne prouve pas une traduction.
Les observations manquantes, contradictoires ou ambigues sont refusees, avec
les pages non corrigees nommees lorsqu'un sous-ensemble est accepte.

Le correcteur relit le fichier HTML litteral, son SHA et le routing, puis sonde
les trois pages servies avant toute ecriture. Seule la valeur du vrai attribut
`href` de l'annotation concernee change. Canonical, langue, navigation, OG,
commentaires et autres octets restent identiques. Doublons, attributs ambigus,
scripts/templates, sources partagees ou frameworks, routes incertaines et
contenu perime sont refuses sans repli IA. Les apercus libres ou anciens ne
contournent pas ce chemin. Les controles couvrent tout le lot avant le premier
PUT ; chaque ecriture est protegee par SHA, sans atomicite multifichier.

```sh
python ops/gauntlet/hreflang_canonical_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `a055e8ee5c2843ed4140a4509560034715cd2b93`. Quatre PUT de setup
ajoutent trois controles et leurs liens dans l'index de la fixture possedee.
Un seul PUT correctif remplace l'alias anglais dans le hreflang de la page
francaise. Deux builds et deux crawls du 05/10/2026 observent 55 routes HTML,
65 lignes de rapport et les memes 49 entrees sitemap. Les PR #83 et #84 sont
fermees en brouillon sans fusion ; main reste intact.

Le premier verdict du banc est **en echec et conserve** : son assertion exigeait
que la reciprocite reste inchangee, alors que le meme href corrige aussi le
retour de la destination prouvee. Le banc final exige exactement cet effet et
ses exemples, sans assouplir les autres gardes. La reevaluation des builds
existants est en lecture seule, sans nouvelles ecritures ni modification du
produit teste. Dans les rapports controles, la famille passe de **3 a 2**,
l'occurrence selectionnee de **1 a 0** et la reciprocite de **2 a 1**. Les deux
anciens controles ambigus restent refuses. Aucun autre compteur n'augmente.

Ces deux familles restent a **0 a 0 dans les rapports bruts** : les previews
portent `noindex` et sont filtrees par ces controles d'indexabilite. Les chiffres
3 a 2 et 2 a 1 viennent uniquement de la projection d'hote/en-tete strictement
bornee au site possede. Elle ne prouve pas l'indexabilite en production.
La contre-verification fraiche du 07/10/2026 relit les SHA, sources, builds,
XML et six reponses HTML entieres. Un parseur independant derive le changement
de href ; seul l'identifiant de deploy Netlify possede est normalise. Six
sondes ordinaires refusent reellement les previews natives. Les huit empreintes
du code/test final concordent et l'idempotence ainsi que les refus sont controles.

La suite complete finale passe **5 058 tests**, avec 42 ignores ; la suite ciblee
passe **2 012 tests**. Cette phase ajoute 233 cas. Le test CI existant demande
explicitement un lot couvrant sa base partagee ; le plafond et le fonctionnement
du planificateur produit ne changent pas. Ce n'est pas un test de capacite.
L'inventaire passe de six a cinq familles sans mention exacte dans un bilan :
une mention n'est toujours pas une reparation certifiee et le diagnostic
historique cache n'est pas active pour reduire ce chiffre.

Le bilan est dans `validation-hreflang-canonical-2026-10-07.json`, avec l'echec
initial, la reevaluation et la contre-verification distincts. La semantique des
traductions, les groupes plus larges, frameworks, DOM JavaScript, Claude reel,
PostgreSQL et l'indexation de production restent a valider separement. Aucun
site client n'est modifie et la mise en service globale n'est pas certifiee.

## Retrait d'une fausse langue hreflang (07/10/2026)

`page_referenced_for_more_than_one_language_in_hreflang` dispose d'un plan
protege sans IA : le rapport seul ne suffit plus a autoriser une suppression.
Deux pages HTTPS directes, indexables et self-canonical du meme hote doivent
etre observees avec leurs langues et annotations. La source pointe deux codes
de langues primaires differentes vers une meme cible. Cette cible doit declarer
sa propre langue et un retour vers la source. Seul le code qui contredit sa
langue actuelle peut etre retire ; les groupes inconnus ou ambigus sont refuses.

Le correcteur relit le HTML litteral, son SHA et le routing, puis sonde la source
et la cible servies avant tout PUT. Le canonical actuel doit aussi correspondre
litteralement : un fragment ne peut pas etre masque par la normalisation d'URL.
Seule la vraie balise de head visee est supprimee ; les autres octets, espaces,
fins de ligne, langue racine, canonical, annotation correcte, navigation et
metadonnees restent identiques. Les scripts/templates, attributs ambigus,
sources partagees/frameworks et routes incertaines sont refuses sans repli IA.
Les apercus libres ou anciens ne contournent pas ce chemin. Le controle couvre
tout le lot avant la premiere ecriture, sans atomicite multifichier.

```sh
python ops/gauntlet/hreflang_drop_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `14ba42a4104fb9439d8045c811846a0a7ef6660a`. Quatre PUT de setup
ajoutent trois controles et leurs liens d'index. Un PUT retire la fausse balise
francaise de `gauntlet/qa-hreflang-drop-fr.html`, sans toucher l'annotation anglaise.
Deux builds et deux crawls mesurent **2 anomalies a 1 dans le rapport controle**,
avec le cas selectionne de **1 a 0**. Le temoin vers une cible sans langue connue
reste a un, nomme et refuse. Les 58 routes HTML, 68 lignes, 49 entrees sitemap et
corrections precedentes sont conservees ; aucun autre compteur n'augmente.
Les PR #85 et #86 sont fermees en brouillon sans fusion ; main reste intact.

La famille reste a **0 a 0 dans les rapports bruts**, les previews natives
portant `noindex`. La projection d'hote/en-tete est strictement bornee au site
possede. La contre-verification en lecture seule relit les SHA, builds, sources,
XML et HTML entier. Un parseur independant derive la seule suppression ; seul
l'identifiant de deploy Netlify possede est normalise. La cible sans langue est
relue sur les deux builds et six sondes normales refusent vraiment les previews.

La suite complete passe **5 269 tests**, avec 42 ignores ; la suite ciblee passe
**2 756 tests**, avec deux ignores. Cette phase ajoute 211 cas. Les huit empreintes
du code/test fige concordent avec les suites et la contre-verification. Le bilan
est dans `validation-hreflang-drop-2026-10-07.json` ; l'inventaire passe de cinq a
quatre familles sans mention exacte, sans activer le diagnostic historique cache.

Cette suppression ne reconstitue pas un groupe hreflang complet : aucune balise
francaise auto-referencee n'est ajoutee a la source. La configuration complete
demande aussi les annotations de chaque version, elle-meme comprise, selon la
[documentation Google](https://developers.google.com/search/docs/specialty/international/localized-versions).
La baisse du compteur ne certifie donc ni ce groupe complet ni l'indexabilite.
Semantique des traductions, frameworks, DOM JavaScript, Claude reel, PostgreSQL
et mise en service globale restent a valider. Aucun site client n'est modifie.

## Reecriture locale des ancres (07/10/2026)

Le reecriveur de `links_with_no_anchor_text` utilisait une regex qui confondait
`data-href` avec `href` et modifiait des commentaires ou du texte de scripts.
`backend/anchor_text.py` lit maintenant les vraies balises HTML et leurs attributs
litteraux, puis insere seulement l'attribut `aria-label` au bon offset.
Les autres octets, guillemets, casse, href et fins de ligne restent identiques.
Noms contradictoires, preuves mal typees, balises dupliquees/mal fermees,
contextes non rendus et contenu dynamique ambigu sont refuses. Un nom existant,
y compris `aria-labelledby`, le texte alternatif d'une image ou un autre lien
nomme avec le meme href litteral, n'est pas ecrase. Source et resultat sont bornes
a 80 000 octets UTF-8 ; une seconde passe n'ecrit rien.

```sh
python ops/gauntlet/anchor_text_rewriter_check.py --workdir <nouveau-dossier-temporaire>
```

Ce controle Chromium est **local et sans reseau** : il verifie le vrai nom
accessible du lien selectionne, cinq autres liens inchanges et des captures
avant/apres identiques en 1280x800 et 390x844. Les comparaisons DOM, le changement
exact et l'idempotence ont leurs propres tests de refus des faux succes.
L'egalite des pixels concerne ce temoin, pas un CSS client utilisant cet attribut.
Un nom accessible ne garantit pas son utilisation comme ancre par Google.

La suite complete passe 5 422 tests, avec les memes 42 ignores ; la suite ciblee
passe 1 602 tests sur 46 modules, avec deux ignores. Les 153 nouveaux cas ne
remplacent aucun ancien test. Les cinq empreintes source/test du code final
concordent entre les deux suites et le controle Chromium.

Le bilan `anchor-text-rewriter-check-2026-10-07.json` est volontairement distinct
des fichiers `validation-*.json` : **cette phase ne certifie pas la famille** et
ne reduit pas les quatre absences de mention de l'inventaire. La preparation
actuelle consomme encore une preuve de rapport sans revalider la cible servie.
Fraicheur source/cible, correspondance des routes, composants partages,
frameworks et cycle deployee/crawl restent a securiser et a mesurer. Aucun site
client, fixture distante ou main n'est modifie par ce controle.

## Ancres avec preuves source et cible (07/10/2026)

Cette phase remplace le callback de rapport ci-dessus par un plan protege de
`links_with_no_anchor_text`. Source et destination doivent etre deux pages HTTPS
directes du meme hote, indexables et auto-canoniques, avec langue racine,
titre/H1 et comptes explicites. Les lignes de liens doivent prouver le href
litteral, sa cible, sa source et son absence de nom. Les propositions mal typees,
contradictoires ou non observees sont refusees sans IA ; un sous-ensemble accepte
nomme les pages laissees intactes.

Le correcteur individuel et par lot relit les fichiers/SHA actuels, meme avec
un cache, et exige une route HTML litterale unique non partagee pour chaque
source et chaque cible. Routage, metadonnees et liens actuels sont controles,
puis source et cible sont resondees en HTTPS avant le premier PUT du lot.
Le plafond porte sur les fichiers sources ecrits ; la cible qui donne le nom
reste en lecture seule. Les anciens apercus IA ne peuvent pas contourner ce
chemin. Seul `aria-label` est insere ; aucun autre octet, commentaire, href ou
contenu visible n'est change. Les erreurs multifichiers restent explicites,
sans promesse d'atomicite ni cout IA pour ces corrections mecaniques.

```sh
python ops/gauntlet/anchor_text_cycle.py --workdir <dossier-vide-temporaire>
```

Le banc epingle `d949b4d86a2eacc123a4ec475bf2def4f35df863`. Quatre PUT ajoutent
trois controles et leurs liens d'index ; un PUT corrige seulement l'ancre de
`gauntlet/qa-anchor-source.html`. Deux builds et deux crawls mesurent **2 a 1**
dans les rapports bruts et controles, et **1 a 0** pour le lien selectionne.
Le lien vers une cible sans langue racine connue reste a un, refuse et intact.
Les 61 routes HTML, 71 lignes et 49 entrees sitemap sont conservees ; aucun
autre compteur n'augmente. Les PR #89/#90 sont fermees en brouillon sans fusion.
Le premier essai #87/#88 reste en echec : le banc attendait a tort `served_lang`
sur des pages sans hreflang. Aucun champ absent n'a ete invente pour le faire passer.

La projection d'hote et du seul en-tete natif `noindex` reste bornee au site
possede dans le banc. Une contre-verification en lecture seule relit les SHA,
builds, HTML entier et XML ; un parseur independant derive l'insertion exacte.
Six vraies sondes normales refusent les previews natives. Chromium verifie
le nouveau nom accessible et les trois autres liens inchanges ; les captures
avant/apres des corps servis sauvegardes sont identiques en 1280x800 et 390x844.
Cela ne certifie pas un DOM JavaScript client ni un CSS reagissant a cet attribut.

La suite finale passe **5 671 tests**, avec les memes 42 ignores ; la suite
ciblee passe **3 002 tests** sur 62 modules, avec deux ignores. Les 249 nouveaux
cas s'ajoutent aux anciens, tous conserves. Les neuf empreintes source/test
figees concordent avec les deux suites, le cycle et la contre-verification.
Le bilan `validation-anchor-text-2026-10-07.json` reduit les absences de mention
exacte de quatre a trois, sans transformer les mentions en verdict de reussite
globale ni activer le diagnostic historique cache.

Seul le sous-ensemble litteral prouve est mesure, pas la famille complete ni
tous les frameworks. Les sources de depot script/template sont refusees ;
le lecteur HTTP existant tolere les scripts de body mais n'atteste pas leur
execution. Indexabilite de production, Claude reel, PostgreSQL et preparation
globale aux clients restent a valider. Aucun site client ni main n'est modifie.

## Descriptions courtes : extrait existant prouve (08/10/2026)

`short_description_cycle.py --workdir EMPTY_TEMP_DIRECTORY` part du SHA
`05ff4c231528a024f7f639a625edd1a5058dbd1e` du seul site HTML possede.
Il ajoute trois temoins : description courte, description absente et langue
racine inconnue. Chaque cycle autorise quatre PUT de preparation, deux PUT
correctifs et au maximum deux appels Claude, sans reprise ni autre fournisseur.
Les PR restent en brouillon et sont fermees sans fusion ; main reste intact.

Le correcteur exige des observations typees coherentes, une page HTTPS directe
indexable auto-canonique, une route HTML non partagee et des octets/SHA actuels.
Toutes les sources et leurs paragraphes sont revus par HTTP avant le modele ;
toutes les selections du lot precedent le premier PUT. Claude ne redige rien :
il choisit un indice entier parmi des extraits existants de 100 a 160 caracteres,
termines par une ponctuation. Le lexer commun borne le seul attribut content
modifie ou l'unique balise head ajoutee. Cache obsolete, ambiguite, contexte
masque, doublon connu et repli fichier libre sont refuses. Les descriptions
longues sans echantillon litteral valide ne permettent pas de contourner ce
plan ; l'application interne respecte aussi ce refus avant lecture ou IA.
Les fichiers choisis avec IA restent factures et soumis a relecture, y compris
lorsqu'ils ne contiennent qu'un extrait. Un PUT partiel n'est pas atomique.

Le bilan `validation-short-description-2026-10-08.json` conserve les mesures,
les tentatives precedentes et les empreintes. La suite finale passe 5 894 tests
avec les memes 42 ignores ; les 223 nouveaux cas conservent tous les anciens.
La suite ciblee passe 3 311 tests sur 71 modules, avec deux ignores. Le dernier
cycle conserve les 64 routes HTML et les 49 entrees XML ; les deux sources
selectionnees passent de deux anomalies a zero, mais la famille entiere reste
a trois occurrences. Les PR 95/96 sont fermees sans fusion. Les sept empreintes
figees concordent avec les suites, le cycle et la contre-verification fraiche.
Six sondes normales refusent reellement les previews natives ; Chromium lit
les balises head et conserve six paires de captures identiques en 1280x800 et
390x844. Trois cycles ont utilise six appels Claude au total, dont deux sur la
version finale. Le nettoyage du harness ne retire que ses propres donnees de
test ; aucun plafond produit ni ancien test ignore n'est modifie.
La preuve porte sur le contenu
litteral publie, pas sur sa pertinence editoriale ni sur une visibilite CSS
universelle. Les scripts de body servis ne sont pas executes dans l'attestation
HTTP ; les sources script/template de depot sont refusees. Aucun temoignage
de ce banc ne certifie tous les frameworks, PostgreSQL, l'indexabilite de
production ou la preparation globale aux clients.

## Diagnostic historique canonical : retrait et refus (08/10/2026)

`missing_canonical` etait deja masque, mais le correcteur le revendiquait encore
et le conseil promettait un ajout systematique. Cette revendication est retiree,
pas transformee en nouvelle reparation. Google peut choisir une URL canonique
sans declaration explicite ; l'absence seule ne prouve pas un defaut a corriger
([Search Central](https://developers.google.com/search/docs/crawling-indexing/consolidate-duplicate-urls?hl=fr)).
Les doublons prouves gardent leur famille et leurs protections propres.

Les variantes historiques restent dans le rapport brut mais sont masquees.
Les API individuelles, apercus et confirmations refusent avant journal/cache,
quota, GitHub ou IA, apres controle d'acces au projet. Preparation, execution
interne et patch direct refusent aussi, meme avec un plan forge. Le groupe de
refus est maintenant surveille par la garde existante des branchements ; aucune
exception de couverture ni aucun test ignore n'est ajoute.

`missing_canonical_policy_cycle.py --workdir EMPTY_TEMP_DIRECTORY` lit seulement
la branche deja construite de la PR 96, fermee en brouillon sans fusion, au SHA
`3a4a8c19c1d47c6d8a9453b5288545ec54349c8c`. Deux crawls conservent les 64 routes
HTML, les autres comptes/exemples et le diagnostic a deux occurrences : 2 -> 2,
pas une correction. Sources, SHA de blobs, corps HTTPS et XML sont inchanges.
Les 49 entrees XML representent 48 URL distinctes ; le doublon volontaire de
page absente reste intact. Les previews natives gardent leur noindex ; seule
la mesure possedee utilise la projection de test existante, sans exemption produit.

Le bilan `validation-missing-canonical-policy-2026-10-08.json` conserve aussi les
premiers echecs du banc et les empreintes. La suite finale passe 6 009 tests,
avec les memes 42 ignores ; les 115 nouveaux cas conservent les 5 936 anciens.
La suite ciblee passe 3 492 tests sur 77 modules, avec deux ignores. Les neuf
empreintes concordent entre les suites, le cycle et la contre-verification
independante par HTMLParser/XML. Aucun appel IA, aucune ecriture distante,
aucune nouvelle branche ou PR, aucun site client ni main modifies.

L'inventaire passe de 65 a 64 familles revendiquees, pour les memes 64 familles
visibles proposees. Les absences de mention exacte passent de deux a une par
retrait du diagnostic cache, pas par preuve d'une reparation supplementaire.
Cela ne certifie ni toutes les anomalies ni tous les frameworks, PostgreSQL,
l'indexabilite de production ou la preparation globale aux premiers clients.

## Donnees structurees : controles locaux et refus (08/10/2026)

Les familles `structured_data_schema_org_validation_error` et
`structured_data_google_rich_results_validation_error` restent visibles, avec
leurs codes et compteurs, mais ne promettent plus une correction automatique.
Le crawler mesure du JSON invalide, un type manquant et quatre cas de FAQ
incomplete ; il n'appelle aucun validateur externe. L'ancien correcteur
convertissait surtout les prix en nombres puis pouvait solliciter une IA libre.
Or [price accepte aussi Text](https://schema.org/price) et
[Google a arrete les resultats enrichis FAQ le 7 mai 2026](https://developers.google.com/search/updates?hl=fr).
Le type voulu et les reponses manquantes exigent des donnees verifiees.

Preparation, patch direct, execution interne et anciennes confirmations
refusent avant GitHub, modele ou facturation. Les API verifient d'abord l'acces
au projet et refusent avant journal/cache et quota. Le lot normal exclut ces
familles. Les anciens cas de branchement prouvent desormais le refus du mauvais
convertisseur, sans suppression de test ni nouvel ignore.

`structured_data_policy_cycle.py --workdir EMPTY_TEMP_DIRECTORY` utilise huit
routes HTTP possedees sur loopback et le vrai extracteur du crawler. Apres
conseil, les erreurs JSON/type restent a 2 et les FAQ a 4 ; les deux prix valides
restent sans erreur. Les octets ne changent pas et les serveurs sont arretes.
Ce n'est ni une correction ni une attestation HTTPS publique. Une lecture
distante independante de la PR 96 fermee, non fusionnee, conserve le prix texte,
le type manquant et le noindex natif : compte JSON/type 1 -> 1, FAQ 0 -> 0.
Cette fixture distante ne contient pas de temoin FAQ positif.

Le bilan `validation-structured-data-policy-2026-10-08.json` conserve les preuves,
les premieres tentatives et les empreintes. La suite complete passe 6 164 tests
avec les memes 42 ignores ; les 155 nouveaux cas conservent les 6 051 anciens.
La suite ciblee passe 3 663 tests sur 81 modules, avec deux ignores. Les 36
anciens bilans sont inchanges. Aucun appel IA ni ecriture distante, aucun site
client ni main modifies. L'inventaire passe de 64 a 62 familles automatiques
visibles et de une a zero absences de mention exacte par retrait de promesses,
pas par preuve de reparations supplementaires. Cela ne certifie pas la
preparation globale aux clients, tous les frameworks ou PostgreSQL.

## Reconstruire les pages

`build_pages.py` (les 31 pages) puis `build_scaffold.py` (index, ressources, redirection,
sitemap). Chaque page porte en tete un commentaire nommant la famille visee, pour qu'un lecteur
du depot distingue une fixture d'une erreur.
