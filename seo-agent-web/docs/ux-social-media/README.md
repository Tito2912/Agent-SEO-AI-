# Première fabrication vidéo Noyaru — 14 novembre 2026

Base : `4caec100` (studio #84). Ce lot traite une première démonstration du score,
en français et en anglais, sur un projet fictif. Aucun site client ni SAV utilisé.

## Audit et choix

| Écran | Problème constaté dans le code | Conséquence | Livraison | Priorité |
| --- | --- | --- | --- | --- |
| Studio TikTok | Le dossier s'arrête au texte | Le propriétaire doit fabriquer son média ailleurs | Lien contextualisé vers le storyboard du score | P1 |
| Fabrication | Pas de rendu ni de suivi durable | Impossible de relire le fichier exact | Captures locales réelles, voix synthétique, MP4 privé et progression | P1 |
| Telegram | Aucun mandat média | Effet et coût difficiles à distinguer | Deux décisions : fabrication payante, puis relecture du fichier exact | P1 |
| Interruption | Un appel vocal répété pourrait être facturé deux fois | Coût et doublons | Réservation durable avant appel, voix sauvée avant montage, résultat inconnu arrêté | P1 |
| Retour et mobile | Nouveau parcours à rendre explicite | Perte d'objectif/public ou d'étape | Liens contextualisés, lecteur natif, états FR/EN, contrôles sans JavaScript | P1 |

## Parcours livré

Studio → TikTok → comprendre le score → découvrir la fabrication → storyboard
et texte exact → demande gratuite → décision Telegram 1/2 → un appel OpenAI
et montage local → lecteur privé → décision Telegram 2/2 → fichier marqué relu.
Chaque décision garde la confirmation du choix. Un refus retire le rendu ; la
seconde décision **ne publie rien**. La carte renvoie vers le lecteur privé
Noyaru : la vidéo n'est pas téléversée dans Telegram.

Le MP4 720 × 1280, H.264/AAC, 24 images/s, assemble trois captures commentées.
Ce n'est pas une séquence de clics filmée en continu. Les textes sont incrustés
par scène ; le découpage est proportionnel aux mots, pas un alignement vocal
mot à mot. La relecture doit vérifier la synchronisation et l'écoute entière.
La mention de voix générée par IA et de projet fictif reste dans le média et
son texte d'accompagnement. Aucun visage, imitation d'une personne ou musique.

## Limites et reprise

- Seul le sujet « comprendre le score » produit un média dans ce lot ; les cinq
  pistes natives du studio gardent leurs dossiers textuels. Pas d'adaptation
  automatique d'un article, de calendrier ni de publication Instagram/TikTok.
- Fournisseur fixe : OpenAI `gpt-audio-1.5`, voix `coral`, WAV, un appel par
  fabrication. Plafond propriétaire réglable de 1 à 100 tentatives par 24 heures
  glissantes (10 par défaut depuis le 15/11 ; les anciens mandats gardent 2) et
  4 096 jetons de sortie par nouvel appel (profil v2, voir le correctif ci-dessous) ; ce n'est pas un plafond en euros. Aucun
  quota SEO client. Aucune tentative fournisseur réelle pendant cette recette.
- La clé OpenAI et le bot privé doivent être déjà configurés. Le mandat lie le
  propriétaire actuel, les accès, le texte, les sources et les captures exacts.
  Une édition, expiration ou révocation empêche l'exécution. Aucun mandat ancien
  élargi ; ni session administrateur de production utilisée ni reconstruite.
- Un appel dont le résultat est inconnu n'est jamais relancé automatiquement.
  Une voix sauvée permet de reprendre le montage sans nouvelle facturation IA.
  Deux essais locaux de montage au plus, bail de travail pour les doublons.
  Une fabrication active au plus par propriétaire ; la suivante reste en file
  pour préserver les ressources du service web.
  Un arrêt définitif retire aussi la voix intermédiaire.
- Demandes gratuites préparées limitées à max(10, 2 × plafond vocal) par 24 h,
  avec erreur distincte ; dix rendus conservés avant nouvelle
  fabrication. Le contrôle du stockage est revérifié avant l'appel vocal.
  Le propriétaire retire un ancien rendu via une confirmation sur la page.
- Chaque vidéo fait moins de 12 Mo ; données binaires privées dans la base
  (base64), suppression du fichier conservé à l'arrêt/refus, cascade avec le
  propriétaire. La voix intermédiaire est conservée pour une reprise du montage
  puis remplacée par le MP4. Pas de nouvelle politique de purge promise.
- Captures vérifiées par empreintes de leurs images et quatre fichiers UI de
  référence. Après modification de ces fichiers, régénérer et relire les captures
  avec `capture_demo.py`, avant de livrer un nouveau dossier. Le widget d'aide est
  masqué pendant la capture pour ne pas couvrir les informations ; contenu réel
  de la route locale, projet `atelier.example`, aucune requête externe.
- FFmpeg et DejaVu ajoutés à l'image de production (Arial disponible comme
  repli sur Windows). Sur un poste sans ces outils,
  la page explique l'indisponibilité ; aucune installation déclenchée par HTTP.
- Lecture historique du fichier permise au propriétaire même si la source change,
  avec avertissement ; un nouvel accord exige toujours le contexte actuel.

Documentation fournisseur vérifiée le 14/11 :
[sortie audio Chat Completions](https://developers.openai.com/api/docs/guides/audio-chat-completions),
[modèle](https://developers.openai.com/api/docs/models/gpt-audio-1.5),
[dépréciations](https://developers.openai.com/api/docs/deprecations).
L'ancien endpoint TTS n'a pas été retenu pour ce nouveau flux.

## Vérifications

- 132 cas ciblés uniques couverts par les passages finaux (130 groupés puis
  27 cas média après les derniers ajustements, dont deux nouveaux) : décisions et refus, accès, CSRF,
  rejeu, expiration, changements d'accès/captures/texte, concurrence, reprise,
  résultat incertain, plafonds ; anciennes cartes Telegram/studio et inventaire UX.
- Migration additive 0031 depuis 0030 : upgrade répété, base créée par migrations
  ou `create_all`, rollback et nouvel upgrade ; propriétaire et DPA conservés,
  table vide d'autorité et contrainte de cascade vérifiées.
- `render_smoke.py` encode réellement les deux MP4 puis les décode entièrement.
  Son de silence synthétique de 30 s : contrôle du conteneur et des images, **pas
  une preuve de qualité du service vocal**. Résultats dans `render-results.json`.
- `browser_smoke.py` utilise Chromium, de vrais écrans/MP4 et des comptes fictifs.
  82 observations FR/EN à 1 440/768/390/360 px, dont deux sans JavaScript ;
  aucune image cassée, erreur JavaScript ou requête externe, pas de débordement.
  Les états injectés vérifient l'interface ; les tests transactionnels vérifient
  séparément les décisions. Télégram simulé et aucun appel fournisseur.
- Compilation, Ruff F/E9 et Bandit medium/high réussis ; CI complète référencée
  dans la PR de livraison. Aucune recette privée de production.

Reproduction : venv des dépendances de développement, FFmpeg dans `PATH`, Chromium
Playwright installé ; `CHROMIUM_EXECUTABLE`/`CHROMIUM_LIBRARY_PATH` facultatifs.
Exécuter dans l'ordre `capture_demo.py`, `render_smoke.py`, `browser_smoke.py`.
Les MP4 et captures de recette navigateur restent dans `/tmp`, pas dans Git.
Les six captures produit fictives et leur manifeste sont versionnés séparément.

## Correction WAV du 15 novembre 2026

Constat opérateur : fabrication arrêtée, « réponse vocale inexploitable ».
La capture ne donne pas les octets reçus : la cause exacte de cet ancien essai
ne peut pas être établie à partir de ce seul écran. Aucun nouvel appel payant
ou accès privé utilisé pour le diagnostic.

Défaut reproduit localement : un WAV de 30 s avec tailles RIFF/data provisoires
`0xffffffff` est lu comme 89 478 s par le calcul précédent, qui le rejetait.
Le lecteur finalise désormais ces tailles (ou `0x7fffffff`) avec les octets PCM
effectivement reçus, puis conserve les mêmes échantillons dans un WAV standard.
Une taille finie incohérente, un fichier tronqué, un format incompatible ou une
durée réelle hors 8–60 s restent refusés. Le montage utilise le WAV normalisé.

Les erreurs distinguent maintenant absence de voix, encodage, format, fichier
incomplet et durée. La transcription exacte, les mandats Telegram, le plafond
de deux tentatives et l’arrêt sans rappel fournisseur sont préservés. Aucun
redémarrage des anciennes fabrications arrêtées, aucune remise à zéro de quota
ou hypothèse de remboursement. Un prochain essai passe par un nouveau mandat
et reste soumis à la fenêtre de 24 h. Les anciens fichiers vocaux rejetés n’ont
pas été conservés et ne peuvent pas être récupérés par cette correction.

Les fixtures PCM et réponses HTTP simulées couvrent les tailles provisoires,
les échantillons inchangés, les fichiers réellement tronqués, les diagnostics
et l’absence de second appel après échec. La documentation officielle OpenAI
sur le format WAV dans Chat Completions a été relue le 15/11 ; elle ne spécifie
pas les tailles provisoires de l’en-tête. Cette tolérance est donc testée comme
une compatibilité de conteneur, sans affirmer qu’elle était la cause en production.

Validation de ce correctif : 72 cas ciblés uniques (71 groupés puis les 31 cas
WAV, dont un nouveau PCM extensible), compilation/Ruff/Bandit medium/high verts.
40 observations Chromium FR/EN à 1 440/768/390/360 px sur les cinq diagnostics,
sans débordement ni erreur JavaScript/requête externe ; captures relues.
Deux MP4 de 30 s réellement encodés et entièrement décodés depuis les en-têtes
provisoires (FR 450 165 octets, EN 408 335). Son synthétique, pas une écoute
de la voix fournisseur. Preuves temporaires : `/tmp/noyaru-audio-wav-proof/`
et `/tmp/noyaru-audio-browser-proof/`. CI complète à référencer dans la PR.
Tests reproductibles : `python -m pytest -q tests/test_own_social_audio.py
tests/test_own_social_media.py` depuis `seo-agent-web/`.

## Disponibilité avant Telegram — 15 novembre 2026

Constat opérateur : une nouvelle carte était approuvée alors que les deux appels
du pilote avaient déjà commencé. Le worker refusait correctement le troisième
appel, mais le parcours demandait une décision inutile et proposait de recommencer
sans indiquer quand ce serait possible.

La page montre désormais les appels disponibles et, au plafond, la première
heure de réouverture. L’heure est rendue en UTC sans JavaScript, puis dans le
fuseau du navigateur avec son nom de zone. Le contrôle vient de la base à chaque
affichage : la date ne réserve aucun appel. Le lien de disponibilité conduit à
une nouvelle lecture, sans création ni relance. Le formulaire et l’invitation à
préparer un nouvel essai restent masqués au plafond ; le POST direct est aussi
refusé avant toute création de demande/carte Telegram.
Une proposition déjà préparée mais pas encore notifiée attend aussi une place
avant la création d’une nouvelle carte de fabrication ; les cartes de relecture
restent disponibles.

La fenêtre de 24 h exclut les appels ayant exactement 24 h ou plus. Les appels
commencés restent comptés quels que soient le résultat ou le retrait du rendu ;
une préparation refusée sans appel ne repousse pas l’échéance. Si l’historique
contient plus de deux appels, la disponibilité attend l’expiration d’un nombre
suffisant. Le contrôle transactionnel final avant appel reste obligatoire :
une disponibilité affichée auparavant ne l’autorise pas. Le montage d’une voix
sauvée et la relecture du fichier exact restent possibles sans nouvel appel.

Aucune augmentation, remise à zéro, suppression d’historique ou reprise des
anciens essais. Aucune nouvelle requête fournisseur/Telegram réelle ou session
de production utilisée. Les garde-fous et mandats des lots précédents demeurent.
Tests locaux : 80 cas média/audio réussis, dont huit nouveaux cas de limite,
date/frontière, isolation propriétaire, absence de carte à la préparation refusée,
idempotence et reprise locale. Compilation, Ruff et Bandit medium/high verts.
26 observations Chromium locales du compteur, du refus et de la réouverture en
FR/EN, quatre largeurs et repli sans JavaScript. Heure locale Paris vérifiée en
FR 24 h et EN AM/PM ; repli UTC explicite. Aucune erreur JavaScript, requête
externe ou débordement global ; captures relues. Preuves temporaires sous
`/tmp/noyaru-quota-browser-proof/` ; référence de CI dans la PR.

## Réglage propriétaire du plafond — 15 novembre 2026

Tony juge le plafond fixe de deux appels trop contraignant et autorise son
augmentation. Le défaut devient **10 appels commencés / 24 h glissantes**,
réglable de 1 à 100 dans « Régler mon plafond vocal » sur la page vidéo. Ce
réglage Noyaru ne correspond ni au quota OpenAI ni au forfait SEO d’un client.
La limite de 2 048 jetons par appel demeure ; aucun budget en euros n’est promis.

- Paramètre durable par propriétaire, formulaire POST propriétaire + CSRF,
  contrôle de version pour les onglets concurrents et journal avant/après.
  Même verrou transactionnel que la réservation d’un appel ; lecture pure en GET.
- Une hausse garde les dates et états de toutes les tentatives, y compris les
  échecs, annulations et résultats inconnus ; elle ne lance aucun travail.
  Un nouveau dossier prend une copie du plafond dans son empreinte approuvée.
  L’ancien mandat reste plafonné à son chiffre initial (2 pour un dossier
  antérieur à ce lot), même après une hausse. Un réglage abaissé est toujours
  revérifié avant une nouvelle réservation ; le plus petit des deux plafonds
  s’applique. Voix déjà reçue et relecture n’exigent aucun nouvel appel vocal.
- Chaque nouvel appel payant reste lié à sa décision Telegram exacte. Aucun
  ancien essai arrêté ou incertain n’est relancé. Nouvelle fabrication et nouvel
  accord nécessaires ; la relecture du MP4 reste une décision distincte.
- Les préparations gratuites disposent d’un plafond distinct, affiché avec les
  limites de stockage : max(10, deux fois le plafond vocal) par 24 h. Cela évite
  que les préparations gratuites consomment le compteur vocal ou que leur ancien
  plafond fixe de dix empêche d’utiliser un plafond vocal plus élevé.
- Migration additive `20261115_0032` après 0031 : table de réglage seule, vide
  initialement ; aucune réécriture d’un média ou d’un mandat existant. Sans
  réglage enregistré, le défaut 10 s’applique aux prochaines demandes. Rollback
  de la table et reprise des migrations contrôlés localement.

Recette de ce lot : comptes fictifs, bot et voix simulés ; aucun appel vocal
payant, donnée SAV, site client ou accès privé de production. Vérifications et
PR consignées au journal partagé lors de la livraison.

Validation locale finale : 154 cas ciblés uniques (média, politique, audio,
préservation des migrations, décisions Telegram et inventaire) ; 26 observations
Chromium FR/EN à 1 440/768/390/360 px, dont les deux enregistrements du plafond
sans JavaScript. Hausse réelle du formulaire 2 → 10 : deux tentatives conservées,
8 appels disponibles ; baisse à 1 : nouvelle préparation bloquée. Aucun message
Telegram réel, appel vocal, débordement global ou erreur JavaScript ; captures
relues, champ de 48 px et libellé associé. Contrôles compilation, Ruff F/E9 et
Bandit medium/high réussis. Preuves temporaires `/tmp/noyaru-limit-browser-proof`.

Un enregistrement explicite de la valeur par défaut crée aussi le réglage durable :
un futur changement de défaut applicatif ne remplace pas le choix enregistré.

## Écarts de transcription et réponse coupée — 15 novembre 2026

Base : `213d5d73` après #88. Constat opérateur : « La transcription fournie ne
correspond pas au texte approuvé ». Le code utilisait ce diagnostic lorsque
`finish_reason` n'était pas `stop` **ou** que le texte normalisé différait. Le
fichier et la transcription étaient alors jetés. La cause de l'ancien essai
reste indéterminée ; ses données ne sont pas récupérables par ce correctif.

| Écran / contrôle | Défaut vérifié dans le code | Conséquence | Correction | Priorité |
| --- | --- | --- | --- | --- |
| Fabrication arrêtée | Troncature et différence partagent la même erreur | Relance payante sans savoir ce qui s'est passé | Diagnostics distincts pour limite de sortie, fin anormale et écart | P1 |
| Préécoute | Un WAV complet et valide est perdu dès qu'un mot diffère | Le propriétaire ne peut pas écouter ou refuser la voix reçue | Montage privé conservé avec les deux textes et leurs différences | P1 |
| Relecture Telegram 2/2 | La carte ne prévoit que la narration attendue | Risque d'accepter une version différente sans le comprendre | Avertissement, script initial, transcription reçue, résumé explicite de l'écart, empreinte du fichier | P1 |

La comparaison reste stricte après normalisation de casse, ponctuation et
espaces : aucun seuil de similarité ni jugement IA n'accepte une reformulation.
Une réponse terminée par `stop`, un WAV valide de 8–60 secondes et une
transcription textuelle bornée sont requis pour conserver l'aperçu. Le rendu
passe en **relecture**, jamais directement en prêt. La page affiche les retraits
barrés et les ajouts soulignés, avec une légende textuelle, les deux versions
complètes et le lecteur natif. Le texte du fournisseur est échappé par Jinja.

Le propriétaire doit écouter l'ensemble, vérifier les affirmations, les textes
incrustés et leur synchronisation, puis accepter explicitement cette version
dans la décision Telegram 2/2 existante, ou la refuser. Aucun troisième accord
ni nouvel appel IA n'est ajouté pour cette comparaison. La transcription vient
du fournisseur ; elle n'est pas une reconnaissance vocale indépendante et ne
prouve pas à elle seule les mots prononcés. Le découpage des scènes demeure
fondé sur le script initial : une différence peut aussi rendre le montage
inadapté et justifier son refus.

La décision lie également la transcription reçue, en plus du MP4 exact. Tout
changement invalide une carte antérieure. Le refus ou l'arrêt retire le rendu ;
l'acceptation garde les différences comme trace de la relecture et marque ce
fichier prêt uniquement, sans publication sociale. Une panne locale de montage
garde le WAV et la comparaison pour la reprise autorisée sans nouvel appel vocal.
Une sortie coupée (`length`) ou une autre fin anormale reste arrêtée, sans
rappel automatique. Historique, plafonds, droits et confirmations sont conservés.

Les **nouvelles** demandes utilisent le profil v2, plafonné à 4 096 jetons de
sortie au lieu de 2 048. Le texte de la page et la carte de fabrication indiquent
ce plafond ; ce n'est pas un budget en euros ni une garantie de résultat. Le
changement de profil invalide les anciens mandats avant tout appel, sans les
élargir, relancer ou remettre leurs compteurs à zéro. Les anciens fichiers
déjà conservés restent consultables historiquement par leur propriétaire, avec
l'avertissement de contexte habituel ; une nouvelle action exige le contexte
actuel. Aucun changement de modèle, migration ou dépendance dans ce lot.

Documentation officielle OpenAI relue le 15/11 :
[audio Chat Completions](https://developers.openai.com/api/docs/guides/audio-chat-completions)
pour les champs audio/transcription ;
[référence Chat Completions](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)
pour la fin de génération ;
[dépréciations](https://developers.openai.com/api/docs/deprecations)
pour le maintien de `gpt-audio-1.5` dans ce lot. La documentation ne garantit pas
une lecture verbatim de toute sortie audio. Aucun essai payant n'a été lancé
pour attribuer la cause de l'ancienne réponse ou mesurer la voix réelle.

Vérification locale : tests de réponses HTTP simulées, mots modifiant une
affirmation, refus/acceptation explicite, carte périmée, comparaison altérée,
WAV invalide, texte non exploitable, reprise locale et ancien mandat v1. Les
comptes, clés et messages sont fictifs. La recette navigateur et la CI sont
consignées dans la PR de livraison ; elles ne valent pas écoute du fournisseur.
Reproduction : `python -m pytest -q tests/test_own_social_narration.py
tests/test_own_social_media.py tests/test_own_social_audio.py
tests/test_own_social_media_policy.py tests/test_telegram_approvals.py
tests/test_current_ux_inventory.py` depuis `seo-agent-web/`.

## Fabrication visible et suivi explicite — 16 novembre 2026

Base `f24bfe10`, après #89. Constat opérateur par capture : « Fabrication en
cours » ressemble à un texte ordinaire, sous les réglages. L'écran conserve
« Valider dans Telegram » pendant le travail. Le code du suivi confirme aussi
un arrêt silencieux après trois erreurs réseau. Ces constats ne prouvent pas
que le worker réel avance ou qu'il est bloqué ; aucune session de production
n'est utilisée pour les compléter.

| Écran / état | Problème constaté | Conséquence | Correction | Priorité |
| --- | --- | --- | --- | --- |
| Fabrication autorisée / en cours | Statut discret sous les réglages | Impression que rien ne se passe | Panneau avant les quotas, titre de l'étape et animation discrète | P1 |
| Création de la voix / montage | Même libellé pour plusieurs étapes | Impossible de comprendre l'attente | Quatre repères : accord, voix off, montage, relecture | P1 |
| Fabrication en cours | Invitation Telegram permanente | Validation supplémentaire supposée nécessaire | Action Telegram réservée à l'accord et à la relecture | P1 |
| Erreurs du suivi / perte d'accès | Arrêt silencieux du polling | Un écran ancien semble encore à jour | Avertissement visible, animation arrêtée, lien d'actualisation sans relance | P1 |
| Traitement encore marqué en cours mais bail expiré | Texte trop affirmatif | Confusion entre état enregistré et activité confirmée | « L'activité reste à vérifier », aucun nouveau travail déclenché | P1 |

Le parcours reste : accord Telegram → file d'attente → appel vocal → montage
→ lecteur privé → relecture Telegram. La page permet de rejoindre directement
le lecteur et de retrouver le panneau d'avancement. Elle n'ajoute ni décision,
appel IA, crédit consommé, changement de plafond ou publication.

Les étapes viennent des marqueurs durables existants : appel commencé,
tentative de montage enregistrée après sauvegarde de la voix, état de
relecture. Le bail du travail et l'expiration de la demande sont contrôlés en
lecture seule. Un bail expiré donne un état « à vérifier », sans modifier la
ligne, renouveler le bail ou relancer le worker. Une reprise locale de montage
est distinguée d'un nouvel appel vocal. Aucun pourcentage ou délai de fin
estimé n'est inventé ; la lecture du statut n'est pas un heartbeat du worker.

Le GET privé de statut ne sélectionne que les métadonnées nécessaires, jamais
le WAV, MP4, la clé ou le texte. Le suivi actualise la page à chaque changement
de phase. Trois erreurs consécutives, une réponse illisible ou une perte d'accès
deviennent visibles au lieu de laisser une animation rassurante. Les requêtes
ont un délai d'attente de dix secondes et le polling n'est pas cumulé ; dans un
onglet masqué, il attend. La date de dernière lecture réussie est distincte de
l'activité du worker. Le lien d'actualisation ne relance aucun travail.

Les mouvements réduits désactivent l'animation. Les étapes et l'état restent
lisibles sans JavaScript, avec une invitation à actualiser manuellement. Le
profil vocal v2, les quotas, les confirmations et la comparaison de narration
de #89 sont conservés. Aucun changement de schéma, migration, fournisseur,
capture produit, règle SAV/CMS ou fichier juridique. Les tests utilisent des
comptes et états fictifs ; la recette visuelle ne vaut pas diagnostic du
traitement réel. Les validations locales et la CI sont consignées dans la PR.
