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
  fabrication. Au plus deux tentatives vocales par 24 heures glissantes et
  2 048 jetons de sortie par appel ; ce n'est pas un plafond en euros. Aucun
  quota SEO client. Aucune tentative fournisseur réelle pendant cette recette.
- La clé OpenAI et le bot privé doivent être déjà configurés. Le mandat lie le
  propriétaire actuel, les accès, le texte, les sources et les captures exacts.
  Une édition, expiration ou révocation empêche l'exécution. Aucun mandat ancien
  élargi ; ni session administrateur de production utilisée ni reconstruite.
- Un appel dont le résultat est inconnu n'est jamais relancé automatiquement.
  Une voix sauvée permet de reprendre le montage sans nouvelle facturation IA.
  Deux essais locaux de montage au plus, bail de travail pour les doublons.
  Un arrêt définitif retire aussi la voix intermédiaire.
- Dix demandes préparées par 24 h et dix rendus conservés avant nouvelle
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

- 131 cas ciblés uniques couverts par les passages finaux (130 groupés puis
  26 cas média après les derniers ajustements, dont un nouveau) : décisions et refus, accès, CSRF,
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
