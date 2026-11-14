# Publication d’un article français sans traduction obligatoire

Le premier article publié par le parcours Noyaru a créé la PR #81, tête
`b533d9b4e0f8ec81ffaa246308d511d967a0ded3`. Sa CI 43604669222 a signalé deux
écarts : le test de traduction imposait 10 versions anglaises pour 10 articles
français, alors que l’anglais est facultatif dans le formulaire ; le titre HTML
comptait 74 caractères avec le suffixe de marque, au-delà de la borne interne 70.
9 839 tests réussis, 2 échecs, 45 ignorés ; les étapes audit et Bandit étaient
ignorées après cet échec. Il ne s’agissait pas d’une validation pour publication.

## Résultat du correctif

Le titre d’onglet conserve le texte entier lorsqu’il tient dans 70 caractères.
Le suffixe de marque n’est ajouté que si l’ensemble tient dans cette borne.
Un titre encore plus long est abrégé entre les mots, avec une ellipse. Le H1,
Open Graph, le texte, le fichier Markdown relu et les empreintes restent intacts.
Cette borne est une convention du produit, pas une limite universelle de Google.

Les neuf traductions déjà publiées restent requises par leur slug. Les nouveaux
articles peuvent être seulement français : catalogue anglais lié à leur vraie
adresse française, redirection de l’adresse anglaise inexistante vers le français,
aucun faux hreflang anglais ni URL anglaise dans le sitemap. Le test de présence
de toutes les traductions existantes est conservé, le simple compte global est
remplacé par ces invariants.

## Validation locale

- 99 tests ciblés réussis : publication, catalogue, traductions, titres et
  contrôles du site public ; huit cas de titre FR/EN, dont le cas exact du blocage.
- 24 tests des deux fichiers de contrôle initialement en échec réussis sur une
  copie combinant le correctif et le fichier exact de #81.
- Cinq fenêtres Chromium locales, 1440/1024/768/390/360 px : pas de débordement
  horizontal, image cassée ou erreur JavaScript ; ancres des références utilisables.
- Comparaison des sept paragraphes et de leurs jeux d’URL de références avec
  l’export relu : texte et sources identiques, indépendamment de la renumérotation.
- Métadonnées, langue française, canonique, réponse HEAD, source SHA-256,
  catalogue et sitemap contrôlés avec les vrais gabarits FastAPI/Jinja.
- Ruff F/E9 et `git diff --check` réussis.

Détails : [browser-results.json](browser-results.json). Ce fichier décrit une
**simulation locale** avec le fichier de l’article ; la PR du correctif n’ajoute
aucun article. Les captures intégrales et vues 1440/390 px ont été examinées et
restent sous `/tmp/noyaru-blog81-combined-proof/`. Aucun compte ni service externe
n’a été utilisé pour cette recette locale.

## Suite de la recette réelle

La CI complète du correctif doit passer avant sa fusion. Le parcours Noyaru doit
ensuite synchroniser la proposition sur ce nouveau main et demander une nouvelle
validation de préparation : ne pas fusionner #81 depuis un autre outil. Le fichier
approuvé ne change pas, mais la base, la tête et les contrôles changent. Attendre
la CI de cette nouvelle tête puis la seconde décision Telegram de publication.
Aucun nouvel appel IA ni seconde rédaction n’est nécessaire.

L’observation de l’article en production vient après cette décision. Une présence
au catalogue et au sitemap ne prouve ni indexation, ni hausse du trafic ou de
citations IA. C002 et C016 restent applicables ; aucun fichier juridique touché.
