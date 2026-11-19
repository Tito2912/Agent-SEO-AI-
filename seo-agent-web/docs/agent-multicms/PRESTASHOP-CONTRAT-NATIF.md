# Adaptateur PrestaShop — contrat natif pages CMS

Lot C082 (19 novembre 2026). Document decrit l'interface reelle de l'adaptateur
`backend/cms_content_adapters/prestashop.py` apres correction du contrat de
revision et de la lecture apres ecriture ; aucune recette distante n'est
affirmee par ce document. Les ecarts cites ci-dessous ont ete mesures par Codex
sur une boutique PrestaShop 8.2.8 fr/en rejouee en local (C082, 19/11/2026).

## Sources officielles verifiees

- Schema Webservice de la ressource `content_management_system` (PrestaShop 8) :
  https://devdocs.prestashop-project.org/8/webservice/resources/content_management_system/
- Code de reference `classes/CMS.php` de PrestaShop 8.2.8 :
  https://raw.githubusercontent.com/PrestaShop/PrestaShop/8.2.8/classes/CMS.php

Ces deux sources enumerent les champs de la ressource CMS. **Aucune colonne
`date_add` ni `date_upd` n'est declaree sur cette ressource** ni retournee par
le webservice. L'ancienne implementation de l'adaptateur en attendait pourtant
une valeur : la lecture d'une page CMS reelle aurait donc leve
`AdapterError(code="unreadable")` a chaque fois. C'est le defaut corrige par ce
lot.

## Reponse d'ecriture non autoritative

Ecart mesure sur 8.2.8 fr/en (19/11/2026) :
- Apres **POST 201**, le corps `<content>` renvoye par PrestaShop echoe
  `indexation` vide et les variantes langue 2 de `meta_title` / `link_rewrite`
  vides. Le **GET immediat** sur le meme id retourne `indexation=0` et le repli
  langue par defaut dans ces deux champs. L'empreinte POST != GET sans la
  moindre edition entre les deux.
- Un **PATCH** peut echoer un ORM partiel (champs texte vides ou absents) qui
  laisserait croire a une mutation, alors que le GET autoritatif montre la
  ressource preservee. Inversement, le PATCH peut echoer les valeurs envoyees
  tandis que le GET revele une reecriture reelle (conversion de `\n` litteraux
  constatee par l'ancien labo 8.2.8).

**Decision :** la reponse d'ecriture n'est jamais utilisee pour decider.
L'adaptateur relit TOUJOURS la ressource via `GET /content_management_system/{id}`
apres chaque ecriture reussie. C'est cette relecture qui :

- fournit la revision (empreinte SHA-256) renvoyee a l'appelant ;
- sert de base de comparaison pour verifier les preservations exigees.

Si la relecture echoue (reseau, 5xx, 401/403, corps illisible, etc.),
l'adaptateur leve `AdapterError(uncertain=True)` en conservant le code d'origine
et en inscrivant l'identifiant connu dans `details.remote_id`. L'ecriture a pu
aboutir cote CMS ; l'appelant doit verifier avant tout nouvel envoi. **Aucun
reessai d'ecriture n'est jamais tente.**

## Interface

Inchangee : `create_draft`, `read`, `publish`, `update_draft`, telle que decrite
dans `CONTRAT-ADAPTATEURS.md`. Seuls le mode de calcul de la revision et la
construction des PATCH changent.

## Revision : empreinte deterministe

La revision renvoyee par l'adaptateur est le SHA-256 (hex) d'une serialisation
canonique des champs CMS reels. Elle est :

- **deterministe** : meme document → meme empreinte, quel que soit l'ordre des
  langues ou des attributs XML dans la reponse. Les langues sont triees par
  identifiant numerique ; l'ordre d'apparition dans la reponse n'est pas pris
  en compte ;
- **complete** : `id_cms_category`, `position`, `active`, `indexation`,
  `meta_title`, `head_seo_title`, `meta_description`, `meta_keywords`,
  `link_rewrite`, `content`. Un champ absent et un champ vide produisent deux
  empreintes distinctes ;
- **fidele au texte** : les espaces et sauts de ligne a l'interieur du contenu
  HTML d'une page CMS sont preserves byte-a-byte dans la serialisation ;
- **sans timestamp invente** : aucune valeur hors reponse n'est utilisee.

La revision est fournie a `expected_revision` pour `update_draft` et `publish`.
L'adaptateur relit le document distant juste avant d'ecrire, recalcule
l'empreinte et refuse l'ecriture si elle ne correspond pas (`code="conflict"`).

## Comportement d'ecriture

### `create_draft`
1. POST `/api/content_management_system` avec `active=0`.
2. Validation de l'identifiant renvoye par le POST ; absence ou forme invalide
   → `unreadable, uncertain=True` (l'ecriture a pu aboutir).
3. **GET autoritatif** sur cet identifiant. La revision renvoyee et l'etat sont
   ceux du GET, pas du POST.

Envoi UNIQUEMENT de la langue configuree (`id_lang`). La creation d'une page
CMS dont la langue configuree n'est PAS la langue par defaut de la boutique
n'est **pas validee** dans ce lot : PrestaShop exige les champs multilingues
obligatoires (`meta_title`, `link_rewrite`) pour la langue par defaut, et cet
adaptateur **ne recopie jamais** la langue configuree dans les autres langues.
Le GET autoritatif peut neanmoins montrer un repli langue par defaut applique
par PrestaShop lui-meme ; c'est son comportement, pas une recopie inventee par
l'adaptateur. Cette contrainte doit etre verifiee en recette reelle avant
publication a un client dont la langue de travail est non-defaut.

### `update_draft`
1. GET pre-ecriture pour relire la ressource complete.
2. Verification de la revision (empreinte du GET pre).
3. PATCH qui re-envoie **toutes** les langues connues pour les champs modifies
   (`meta_title`, `meta_description`, `link_rewrite`, `content`). La langue
   configuree porte la valeur du brouillon ; les autres langues sont re-envoyees
   avec leur valeur distante exacte. Ceci evite l'effacement des langues non
   selectionnees constate dans l'ancien labo 8.2.8 (PATCH mono-langue qui
   effacait les autres variantes et repondait 500 apres ecriture).
4. Les champs multilingues non touches (`head_seo_title`, `meta_keywords`)
   sont re-envoyes tels quels. Le champ `active` est preserve :
   `update_draft` ne publie jamais.
5. Validation de l'identifiant echoe par le PATCH (doit correspondre a celui
   envoye). En cas de mismatch → `unreadable, uncertain=True,
   details={remote_id, returned_id}`.
6. **GET post-ecriture autoritatif.** Si cette relecture echoue, l'erreur
   devient `uncertain=True` avec `details.remote_id`.
7. Sur le GET autoritatif : `active` doit etre egal a l'etat pre ; toutes les
   langues NON ciblees doivent etre byte-a-byte identiques. Un ecart entraine
   `AdapterError(code="unreadable", uncertain=True, details={remote_id, ...})`.
   La reponse PATCH, meme partielle ou trompeuse, ne declenche pas de fausse
   mutation : seule la relecture decide.

### `publish`
1. GET pre-ecriture pour relire la ressource complete.
2. Verification de la revision.
3. PATCH qui n'envoie **que** `<id>` et `<active>1</active>` (aucun champ texte).
4. Validation de l'identifiant echoe par le PATCH.
5. **GET post-ecriture autoritatif.** Idem `update_draft` pour les erreurs de
   relecture.
6. Sur le GET autoritatif : `active` doit valoir `1`, et tous les champs
   multilingues (toutes langues) doivent etre byte-a-byte identiques a l'etat
   pre. Ceci couvre la regression observee par l'ancien labo 8.2.8 (un PATCH
   sur une page CMS pouvant reencoder le contenu : conversion de `\n` litteraux,
   puis reponse 500). Toute modification detectee leve
   `AdapterError(code="unreadable", uncertain=True, details={remote_id, ...})` :
   l'activation n'est pas consideree comme reussie si le texte a change
   cote GET, meme si la reponse PATCH suggerait le contraire.

## Codes d'erreur stables

- `mapping` : configuration invalide (`id_lang` ou `id_shop` hors forme).
- `validation` : brouillon incomplet, HTML trop volumineux, document XML trop
  volumineux.
- `revision_required` / `conflict` : ecriture refusee avant tout envoi reseau.
- `auth`, `not_found`, `limit`, `refused` : codes HTTP applicatifs explicites.
- `server`, `unreadable` : reponses serveur (5xx, corps illisible, DOCTYPE
  interdit, texte modifie). `uncertain=True` est pose quand l'ecriture a pu
  aboutir cote CMS avant la detection : l'appelant doit relire la ressource.

## Securite XML

- Documents acceptes bornes a `MAX_XML = 2 Mio`.
- Toute valeur est echappee via `xml.sax.saxutils.escape` ; aucune
  interpolation brute.
- Toute reponse contenant `<!DOCTYPE` ou `<!ENTITY` dans les 4 premiers Kio
  est refusee avant parsing (code `unreadable`).
- Parsing via `defusedxml.ElementTree` ; les entites externes et expansions
  pathologiques sont bloquees par la bibliotheque.

## Limites declarees

1. Pages CMS uniquement, PAS de blog ; `capabilities.upload_images=False`.
2. Creation hors langue par defaut de la boutique non validee ; aucune recopie
   inventee entre langues.
3. La relecture GET apres ecriture peut, sur certaines boutiques, etre plus
   lente que l'ecriture elle-meme ; l'adaptateur ne retente rien et laisse
   l'appelant decider.
4. Les tests livres ne ciblent pas de boutique distante : ils valident la
   forme des echanges XML, l'empreinte, la relecture autoritative et la
   propagation d'incertitude. La recette reelle sur PrestaShop 8.2.8 fr/en est
   assuree par Codex dans un clone Linux isole (C082), avec cle Webservice
   jetable et boutique locale. Les ecarts mesures dans cette recette alimentent
   directement les cas tests ci-dessus.
