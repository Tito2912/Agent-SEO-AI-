# Pages juridiques — validations nécessaires avant publication

Le 21 octobre 2026, le propriétaire a validé la publication des six pages (CGU, confidentialité,
mentions légales, en français et en anglais ; demande #39). Les points ci-dessous restent ouverts
après cette validation : chacun est à régler dès que l'information est disponible.

Les valeurs de l'éditeur sont dans `seo-agent-web/backend/legal.py` ; une valeur inconnue y vaut
`None`. Le détail des changements est dans `LEGAL-CHANGES.md`.

## 1. Informations manquantes

| Information | Document | Pourquoi |
| --- | --- | --- |
| Durée de conservation des journaux techniques chez Render, selon votre offre | Confidentialité § 7 (FR/EN) | La page dit « la durée appliquée par notre hébergeur ». L'audit demande une durée exacte : indiquez-la, sans l'estimer. |
| Durée de conservation des traces d'erreur chez Sentry | Confidentialité § 4 et § 7 | Aucune durée n'est indiquée. À fournir si vous voulez la mentionner. |

## 2. Faits à confirmer

| Point | Document | Ce qu'il faut confirmer |
| --- | --- | --- |
| Adhésion au médiateur CM2C | CGU article 12 (FR/EN) | Les CGU nomment CM2C, comme la version en ligne. Confirmez qu'E-Com Shop y est bien adhérente et que l'adresse (49 rue de Ponthieu, 75008 Paris) est à jour. |
| Mécanisme de transfert de chaque prestataire hors UE | Confidentialité § 5, DPA annexe IV | La page ne dit plus quel mécanisme couvre quel prestataire. Pour Render, Stripe, Anthropic, OpenAI, Google (dont Analytics), GitHub, Microsoft, SerpAPI, Netlify, Amazon Web Services, Sentry, Wix, Webflow, Cloudflare, Telegram (et Ahrefs s'il est activé) : certification au cadre UE–États-Unis, ou clauses contractuelles types signées. La page promet d'en fournir la description sur demande. |
| Conditions d'entraînement des modèles d'IA | Confidentialité § 6 | Vérifier les conditions API actuelles d'Anthropic et d'OpenAI, et l'offre Gemini utilisée (payante ou gratuite) : l'exclusion de l'entraînement en dépend. |
| Prix « toutes taxes comprises » pour les consommateurs | CGU article 3 | La phrase est conservée de la version en ligne. Confirmer que le paramétrage Stripe affiche bien des prix TTC aux consommateurs. |
| Coordonnées de l'hébergeur Render | Mentions légales FR/EN | Reprises de la version en ligne (525 Brannan Street, Suite 300, San Francisco). À vérifier avant publication. |
| Téléphone publié (06 14 97 64 01) | Mentions légales FR/EN | Repris de la version en ligne. Confirmer qu'il doit rester affiché. |

## 3. Engagements qui dépendent de vous

| Engagement | Document | Dépendance |
| --- | --- | --- |
| Informer les clients par e-mail au moins 30 jours avant une modification substantielle des CGU ou du service | CGU article 13 (FR/EN) | Écran « Annonces aux clients » (`/settings/legal-notices`, réservé au propriétaire) : il envoie l'e-mail à tous les comptes, refuse une date à moins de 30 jours et garde la preuve de chaque envoi. Il reste à l'utiliser à chaque modification substantielle. Un consommateur gêné par une modification du service peut aussi mettre fin au contrat dans les 30 jours, avec remboursement de la période restant à courir (remboursement manuel dans Stripe). |
| Rembourser une rétractation sous 14 jours | CGU article 5 (FR/EN) | Le remboursement est une opération manuelle dans Stripe (`RUNBOOK.md`, « Rétractation »). La case « Je demande que mon abonnement commence immédiatement » existe bien avant le paiement. |
| Fournir un accord de traitement des données (DPA) sur demande | Confidentialité § 1, CGU article 11 | Les pages annoncent qu'il « peut être demandé ». Le DPA (`DPA-fr.md`, `DPA-en.md`) est un brouillon non opérationnel, reconstruit le 06/11/2026 : il ne peut être proposé aux clients qu'une fois réglés les points de la section 5. Mode d'acceptation choisi le 21/10/2026 : une case à cocher dans le service. |
| Décrire les garanties de transfert sur demande | Confidentialité § 5 | Suppose d'avoir les justificatifs du point 2. |

## 4. Décisions juridiques à faire valider

| Clause | Document | Question |
| --- | --- | --- |
| Clause de langue (français version de référence) | CGU article 14 | Proposition contractuelle de l'audit : son opposabilité n'est pas garantie dans tous les pays. |
| Réserve « faute lourde ou dol » du plafond de responsabilité des professionnels | CGU article 10 | Rédaction à valider. Plafond porté au plus élevé des sommes payées sur 12 mois ou de 1 000 € (choix du 21/10/2026), pour qu'il ne soit pas dérisoire, notamment pour un client du plan gratuit. |
| Compétence des tribunaux de Paris pour les professionnels | CGU article 12 | Conservée, et désormais limitée aux cas où elle est valable et opposable. |
| Rôle de sous-traitant d'E-Com Shop et services du client (GitHub, WordPress, Search Console…) | Confidentialité § 1, DPA annexe IV | Le DPA ne les exclut plus d'office des sous-traitants ultérieurs : leur qualification dépend des contrats et des opérations de traitement, à confirmer service par service (section 5). |
| Cookie de langue `noyaru_lang` (1 an) | Confidentialité § 8 | Présenté comme un cookie de préférence choisie, sans consentement. À confirmer. |
| DPA dans son ensemble | `DPA-fr.md`, `DPA-en.md` | Brouillon reconstruit le 06/11/2026 sur les clauses de la Commission. Points soumis : `QUESTIONS-AVOCAT.md`. |

## 5. Contrat de traitement des données (DPA)

Reconstruit le 6 novembre 2026 en français et en anglais (détail dans `LEGAL-CHANGES.md`). Il reste un
brouillon : il ne peut être proposé aux clients qu'une fois réglés les points ci-dessous, et après la
décision d'E-Com Shop de le publier. Aucun texte n'impose la relecture par un juriste : le contrat de
l'article 28 est obligatoire, sa relecture est un choix, dont le propriétaire assume le risque s'il y
renonce.

### Reconstruction du 6 novembre 2026

Le propriétaire a retenu le 06/11/2026 de se fonder sur les clauses types de la décision d'exécution (UE)
2021/915, pour réduire la relecture à payer. Leur texte officiel (EUR-Lex) est repris par programme ;
chaque paragraphe a été comparé à l'original : seuls manquent l'option 1 de la clause 7.7 et la clause
7.7 e). **Le DPA n'est donc pas une adoption intégrale** de ces clauses, mais un contrat « en partie »
fondé sur elles (article 28, paragraphe 6, du RGPD), qui s'en écarte sur deux points :

| Écart | Motif | Constat |
| --- | --- | --- |
| Clause 7.7 e) non reprise (tiers bénéficiaire si E-Com Shop disparaît) | Promesse impossible à tenir | Lu le 06/11/2026 : aucun droit de ce type chez Render, Anthropic et OpenAI ; Sentry exclut les tiers bénéficiaires hors clauses de transfert. AWS non vérifié |
| Clause 7.7 a) : exception de remplacement urgent (B3) | Décision du 21/10/2026, conservée | Sans elle, changer d'hébergeur en urgence violerait le préavis de 30 jours |

Les clauses complémentaires (partie B) reprennent les décisions du 21/10/2026. Les questions à soumettre
sont dans `QUESTIONS-AVOCAT.md`.

### Informations à compléter par E-Com Shop

| Information | Article du DPA |
| --- | --- |
| Contrat de SerpApi, demandé le 06/11/2026 (les autres sont rangés dans `documents prestataires`) | Annexe IV |
| Mécanisme de transfert de SerpApi ; région d'Amazon S3 ; accès de Sentry depuis un pays tiers. Registre DPF relevé le 06/11/2026 pour les autres (14 captures rangées) : à revérifier avant publication, Render et Netlify arrivant à échéance en décembre 2026 | Annexe IV |
| Conditions applicables à l'API PageSpeed de Google | Annexe IV |
| Droits limités des clés d'accès AWS ; suppression effective des sauvegardes à 30 jours (date de la plus ancienne dans la console S3) | Annexe III |
| Mesures organisationnelles réellement appliquées : authentification à deux facteurs, poste de travail chiffré, procédure d'incident ; désignation d'un délégué à la protection des données | Annexes I et III |

### Faits à confirmer

| Fait | Article du DPA |
| --- | --- |
| La sauvegarde quotidienne et sa suppression à 30 jours fonctionnent en production, et une restauration est possible. Elles sont configurées dans `render.yaml` (tâche `noyaru-db-backup`, `BACKUP_RETENTION_DAYS=30`), sans preuve de leur fonctionnement. **Depuis le 05/11/2026**, un essai automatique la vérifie chaque mois (tâche `noyaru-restauration-test`, `backend/restauration.py`) : la dernière sauvegarde est restaurée dans une base jetable, son empreinte et ses tables contrôlées, le rapport gardé au journal d'audit (`backup.restore_check`) et envoyé au propriétaire. **Premier essai réussi le 06/11/2026** : sauvegarde du 06/11/2026 à 02:01 UTC restaurée, 42 tables, 1 988 lignes, 7 comptes, 4,5 s (recoupé avec le code et la production). La suppression à 30 jours reste à constater. | Annexe III |
| Les traces d'erreur envoyées à Sentry contiennent-elles des données personnelles des clients ? **Constaté le 05/11/2026 : OUI jusqu'à ce jour** (variables du code, formulaires reçus, paramètres d'adresse, e-mails dans les journaux, aucun filtre). **Corrigé le même jour** (`backend/sentry_filtre.py`, testé avec le vrai SDK) : ni variables, ni formulaires, ni cookies, ni paramètres d'adresse ; e-mails et jeton Telegram masqués. Reste possible : un nom écrit dans le texte d'une erreur. | Annexe IV |
| SerpAPI et Ahrefs ne reçoivent-ils vraiment aucune donnée personnelle ? **Constaté le 05/11/2026 (lecture du code)** : SerpAPI reçoit des recherches courtes générées à partir des titres des pages, sans le nom ni la marque du site, ou les mots-clés que le Client saisit lui-même (recherche d'opportunités de liens) ; une recherche saisie par le Client peut donc contenir une donnée personnelle. Ahrefs n'est pas activé ; il recevrait le domaine analysé. | Annexe IV |
| Google PageSpeed Insights, appelé avec la clé d'E-Com Shop sur les adresses des pages du client : ne reçoit-il vraiment aucune donnée personnelle ? **Constaté le 05/11/2026 (lecture du code)** : il reçoit l'adresse publique de la page, la stratégie (mobile ou ordinateur) et la catégorie mesurée, rien d'autre ; Google charge lui-même la page publique. | Annexe IV |

### Décisions contractuelles arrêtées le 21 octobre 2026

Le propriétaire a délégué ces choix le 21/10/2026, en demandant les solutions les plus sûres
juridiquement. Ce sont des choix courants et équilibrés ; ils sont repris dans la partie B du DPA.

| Décision | Article du DPA | Choix retenu | Pourquoi |
| --- | --- | --- | --- |
| Conservation des preuves d'acceptation | B1 | Pendant l'accord, puis 5 ans | Durée de la prescription de droit commun des actions entre commerçants et en matière civile. |
| Liste à jour des sous-traitants ultérieurs | B3 | Communiquée sur demande à contact@noyaru.com ; prestataires repris dans la politique de confidentialité | Rend la liste accessible sans construire de page nouvelle. |
| Préavis avant un nouveau sous-traitant ultérieur | 7.7 a), B3 | 30 jours, par e-mail à l'adresse du compte | Délai courant, qui laisse au client le temps d'examiner et de s'opposer. |
| Objection non résolue | B3 | Résiliation sans pénalité après 30 jours, avec remboursement au prorata de la période payée non utilisée | Rend le droit d'opposition effectif sans pénaliser le client. |
| Notification d'une violation de données | B4 | Sans retard injustifié et, dans la mesure du possible, dans les 48 heures ; par e-mail à l'adresse du compte ; contact de suivi à contact@noyaru.com | Laisse au client le temps de notifier lui-même l'autorité dans ses 72 heures. « Dans la mesure du possible » évite un délai impossible à tenir. |
| Choix entre restitution et suppression | B5 | Par écrit à contact@noyaru.com ; supprimer ses projets ou son compte vaut suppression ; suppression à défaut de choix dans les 30 jours | Respecte le choix du client exigé par l'article 28.3.g, avec une règle claire s'il ne répond pas. |
| Restitution des données non exportables | B5 | Remise dans les 30 jours, dans un format structuré et couramment utilisé | Rend la restitution réelle, au-delà des exports existants. |
| Audits | B6 | Préavis de 30 jours ; une fois par période de 12 mois ; chacun ses frais ; frais d'un audit sur place remboursés par le client, sauf manquement significatif révélé ou audit exigé par une autorité | Garantit le droit d'audit sans le rendre disproportionné pour une petite société. |
| Responsabilité entre les parties | B7 | Mêmes limites que l'article 10 des CGU, réciproques, choisies expressément ; jamais en cas de faute lourde, de dol ou de règle impérative ; sans effet sur les personnes concernées (article 82) ni sur les amendes | Cohérent avec les CGU, tout en écartant les cas où une limitation serait nulle ou inopposable. |
| Services connectés par le client | Annexe IV | Prestataires du client quand il les a souscrits et qu'E-Com Shop y accède avec son autorisation ; ajoutés à la liste s'ils traitaient des données pour E-Com Shop | Qualification fondée sur la relation contractuelle réelle, sans exclusion d'office. |

### Engagements qui dépendent de vous

| Engagement | Article du DPA | Ce qu'il suppose |
| --- | --- | --- |
| Notifier une violation de données dans les 48 heures, dans la mesure du possible | B4 | Lire contact@noyaru.com et les alertes du service chaque jour ouvré. |
| Rembourser au prorata un client qui résilie après une objection | B3 | Un remboursement manuel dans Stripe, comme pour une rétractation. |
| Restituer en 30 jours les données que les exports ne couvrent pas | B5 | Le client les télécharge lui-même depuis « Mon compte → Mes données » (05/11/2026) ; au-delà de 300 Mo de fichiers d'analyse, les fichiers laissés de côté sont listés dans le ZIP et remis sur demande. |
| Prévenir les clients 30 jours avant un nouveau sous-traitant ultérieur | 7.7 a), B3 | Une annonce « Politique de confidentialité, et liste des prestataires » depuis l'écran « Annonces aux clients ». |
| Faire signer un engagement de confidentialité avant de donner accès aux données à une autre personne | Annexe III | Le propriétaire, seul à accéder aux données le 21/10/2026, l'a indiqué ce jour-là. |
| Prévenir le client du manquement d'un sous-traitant ultérieur, ou s'il devient impossible de respecter les clauses (nouveau, 06/11/2026) | 7.7 d), 10 a) | Surveiller les annonces des prestataires et écrire au client concerné. |
| Suspendre le traitement sur instruction du client en cas de manquement ; accepter sa résiliation dans les cas prévus (nouveau, 06/11/2026) | 10 a), 10 b) | Une action manuelle : aucun bouton de suspension n'existe. |
| Certifier la suppression sur demande, en indiquant l'échéance des sauvegardes (nouveau, 06/11/2026) | 10 d), B5 | Répondre par e-mail à contact@noyaru.com. |
| Fournir une copie des contrats des sous-traitants ultérieurs (nouveau, 06/11/2026) | 7.7 c), B3 | Les contrats publics sont indiqués à l'annexe IV ; les autres sont rangés dans `documents prestataires`. |

### Où trouver le contrat de traitement de chaque sous-traitant ultérieur

Relevé le 21 octobre 2026 sur les pages des prestataires. Pour chacun : télécharger ou accepter le
contrat, le ranger, et vérifier sa certification au cadre UE–États-Unis sur
dataprivacyframework.gov (rubrique « Data Privacy Framework List »).

| Prestataire | Où est le contrat | Comment l'obtenir | Transferts, d'après le contrat |
| --- | --- | --- | --- |
| Render | render.com/dpa | Contrat lié aux conditions de Render ; enregistrer la page en PDF, ou le demander au support si une version signée est nécessaire | Certifié au cadre UE–États-Unis depuis le 6 janvier 2025 (annonce de Render) ; clauses types en complément |
| Amazon Web Services | d1.awsstatic.com/legal/aws-dpa/aws-dpa.pdf (corrigé le 05/11/2026 : AWS Artifact n'est pas nécessaire) | Contrat intégré aux conditions de service AWS, applicable d'office à tous les clients ; enregistrer le PDF officiel | Clauses types incluses dans le contrat |
| Anthropic | anthropic.com/legal/data-processing-addendum | Intégré automatiquement aux conditions commerciales de l'API ; rien à signer ; enregistrer la page en PDF | Clauses types (modules 2 et 3) réputées signées ; liste des sous-traitants : anthropic.com/subprocessors |
| OpenAI | openai.com/policies/data-processing-addendum | Intégré au contrat de l'API ; un client établi dans l'EEE contracte avec OpenAI Ireland Ltd ; pour une version signée, remplir le formulaire de la page (d'après OpenAI, 05/11/2026), ou écrire à privacy@openai.com | À vérifier dans le contrat |
| Google (Gemini) — **désactivé le 05/11/2026** (clé retirée de la production : l'assistant passe par Claude ; Gemini disparaît de la confidentialité et n'a plus besoin de contrat tant qu'il l'est) | business.safety.google/processorterms, d'après les conditions de l'API Gemini (ai.google.dev/gemini/terms) | S'applique à l'usage payant (projet Google Cloud avec facturation) et, pour l'EEE, à tout usage ; vérifier que le projet a une facturation active | À vérifier dans le contrat |
| Sentry | sentry.io/legal/dpa | À accepter en ligne : Paramètres de l'organisation → Legal & Compliance, par un membre au rôle Owner ou Billing ; Sentry garde qui l'a accepté et quand | Cadre UE–États-Unis, et clauses types (modules 2 et 3) à défaut ; liste des sous-traitants : sentry.io/legal/subprocessors |
| SerpApi | Sur demande | Écrire à privacy@serpapi.com pour obtenir le contrat en vigueur | Clauses types citées dans sa politique ; aucune certification mentionnée ; recherches gardées 31 jours (aucune avec le mode ZeroTrace) |
| Ahrefs (non activé le 21/10/2026) | ahrefs.com/legal (Data Processing Addendum) | À faire seulement si l'intégration est activée ; les clauses types deviennent applicables une fois signées et envoyées par e-mail à Ahrefs | Clauses types (modules 2 et 3) |

### Où trouver le contrat des prestataires de Noyaru pour ses propres traitements

Ajouté le 5 novembre 2026. Ces prestataires ne sont pas des sous-traitants ultérieurs du DPA : ils
traitent des données dont E-Com Shop est responsable (comptes, facturation, e-mails, inscription,
visiteurs du site public), pas des données traitées pour le compte du Client. L'article 28 du RGPD
impose tout de même un contrat de traitement avec chacun. Même démarche que ci-dessus : récupérer ou
accepter le contrat, le ranger, vérifier la certification au cadre UE–États-Unis.

| Prestataire | Où est le contrat | Comment l'obtenir | Transferts, d'après le contrat |
| --- | --- | --- | --- |
| Brevo (envoi des e-mails) | À la fin des conditions générales d'utilisation de Brevo, d'après son centre d'aide | Enregistrer la page en PDF ; vérifier dans le compte Brevo si une acceptation est demandée | Société française ; ses propres sous-traitants à vérifier dans le contrat |
| Stripe (paiement et facturation) | stripe.com/legal/dpa | Intégré au contrat de services Stripe, rien à signer ; pour un compte européen, le contrat est passé avec Stripe Payments Europe, Limited (Irlande) ; enregistrer la version européenne en PDF | Avenant de transferts intégré au contrat ; le cadre UE–États-Unis prime quand plusieurs mécanismes s'appliquent |
| Cloudflare (captcha Turnstile du formulaire d'inscription, depuis le 05/11/2026) | cloudflare.com/cloudflare-customer-dpa | Contrat lié aux conditions d'abonnement, y compris en libre-service ; sa documentation ne dit pas si un compte gratuit l'accepte automatiquement : le vérifier dans le tableau de bord Cloudflare ; enregistrer en PDF. Cloudflare se dit sous-traitant pour la détection des robots, mais responsable pour l'amélioration de Turnstile | À vérifier dans le contrat |
| Google Analytics (mesure d'audience du site public, depuis le 02/11/2026) | Dans Google Analytics : Administration → Paramètres du compte → avenant relatif au traitement des données (« Google Ads Data Processing Terms ») | « Examiner l'avenant », l'accepter, puis renseigner les coordonnées de l'organisation ; il faut le rôle Éditeur sur le compte | À vérifier dans le contrat |
| Telegram (notifications du propriétaire, depuis le 02/11/2026) | Aucun contrat de traitement connu pour les bots | À soumettre au juriste : Telegram reçoit les adresses e-mail des comptes, le forfait et le pays déclaré | Émirats arabes unis (pays à confirmer) ; aucun mécanisme connu |

| Resend et SendGrid/Twilio (réponses du SAV, si configurés à la place de Brevo, PR #75 du 05/11/2026) | Non récupéré | À récupérer seulement si l'un d'eux est activé. Conseil du 06/11/2026 : rester sur Brevo, déjà sous contrat | Registre DPF non relevé |
| Gmail réservée au SAV (si activée, PR #75 du 05/11/2026) | Conditions de Google Workspace ou du compte | À vérifier : type de compte, et contrat de traitement applicable | Google LLC, DPF actif |

Telegram reçoit aussi, depuis la PR #75, le correspondant et sa demande SAV réduite : à soumettre au juriste
avec le point ci-dessus. Ghost, PrestaShop, Wix et Webflow sont nommés depuis le 06/11/2026 dans « Services
connectés par le Client » (annexe IV du DPA).

**À corriger avec l'accord du propriétaire** : la confidentialité présente OpenAI comme « en secours
d'Anthropic », alors qu'il génère aussi les illustrations d'articles du plan Business (`gpt-image-2`).
L'annexe IV du DPA dit déjà les deux.

### Points à faire valider par un juriste

Depuis le 06/11/2026, seulement ce que liste `QUESTIONS-AVOCAT.md` : les deux écarts, les transferts sans
certification, Telegram, la qualification des services connectés, les sauvegardes, et les clauses de la partie
B (acceptation, frais d'audit, plafond de responsabilité). Le texte officiel de la partie A n'a pas à être relu.

### Développements nécessaires avant de proposer le DPA

Ceux qui restent sont marqués « À METTRE EN ŒUVRE » dans le DPA.

- L'adresse du Client dans le formulaire d'acceptation (annexe I du DPA, ajoutée le 06/11/2026 : les clauses
  de la Commission la demandent).

- ~~La case à cocher de l'article 1 et la conservation des preuves.~~ **Réalisée le 05/11/2026**
  (`backend/dpa.py`, table `dpa_acceptances`) : organisation, immatriculation, personne et
  fonction, compte et e-mail, date et heure, version, empreinte du texte exact et sa langue, IP et
  navigateur ; preuve gardée après la suppression du compte, effacée cinq ans après la fin de
  l'accord. **Masquée** tant que `legal.DPA_VERSION` vaut None : une fois la décision de publier
  prise, y mettre le numéro de version publié (un test rappelle ce verrou).
- ~~La procédure de restitution des données que les exports ne couvrent pas (article 15).~~
  **Réalisée le 05/11/2026** : « Mon compte → Mes données » télécharge un ZIP de toutes les données
  de la base rattachées au compte et à ses projets (JSON et CSV, déduites du schéma, sans secret ni
  donnée d'un autre compte) et des fichiers d'analyse jusqu'à 300 Mo (`backend/export_compte.py`).
