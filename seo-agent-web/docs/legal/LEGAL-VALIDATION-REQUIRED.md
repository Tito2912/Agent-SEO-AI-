# Pages juridiques — validations nécessaires avant publication

Le 21 octobre 2026, le propriétaire a validé la publication des six pages (CGU, confidentialité,
mentions légales, en français et en anglais ; demande #39). Les points ci-dessous restent ouverts
après cette validation : chacun est à régler dès que l'information est disponible.

Les valeurs de l'éditeur sont dans `seo-agent-web/backend/legal.py` ; une valeur inconnue y vaut
`None`. Le détail des changements est dans `LEGAL-CHANGES.md`.

## 1. Informations manquantes

| Information | Document | Pourquoi |
| --- | --- | --- |
| Capital social d'E-Com Shop (`share_capital`) | Mentions légales FR/EN, CGU article 1 | Mention obligatoire pour une société (LCEN, art. 6 III). Elle n'est pas affichée tant qu'elle est inconnue (l'annuaire officiel des entreprises ne la donne pas). Donnez le montant exact. |
| Durée de conservation des journaux techniques chez Render, selon votre offre | Confidentialité § 7 (FR/EN) | La page dit « la durée appliquée par notre hébergeur ». L'audit demande une durée exacte : indiquez-la, sans l'estimer. |
| Durée de conservation des traces d'erreur chez Sentry | Confidentialité § 4 et § 7 | Aucune durée n'est indiquée. À fournir si vous voulez la mentionner. |

## 2. Faits à confirmer

| Point | Document | Ce qu'il faut confirmer |
| --- | --- | --- |
| Adhésion au médiateur CM2C | CGU article 12 (FR/EN) | Les CGU nomment CM2C, comme la version en ligne. Confirmez qu'E-Com Shop y est bien adhérente et que l'adresse (49 rue de Ponthieu, 75008 Paris) est à jour. |
| Mécanisme de transfert de chaque prestataire hors UE | Confidentialité § 5, DPA article 11 | La page ne dit plus quel mécanisme couvre quel prestataire. Pour Render, Stripe, Anthropic, OpenAI, Google, GitHub, Microsoft, SerpAPI, Netlify, Amazon Web Services, Sentry (et Ahrefs s'il est activé) : certification au cadre UE–États-Unis, ou clauses contractuelles types signées. La page promet d'en fournir la description sur demande. |
| Conditions d'entraînement des modèles d'IA | Confidentialité § 6 | Vérifier les conditions API actuelles d'Anthropic et d'OpenAI, et l'offre Gemini utilisée (payante ou gratuite) : l'exclusion de l'entraînement en dépend. |
| Prix « toutes taxes comprises » pour les consommateurs | CGU article 3 | La phrase est conservée de la version en ligne. Confirmer que le paramétrage Stripe affiche bien des prix TTC aux consommateurs. |
| Coordonnées de l'hébergeur Render | Mentions légales FR/EN | Reprises de la version en ligne (525 Brannan Street, Suite 300, San Francisco). À vérifier avant publication. |
| Téléphone publié (06 14 97 64 01) | Mentions légales FR/EN | Repris de la version en ligne. Confirmer qu'il doit rester affiché. |

## 3. Engagements qui dépendent de vous

| Engagement | Document | Dépendance |
| --- | --- | --- |
| Informer les clients par e-mail avant une modification substantielle des CGU | CGU article 13 (FR/EN) | Le service n'a **pas** de fonction d'envoi automatique pour cela. L'engagement suppose un envoi manuel de votre part à chaque modification substantielle. Confirmez-le, ou demandez-moi d'ajouter cette fonction. |
| Rembourser une rétractation sous 14 jours | CGU article 5 (FR/EN) | Le remboursement est une opération manuelle dans Stripe (`RUNBOOK.md`, « Rétractation »). La case « Je demande que mon abonnement commence immédiatement » existe bien avant le paiement. |
| Fournir un accord de traitement des données (DPA) sur demande | Confidentialité § 1, CGU article 11 | Les pages annoncent qu'il « peut être demandé ». Le DPA (`DPA-fr.md`, `DPA-en.md`) est un brouillon non opérationnel, corrigé le 21/10/2026 : il ne peut être proposé aux clients qu'une fois réglés les points de la section 5. Mode d'acceptation choisi le 21/10/2026 : une case à cocher dans le service. |
| Décrire les garanties de transfert sur demande | Confidentialité § 5 | Suppose d'avoir les justificatifs du point 2. |

## 4. Décisions juridiques à faire valider

| Clause | Document | Question |
| --- | --- | --- |
| Clause de langue (français version de référence) | CGU article 14 | Proposition contractuelle de l'audit : son opposabilité n'est pas garantie dans tous les pays. |
| Réserve « faute lourde ou dol » du plafond de responsabilité des professionnels | CGU article 10 | Rédaction à valider. |
| Compétence des tribunaux de Paris pour les professionnels | CGU article 12 | Conservée, et désormais limitée aux cas où elle est valable et opposable. |
| Rôle de sous-traitant d'E-Com Shop et services du client (GitHub, WordPress, Search Console…) | Confidentialité § 1, DPA annexe 3 | Le DPA ne les exclut plus d'office des sous-traitants ultérieurs : leur qualification dépend des contrats et des opérations de traitement, à confirmer service par service (section 5). |
| Cookie de langue `noyaru_lang` (1 an) | Confidentialité § 8 | Présenté comme un cookie de préférence choisie, sans consentement. À confirmer. |
| DPA dans son ensemble | `DPA-fr.md`, `DPA-en.md` | Brouillon corrigé le 21/10/2026, à faire valider (section 5). |

## 5. Contrat de traitement des données (DPA)

Corrigé le 21 octobre 2026 en français et en anglais (détail dans `LEGAL-CHANGES.md`). Il reste un
brouillon : il ne peut être proposé aux clients qu'une fois réglés tous les points ci-dessous.

### Informations à compléter par E-Com Shop

| Information | Article du DPA |
| --- | --- |
| Accord de traitement en vigueur avec chaque sous-traitant ultérieur | 10, annexe 3 |
| Mécanisme de transfert, justificatif et région de traitement confirmée de chaque sous-traitant ultérieur (Render, Amazon Web Services, Anthropic, OpenAI, Google Gemini, Sentry, SerpAPI, Ahrefs) | 11 |
| Durée de conservation appliquée par chaque sous-traitant ultérieur | 15 |
| Protections appliquées aux sauvegardes (chiffrement du stockage, accès restreints) | 15 |
| Mesures organisationnelles réellement appliquées (accès internes, incidents, tests de sécurité) | Annexe 2 |

### Faits à confirmer

| Fait | Article du DPA |
| --- | --- |
| La sauvegarde quotidienne et sa suppression à 30 jours fonctionnent en production, et une restauration est possible. Elles sont configurées dans `render.yaml` (tâche `noyaru-db-backup`, `BACKUP_RETENTION_DAYS=30`), sans preuve de leur fonctionnement. | 15, annexe 2 |
| Les traces d'erreur envoyées à Sentry contiennent-elles des données personnelles des clients ? | Annexe 3 |
| SerpAPI et Ahrefs ne reçoivent-ils vraiment aucune donnée personnelle ? | Annexe 3 |
| Google PageSpeed Insights, appelé avec la clé d'E-Com Shop sur les adresses des pages du client : ne reçoit-il vraiment aucune donnée personnelle ? | Annexe 3 |

### Décisions contractuelles arrêtées le 21 octobre 2026

Le propriétaire a délégué ces choix le 21/10/2026, en demandant les solutions les plus sûres
juridiquement. Ce sont des choix courants et équilibrés ; ils restent à faire relire par un juriste.

| Décision | Article du DPA | Choix retenu | Pourquoi |
| --- | --- | --- | --- |
| Conservation des preuves d'acceptation | 1 | Pendant l'accord, puis 5 ans | Durée de la prescription de droit commun des actions entre commerçants et en matière civile. |
| Liste à jour des sous-traitants ultérieurs | 10 | Communiquée sur demande à contact@noyaru.com ; prestataires repris dans la politique de confidentialité | Rend la liste accessible sans construire de page nouvelle. |
| Préavis avant un nouveau sous-traitant ultérieur | 10 | 30 jours, par e-mail à l'adresse du compte | Délai courant, qui laisse au client le temps d'examiner et de s'opposer. |
| Objection non résolue | 10 | Résiliation sans pénalité après 30 jours, avec remboursement au prorata de la période payée non utilisée | Rend le droit d'opposition effectif sans pénaliser le client. |
| Notification d'une violation de données | 13 | Sans retard injustifié et, dans la mesure du possible, dans les 48 heures ; par e-mail à l'adresse du compte ; contact de suivi à contact@noyaru.com | Laisse au client le temps de notifier lui-même l'autorité dans ses 72 heures. « Dans la mesure du possible » évite un délai impossible à tenir. |
| Choix entre restitution et suppression | 15 | Par écrit à contact@noyaru.com ; supprimer ses projets ou son compte vaut suppression ; suppression à défaut de choix dans les 30 jours | Respecte le choix du client exigé par l'article 28.3.g, avec une règle claire s'il ne répond pas. |
| Restitution des données non exportables | 15 | Remise dans les 30 jours, dans un format structuré et couramment utilisé | Rend la restitution réelle, au-delà des exports existants. |
| Audits | 16 | Préavis de 30 jours ; une fois par période de 12 mois ; chacun ses frais ; frais d'un audit sur place remboursés par le client, sauf manquement significatif révélé ou audit exigé par une autorité | Garantit le droit d'audit sans le rendre disproportionné pour une petite société. |
| Responsabilité entre les parties | 17 | Mêmes limites que l'article 10 des CGU, réciproques, choisies expressément ; jamais en cas de faute lourde, de dol ou de règle impérative ; sans effet sur les personnes concernées (article 82) ni sur les amendes | Cohérent avec les CGU, tout en écartant les cas où une limitation serait nulle ou inopposable. |
| Services connectés par le client | Annexe 3 | Prestataires du client quand il les a souscrits et qu'E-Com Shop y accède avec son autorisation ; ajoutés à la liste s'ils traitaient des données pour E-Com Shop | Qualification fondée sur la relation contractuelle réelle, sans exclusion d'office. |

### Engagements qui dépendent de vous

| Engagement | Article du DPA | Ce qu'il suppose |
| --- | --- | --- |
| Notifier une violation de données dans les 48 heures, dans la mesure du possible | 13 | Lire contact@noyaru.com et les alertes du service chaque jour ouvré. |
| Rembourser au prorata un client qui résilie après une objection | 10 | Un remboursement manuel dans Stripe, comme pour une rétractation. |
| Restituer en 30 jours les données que les exports ne couvrent pas | 15 | Une procédure d'extraction, à mettre en œuvre (voir ci-dessous). |
| Prévenir les clients 30 jours avant un nouveau sous-traitant ultérieur | 10 | Un e-mail manuel à chaque client concerné. |
| Faire signer un engagement de confidentialité avant de donner accès aux données à une autre personne | 8 | Le propriétaire, seul à accéder aux données le 21/10/2026, l'a indiqué ce jour-là. |

### Où trouver le contrat de traitement de chaque sous-traitant ultérieur

Relevé le 21 octobre 2026 sur les pages des prestataires. Pour chacun : télécharger ou accepter le
contrat, le ranger, et vérifier sa certification au cadre UE–États-Unis sur
dataprivacyframework.gov (rubrique « Data Privacy Framework List »).

| Prestataire | Où est le contrat | Comment l'obtenir | Transferts, d'après le contrat |
| --- | --- | --- | --- |
| Render | render.com/dpa | Contrat lié aux conditions de Render ; enregistrer la page en PDF, ou le demander au support si une version signée est nécessaire | Certifié au cadre UE–États-Unis depuis le 6 janvier 2025 (annonce de Render) ; clauses types en complément |
| Amazon Web Services | AWS Artifact, dans la console AWS (rubrique Agreements) | Contrat intégré aux conditions de service AWS ; le télécharger depuis AWS Artifact | Clauses types incluses dans le contrat |
| Anthropic | anthropic.com/legal/data-processing-addendum | Intégré automatiquement aux conditions commerciales de l'API ; rien à signer ; enregistrer la page en PDF | Clauses types (modules 2 et 3) réputées signées ; liste des sous-traitants : anthropic.com/subprocessors |
| OpenAI | openai.com/policies/data-processing-addendum | Accepté par l'utilisation des services ; un client établi dans l'EEE contracte avec OpenAI Ireland Ltd ; enregistrer la page en PDF | À vérifier dans le contrat |
| Google (Gemini) | business.safety.google/processorterms, d'après les conditions de l'API Gemini (ai.google.dev/gemini/terms) | S'applique à l'usage payant (projet Google Cloud avec facturation) et, pour l'EEE, à tout usage ; vérifier que le projet a une facturation active | À vérifier dans le contrat |
| Sentry | sentry.io/legal/dpa | À accepter en ligne : Paramètres de l'organisation → Legal & Compliance, par une personne habilitée | Cadre UE–États-Unis, et clauses types (modules 2 et 3) à défaut ; liste des sous-traitants : sentry.io/legal/subprocessors |
| SerpApi | Sur demande | Écrire à privacy@serpapi.com pour obtenir le contrat en vigueur | Clauses types citées dans sa politique ; aucune certification mentionnée ; recherches gardées 31 jours (aucune avec le mode ZeroTrace) |
| Ahrefs (non activé le 21/10/2026) | ahrefs.com/legal (Data Processing Addendum) | À faire seulement si l'intégration est activée ; les clauses types deviennent applicables une fois signées et envoyées par e-mail à Ahrefs | Clauses types (modules 2 et 3) |

### Points à faire valider par un juriste

- Le DPA dans son ensemble, en vérifiant que les versions française et anglaise donnent les mêmes
  droits, obligations, délais et exceptions.
- L'acceptation par case à cocher (article 1) : déclaration de pouvoir de la personne qui coche,
  preuves conservées et leur durée de conservation, nouvelle acceptation d'une version modifiée.
- L'articulation entre CGU, DPA et RGPD, et le régime de responsabilité (article 17).
- La qualification des services connectés par le client (annexe 3).
- Le module de clauses contractuelles types de chaque transfert (article 11).
- Les décisions contractuelles arrêtées le 21 octobre 2026 (sous-section précédente).

### Développements nécessaires avant de proposer le DPA

Aucun n'est réalisé ; ils sont marqués « À METTRE EN ŒUVRE » dans le DPA.

- La case à cocher de l'article 1 et la conservation des preuves : organisation contractante,
  compte et personne, date et heure, version exacte, trace de l'action d'acceptation.
- La procédure de restitution des données que les exports ne couvrent pas, dans les 30 jours, si
  le client choisit la restitution (article 15).
