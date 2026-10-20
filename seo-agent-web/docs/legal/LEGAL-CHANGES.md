# Pages juridiques — rapport des modifications

Application de l'audit juridique d'octobre 2026, sans nouvel audit. Branche
`claude/noyaru-pages-juridiques`, non fusionnée, non publiée. Les validations restantes sont dans
`LEGAL-VALIDATION-REQUIRED.md`.

Aucune fonction du service n'a changé : ni Stripe, ni tarifs, quotas, paiements ou remboursements,
ni GitHub, ni corrections automatiques, ni base de données. Le design est celui des pages
existantes.

## Conditions générales (`/terms`, `/en/terms`)

| Section | Modification | Raison (audit) |
| --- | --- | --- |
| 1. Éditeur et objet | L'éditeur est toujours nommé (E-Com Shop, SASU, siège, RCS) ; mention des corrections dans WordPress. | 3.1 : une variable vide faisait disparaître l'éditeur. |
| 3. Prix | Clause TVA réécrite : taxes selon les règles applicables, autoliquidation seulement « lorsque l'opération en relève » (phrase affichée seulement si Stripe calcule la TVA), total présenté avant validation. « TTC » conservé pour les consommateurs. | 1.1 |
| 4. Résiliation de l'abonnement | Titre précisé. | 6 (terminologie) |
| 5. Rétractation | La demande d'exécution immédiate ne fait pas perdre le droit de rétractation ; rétractation et résiliation distinguées. Formulaire conservé. | 1.8 |
| 6. Dépôt, site et modifications | Pull requests « lorsqu'une modification passe par GitHub » ; cas WordPress (application sur demande, rétablissement) ; fusion automatique sans relecture humaine préalable ; limites des contrôles automatisés. | 1.2 |
| 7. Contenus produits par IA (nouveau) | Erreurs possibles, vérification, droits des tiers, absence de garantie de classement, rôle des modifications automatiques. | 1.3 |
| 10. Responsabilité | Plafond professionnel conservé, avec réserve faute lourde, dol et responsabilités qu'une règle impérative interdit de limiter ; garantie légale de conformité conservée. | 1.4 |
| 11. Données personnelles | Renvoi au rôle de sous-traitant. | 2.1 |
| 12. Litiges et droit applicable | Droit français sans préjudice des règles impératives du pays de résidence ; juridictions compétentes ; clauses professionnelles limitées à leur validité ; médiateur toujours nommé. | 1.5 |
| 13. Modifications | Datation, information des clients avant une modification substantielle, pas d'effet rétroactif, droit de résilier. | 1.7 |
| 14. Langue (nouveau) | Clause de langue proposée par l'audit. | 1.6 |

Les articles 8 à 15 sont renumérotés (un article ajouté avant, un après).

## Politique de confidentialité (`/privacy`, `/en/privacy`)

| Section | Modification | Raison (audit) |
| --- | --- | --- |
| 1. Qui traite vos données | Responsable du traitement et sous-traitant distingués ; renvoi au DPA. | 2.1 |
| 2. Données | Langue d'affichage ; connexions Netlify et WordPress ajoutées (stockées chiffrées, vérifié dans le code). | Exactitude |
| 3. Bases légales | « Comptes & connexions » remplacé par « Mon compte → Vie privée ». | 2.7 |
| 5. Transferts | Décisions d'adéquation, cadre UE–États-Unis pour les entreprises certifiées, clauses contractuelles types, mesures supplémentaires ; description sur demande. Aucune certification affirmée. | 2.2 |
| 6. IA | Données nécessaires à la fonction utilisée ; exclusion de l'entraînement présentée comme dépendant des conditions de chaque fournisseur (et de l'offre pour Gemini). | 2.6 |
| 7. Conservation | Exceptions légales précisées ; journaux : « durée appliquée par l'hébergeur selon l'offre », en attente d'une durée exacte. | 2.3 |
| 8. Cookies | Tableau des trois cookies réellement posés par le code (session 30 jours, protection CSRF 30 jours, langue 1 an) et stockage local (bandeau, historique de l'assistant, recherche de concurrents). | 5 |
| 9. Sécurité | Pull requests validées par défaut, fusion automatique possible après activation. | 2.4 |
| 10. Droits | Réclamation auprès de la CNIL ou d'une autre autorité compétente. | 2.5 |

**Écart corrigé** : l'ancienne page citait « un cookie temporaire pendant une connexion avec
Google ». Le code n'en pose aucun (la connexion Google passe par un paramètre signé) ; il pose en
revanche le cookie de langue `noyaru_lang`, qui n'était pas mentionné.

## Mentions légales (`/mentions-legales`, `/en/mentions-legales`)

- Une ligne par mention : dénomination sociale, forme juridique, capital social (dès qu'il sera
  connu), siège social, immatriculation, TVA intracommunautaire, téléphone, e-mail, directeur de la
  publication, hébergeur. Valeurs reprises de la version en ligne du 20/10/2026. (3.1, 3.2)
- Traduction anglaise avec la terminologie de l'audit (Website Publisher, Registered Office,
  Company Registration Number, EU VAT Identification Number, Publication Director, Hosting
  Provider) ; dénominations officielles françaises gardées (SASU, R.C.S.). (3.3)
- Date de mise à jour ajoutée.

## Intégration bilingue

- Adresses anglaises `/en/terms`, `/en/privacy`, `/en/mentions-legales`, suivant la convention du
  site (`/en` + adresse française) ; les adresses françaises sont inchangées.
- Adresse canonique, balises hreflang (fr, en, x-default) et `og:locale` par langue ; versions
  anglaises au plan du site ; bouton FR/EN ; liens entre documents et pied de page dans la langue
  de la page ; titres et descriptions traduits.
- Libellés anglais du pied de page : Terms and Conditions, Privacy Policy, Legal Notice.
- Page d'inscription : « Retrait à tout moment dans « Mon compte → Vie privée » » (au lieu de
  « Comptes & connexions »), en français et en anglais. (2.7)

## Fichiers

Modifiés :
- `seo-agent-web/templates/terms_public.html`, `privacy_public.html`, `legal_notice_public.html`
- `seo-agent-web/templates/auth_signup.html` (libellé de l'écran)
- `seo-agent-web/backend/app.py` (routes des trois pages : choix de la langue, `_legal_page`)
- `seo-agent-web/backend/i18n.py` (les trois adresses dans `LOCALIZED_PAGES`)
- `seo-agent-web/locales/en.json` (pied de page, inscription, localisations)
- `RUNBOOK.md` (médiateur)
- Tests mis à jour : `test_les_cgu_disent_ce_que_fait_le_code.py`,
  `test_les_consommateurs_ont_leurs_droits.py`, `test_les_mentions_legales_sont_publiees.py`,
  `test_la_confidentialite_nomme_chaque_tiers.py`

Créés :
- `seo-agent-web/backend/legal.py` (éditeur, médiateur, hébergeur, dates : source unique)
- `seo-agent-web/templates/terms_public_en.html`, `privacy_public_en.html`, `legal_notice_public_en.html`
- `seo-agent-web/templates/_legal_styles.html` (mise en forme commune, reprise telle quelle)
- `seo-agent-web/docs/legal/DPA-fr.md`, `DPA-en.md` (brouillons non publiés)
- `seo-agent-web/docs/legal/LEGAL-VALIDATION-REQUIRED.md`, `LEGAL-CHANGES.md`
- `seo-agent-web/tests/test_les_pages_juridiques_sont_bilingues.py`

## Changement de fonctionnement à connaître

L'identité de l'éditeur et le médiateur ne sont plus lus dans les variables Render `LEGAL_ENTITY`,
`LEGAL_MEDIATOR`, `LEGAL_VAT_NUMBER`, `LEGAL_PUBLICATION_DIRECTOR` et `LEGAL_PHONE`, mais dans
`backend/legal.py` : une variable vide ne peut plus effacer une mention obligatoire. Après la
fusion, ces variables ne servent plus ; vous pourrez les supprimer sur Render (sans urgence).
`LEGAL_BACKUP_LOCATION` reste utilisée : si vous la modifiez, prévenez-moi pour que sa traduction
anglaise suive.

## Points en attente

Voir `LEGAL-VALIDATION-REQUIRED.md`. Hors du périmètre juridique, quelques messages de
l'application citent encore « Comptes & connexions » (ancien nom de « Mon compte ») ; ils n'ont
pas été modifiés.
