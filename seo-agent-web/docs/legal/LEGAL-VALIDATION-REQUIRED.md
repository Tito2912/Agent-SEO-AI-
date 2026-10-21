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
| Fournir un accord de traitement des données (DPA) sur demande | Confidentialité § 1, CGU article 11 | Les pages annoncent qu'il « peut être demandé ». Le DPA (`DPA-fr.md`, `DPA-en.md`) est un brouillon non opérationnel : il faut compléter ses passages « À COMPLÉTER » et choisir son mode d'acceptation (signature, acceptation en ligne) avant de publier ces pages. |
| Décrire les garanties de transfert sur demande | Confidentialité § 5 | Suppose d'avoir les justificatifs du point 2. |

## 4. Décisions juridiques à faire valider

| Clause | Document | Question |
| --- | --- | --- |
| Clause de langue (français version de référence) | CGU article 14 | Proposition contractuelle de l'audit : son opposabilité n'est pas garantie dans tous les pays. |
| Réserve « faute lourde ou dol » du plafond de responsabilité des professionnels | CGU article 10 | Rédaction à valider. |
| Compétence des tribunaux de Paris pour les professionnels | CGU article 12 | Conservée, et désormais limitée aux cas où elle est valable et opposable. |
| Rôle de sous-traitant d'E-Com Shop et services du client (GitHub, WordPress, Search Console…) | Confidentialité § 1, DPA annexe 3 | Le brouillon traite ces services comme ceux du client, pas comme des sous-traitants ultérieurs. |
| Cookie de langue `noyaru_lang` (1 an) | Confidentialité § 8 | Présenté comme un cookie de préférence choisie, sans consentement. À confirmer. |
| DPA dans son ensemble | `DPA-fr.md`, `DPA-en.md` | Brouillon à faire valider, puis à compléter. |
