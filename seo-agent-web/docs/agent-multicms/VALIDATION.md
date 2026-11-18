# Validation locale — première livraison multi-CMS

État au 18 novembre 2026. Branche `feature/agent-multicms-first-delivery`, base de code
identique à l’arbre `d0c7cd17` de `main` `6a72509f` avant changements.

## Éléments déjà contrôlés

- 353 tests composants : stratégie, mesures, lecture GSC par page, six adaptateurs,
  transport HTTP, validation/transfert des médias et workflow de publication initial.
- 32 tests des parcours HTTP et de la journalisation média : contrôle d’accès,
  CSRF, consentement, refus du double envoi et fenêtres GSC. Depuis C079, Webflow
  publie les seuls éléments choisis et Shopify est entièrement hors parcours client.
  Le complément de 200 tests ci-dessous vérifie ces décisions ainsi que les
  états « période trop récente ».
- 52 tests complémentaires du workflow final, de la migration 0033 et des médias
  journalisés, dont récupération d’un brouillon après échec de relecture et refus
  de remplacer silencieusement une version modifiée dans le CMS.
- 66 tests finaux workflow/parcours : reçu d’accord avant écriture, absence de
  doublon après réponse illisible, version distante concurrente refusée, date du
  reçu d’acceptation conservée à travers une récupération et utilisée pour GSC.
  Une mesure GSC n’est jamais étiquetée comme une publication dans l’historique.
- Ruff ciblé vert ; compilation et contrôle du diff.
- Chromium sur 1440×1100 et 390×844 : plan, résultats, WordPress, Webflow, Wix, PrestaShop
  et relecture. Quatorze pages HTTP 200, aucune erreur JavaScript, aucun débordement
  horizontal ; texte de l’iframe sandbox chargé. Données uniquement fictives.
- Migration additive testée sur base vide et base déjà créée par les modèles,
  montée répétée, descente et remontée ; comptes/projets/articles existants conservés.

Les groupes ci-dessus se recoupent : ne pas additionner ces nombres comme un total
de tests uniques. La barrière complète est consignée séparément ci-dessous.

## Barrière complète du dépôt

Les écarts du premier passage sont corrigés : captures réelles régénérées après
le nouveau menu, traductions ajoutées, parsing XML avec defusedxml et environnement
conforme au verrouillage du dépôt. Les 168 tests en régression repassent (un ignoré),
ainsi que 200 tests ciblés finaux CMS/GitHub/accords/migrations/anglais. Audit des
dépendances : aucune vulnérabilité connue ; Bandit moyen/élevé : aucune alerte.
Un rejeu complet sur copie Linux du candidat `642784f` est en cours ; les derniers
correctifs de reçu/date sont couverts par les 66 tests finaux ci-dessus et feront
également l’objet de la CI sur le commit de PR. Le lanceur `ops/ci_local.py` lit les cibles réelles de
`.github/workflows/ci.yml` : compilation, Ruff F/E9, suite complète, audit et Bandit.
Le journal complet Linux est `/tmp/noyaru-agent-full-ci-linux-20261118.log` ;
les 66 tests finaux sont dans `/tmp/noyaru-agent-final-receipts-20261118.log`.
Les tests HTTP sont exécutés hors limitation de boucle asynchrone du bac à sable,
avec une base et des comptes fictifs, sans branche ni base de production.

## Ce que ces résultats ne prouvent pas

Pas de connexion/relecture/publication réelle dans une instance WordPress, Ghost,
Wix, Webflow, Shopify ou PrestaShop. Pas de déploiement, validation App Store,
activation de permission CMS client, évolution SEO ou conversion commerciale
constatée. Les CGU et la confidentialité publiques n’ont pas changé.

Les captures dans `previews/` ne montrent aucune donnée client. Le site, l’adresse
et le compte de démonstration sont explicitement fictifs.

## Décisions C079 intégrées

Webflow publie uniquement les éléments choisis. Shopify n’a aucun parcours client.
Les accords CMS sont persistés par opération avec acteur, date, cibles et empreinte
HTML avant tout appel d’écriture ; les médias conservent leur accord propre.
La création manuelle de contenu GitHub exige désormais une case et un contrôle
serveur, puis lie l’audit aux pages/PR effectivement produites. Le mandat des modes
planifiés existants reste à distinguer d’une case par déclenchement avant toute
formulation juridique générale ; aucune publication native automatique introduite.
