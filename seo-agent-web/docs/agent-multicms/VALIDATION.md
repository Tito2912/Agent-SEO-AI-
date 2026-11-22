# Validation locale — première livraison multi-CMS

État au 19 novembre 2026. Branche `feature/agent-multicms-first-delivery`, base de code
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
Le passage complet Linux du candidat `642784f` a obtenu 10 428 tests passés,
45 ignorés et deux échecs dans les anciens tests de taille/ton GitHub : ils
n’envoyaient pas le nouveau consentement obligatoire. Compilation, Ruff, audit
des dépendances et Bandit sont verts sur cette passe. La première CI GitHub
(`3a7fbced`) retrouve ces deux cas : 10 434 passés et 46 ignorés.
Le helper de ces tests envoie désormais explicitement l’accord pour une demande
consentie ; les assertions taille/ton/quota et le refus sans accord sont conservés.
Les 30 tests taille/ton et parcours complets passent après cette adaptation.
Le résultat de la **CI finale sur le dernier commit** est consultable dans
[la PR nº 95](https://github.com/Tito2912/Agent-SEO-AI-/pull/95/checks) ; ces nombres
historiques ne constituent pas une déclaration de réussite du dernier commit.

Le lanceur `ops/ci_local.py` lit les cibles réelles de `.github/workflows/ci.yml` :
compilation, Ruff F/E9, suite complète, audit et Bandit. Journal complet Linux :
`/tmp/noyaru-agent-full-ci-linux-20261118.log`. Les 66 tests finaux sont dans
`/tmp/noyaru-agent-final-receipts-20261118.log`, les 132 tests transport/workflow/média
et 48 tests de contrat dans leurs journaux `transport-final`/`adapter-final`.
Les tests HTTP sont exécutés hors limitation de boucle asynchrone du bac à sable,
avec une base et des comptes fictifs, sans branche ni base de production.

## Recette WordPress native locale réussie

`ops/wordpress-lab/check.py` exerce les formulaires réels de Noyaru face à
WordPress 6.9.11 temporaire (PHP 8.3, Yoast 28.7 présent pour les anciens parcours).
L’article utilise REST natif, sans requête Yoast : aperçu sans écriture, refus
sans case, accord persisté, brouillon relu, second accord, publication relue et
texte visible dans le HTML public local. Les double envois n’écrivent pas ; une
modification directe dans WordPress bloque la publication du brouillon périmé.
26 requêtes de contenu, quatre écritures au total, dont la modification externe
simulée. Rapport sans identifiants : `WORDPRESS-LOCAL.json`.

Le laboratoire dirige seulement la frontière DNS/TLS du processus de test vers
127.0.0.1 ; le reste du transport natif et les adaptateurs sont ceux de Noyaru.
Les protections de production ne possèdent aucun réglage de contournement.
Le transfert média de ce nouveau parcours et la liaison à un hébergeur HTTPS
externe ne sont pas validés par ce test. Les 132 tests transport/workflow/média
et les tests du contrat adaptateur complètent la recette pour les erreurs simulées.

## Ce que ces résultats ne prouvent pas

Pas de connexion native authentifiée chez un hébergeur externe ni sur Wix/Webflow.
Ghost et PrestaShop ont été éprouvés seulement sur les laboratoires ci-dessous ;
PrestaShop 9 reste à éprouver. Shopify n’est pas exposé. Pas de déploiement, validation App Store,
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

## Portabilité des captures

Un clone configuré avec `core.autocrlf=true` conserve les quatre sources hachées
en LF grâce au `.gitattributes` ciblé. Leurs quatre SHA-256 correspondent exactement
au manifeste ; le garde n’a pas été normalisé ni désactivé.

## C082 — Ghost et PrestaShop réels locaux (19 novembre)

- Ghost 6.73.0 / Node 22.23.5 / SQLite : image WebP native avec accord conservé,
  réutilisation sans second transfert, brouillon puis publication avec deux
  accords distincts, texte dans le HTML local et absence de newsletter attachée.
  Une édition externe de titre a été confirmée au même updated_at ; l’empreinte
  de contenu a bloqué la publication avant un nouveau PUT. 11 requêtes natives,
  quatre écritures. Rapport GHOST-LOCAL-20261119.json, avec empreintes des sources.
- PrestaShop 8.2.8 / PHP 8.1 / MariaDB 10.11, fr/en : création inactive en français,
  anglais modifié sans perte du français, activation sans mutation des textes,
  HTML local 200 et conflit de révision refusé avant PATCH. La création en langue
  non défaut est refusée par cette boutique sans nouvelle langue inventée.
  18 requêtes, cinq écritures. Rapport PRESTASHOP-LOCAL-20261119.json.
- 275 tests ciblés passés (contrats, transport, workflow, accords, médias, parcours
  et anglais), Ruff et Bandit ciblés verts. La CI complète de ce complément est
  à consulter sur le dernier head de la PR #95 ; la CI #806 citée plus haut
  concerne le candidat précédent.

Les scripts native_publication.py des deux laboratoires sont rejouables et leurs
README précisent les dépendances et le périmètre. Le dernier saut DNS/TLS est
adapté uniquement dans le processus de recette : aucune option de contournement
n’est ajoutée à Noyaru. Comptes, bases et pages fictifs ; pas d’accès client, de
clé existante utilisée, de newsletter envoyée ni de coût d’hébergement. Les
rapports ne contiennent aucun identifiant de connexion.

Décisions du 19/11 transmises par Claude : modes automatiques autorisés par une
case unique à l’activation et accords conservés cinq ans. L’intégration au
modèle, à la purge et aux parcours reste un travail distinct de C082 ; aucune
conformité à ces décisions n’est encore revendiquée ici.

## C083 — preuves d’accord conservées cinq ans

Tony confirme le point de départ à la date de chaque accord. Le complément ajoute
les copies minimales, un reçu média durable, les suppressions SET NULL, l’export
isolé et la purge par échéance ; voir CONSERVATION-ACCORDS.md. La migration0034
est éprouvée sur SQLite et PostgreSQL locaux jetables. Le refus de descente en
présence de preuves n’est pas contourné ; seules les preuves arrivées à échéance
sont purgées avant un essai de descente sur table vide.

Les résultats finaux du complément sont consignés dans les checks de la PR #95
et dans les rapports C083 datés de ce dossier. La CI #807 ci-dessus concerne le
candidat natif précédent. Aucun résultat de CI ultérieur n’est présumé ici.
Ce lot ne modifie aucun texte juridique public et ne constitue pas leur validation.
Les mandats automatiques et les essais CMS externes restent distincts.

Résultats C083 locaux : 201 tests ciblés réussis, deux cas PostgreSQL ignorés
dans cette passe puis exécutés séparément avec succès (deux réussis sur
PostgreSQL 16.15 jetable). Ghost final : trois preuves durables contrôlées,
11 requêtes natives et quatre écritures. Rapports CONSERVATION-LOCAL-20261119.json
et GHOST-CONSENTS-LOCAL-20261119.json ; empreintes des sources conservées.
Ruff global, compilation ciblée et Bandit ciblé réussis.


## Complément C084 — autorisations des modes existants

`MODES-AUTOMATIQUES.md` décrit les deux modes, le retrait, les limites et les
accords manuels. Migration 0035 sur SQLite et deux scénarios PostgreSQL 16.15
réel jetable (schéma ancien/modèles déjà créés), montée répétée, refus de descente
avec autorisations et suppression des seules données fonctionnelles éprouvés.
Le service PostgreSQL est arrêté, données et marqueur du laboratoire supprimés.

Première passe : 153 tests passés, deux cas PostgreSQL ignorés puis exécutés à part
avec succès. Passe élargie : 314 tests passés, dont publication/autocorrection et
CMS manuels. Les derniers compléments de signature, déduplication et statut sont
rééprouvés dans la passe finale de **707 tests réussis, deux ignorés** consignée dans `MODES-LOCAL-20261119.json` ; ces
groupes se recoupent. Quatre écrans Chromium réels locaux FR/EN sur 1440×1100
et 390×844 : aucune requête externe, erreur JS ou débordement horizontal.

Ruff, compilation et Bandit moyen/élevé contrôlés. L’ancienne commande `app:app`
rencontre une importation relative préexistante dans db.py ; point d’entrée
éprouvé : backend.app:app. Aucun résultat de Claude n’est attribué à cette revue
(limite d’usage encore active). Les rapports Ghost/PrestaShop/C083 restent leurs
preuves historiques sur les sources hachées d’alors, pas une recette externe du
candidat C084. La CI complète et le pilote WordPress doivent porter sur le dernier
head de la PR 95 ; leur ancien vert n’est pas réutilisé comme nouveau résultat.

## C085 — HTML réel, média WordPress et cases de métadonnées (22 novembre)

La revue Claude du 20/11 est prise en compte : le vrai rendu Markdown est validé
avant transfert, les tableaux alignés et intertitres sont conservés, le H1 ne
double plus celui du thème, et l’identifiant d’un média à URL refusée reste
disponible sans nouvel upload. `WORDPRESS-MEDIA.md` détaille les corrections et
les deux points de récupération encore ouverts. Le correctif de main ea5d794
et les textes publics approuvés de Claude 66c20a6 sont intégrés. Aucune nouvelle
revue Claude Code n’est attribuée à ce complément.

Deux groupes disjoints : **306 tests** de parcours/transport/médias/accords/rendu
et **248 tests** d’adaptateurs/modes/textes publics réussis. Les 35 tests du
correctif Markdown de main font partie du premier groupe. Les anciennes passes
132/707 sont des sous-ensembles ou des sources antérieures, pas un total à ajouter.
Ruff F/E9 global, compilation et Bandit moyen/élevé réussis.

Recette WordPress 6.9.12 / Yoast 28.7 finale : deux occurrences d’une image privée,
un seul transfert, trois accords distincts (média, brouillon, publication),
intertitre/tableau aligné, titre/H1 cohérent et image publique sans authentification
identique en octets et dimensions. 41 requêtes de contenu / sept écritures ;
les 32 requêtes / six écritures des métadonnées sont distinctes. Comptes et pages
fictifs ; runtime et données supprimés par le lanceur.

Douze écrans Chromium : correction individuelle, lot et avertissement planifié,
chacun en français/anglais à 1440×1100 et 390×844. Cases initialement décochées,
texte traduit, HTTP 200, aucun débordement, erreur JS ni requête externe.
Le contrôle utilise un transport de navigation locale sans écriture CMS ni IA.
Rapport avec empreintes : `WORDPRESS-MEDIA-LOCAL-20261122.json`.

La CI complète et le pilote WordPress doivent encore porter sur le dernier head
de la PR 95 ; consulter ses checks pour leur résultat effectif. Les verts C084
ne prouvent pas ce complément. La PR reste en brouillon, sans merge/déploiement.
Les dates des textes publics intégrés seront recalées à leur déploiement réel.
Les recettes HTTPS externes, Wix/Webflow et PrestaShop 9 restent à faire.
