# Reprise du 2 novembre 2026 — état vérifié avant continuation

Dernier point de cette refonte : PR #57 fusionnée le 24 octobre, `main` à `bffc17093b5c3f0099a0512be56a526d552c9901`. Les références distantes et les PR ont été relues avant toute reprise du correctif. Les checkouts utilisés étaient propres ; aucune branche de Claude ou de l’utilisateur n’a été réinitialisée ou changée.

## Ce qui a été fait depuis le dernier point

| Périmètre | Constat vérifié le 2 novembre | Conséquence |
| --- | --- | --- |
| `main` | Toujours `bffc170`, aucun commit supplémentaire après la fusion de #57. | Conserver le travail existant ; aucune nouvelle refonte publiée à réappliquer. |
| Lot 23, PR #57 | Fusionné. La feuille `issue-context.css` publique est identique à celle de `main`. | Le lien guidé qui conserve la page et l’analyse fait partie de la version publiée observée. |
| Lot 22, PR #56 | Toujours ouvert au commit `d81d64b`. L’ancienne CI est réussie ; le commentaire de revue affiche encore `running` depuis le 24 octobre, sans verdict. Le script public correspond à `main`, pas à cette PR. | Le correctif de préparation reste à intégrer et à revalider. Le temps écoulé ne vaut pas validation de la revue. |
| PR #58 de Claude | Correction du capital dans les pages juridiques, toujours ouverte ; CI réussie sur `dd5025b`. | Périmètre réservé à Claude : aucun contenu juridique ni branche associé modifié ou fusionné ici. |
| Consignes | `RUNBOOK.md`, `docs/I18N.md` et la CI de `main` relus ; aucun `AGENTS.md` ou `CLAUDE.md` dans l’arbre suivi. | Utiliser le catalogue existant, conserver les droits et reçus, tester hors des données de production. |

Ces constats concernent l’historique et les références publiés. Ils ne permettent pas d’exclure du travail non publié dans un autre checkout de Claude.

## État réellement observé dans les navigateurs

- **Production anonyme : 31 contrôles.** Accueil, tarifs, aide, statut, documentation, connexion et inscription en FR/EN, à 1 440 et 390 px ; deux écrans privés redirigent vers la connexion et l’API protégée répond 401. Langues exactes, aucun débordement ou erreur JavaScript relevé. Aucun formulaire envoyé. Les deux ressources comparées correspondent à `main` ; le SHA complet du serveur n’est pas exposé.
- **Compte témoin authentifié :** le propriétaire s’est reconnecté dans une nouvelle fenêtre temporaire, l’ancienne étant fermée. Identité attendue confirmée sous forme booléenne, même page sélectionnée, suivi « à faire », aucune proposition enregistrée ni vérification réussie pour cette cible. Configuration GitHub enregistrée identique, mode `review`, forfait Free et quota de corrections **0/2**. Cela atteste la configuration lue, pas un renouvellement OAuth ou une écriture GitHub.
- **Page publique témoin :** HTTP 200, langue française, une seule méta-description, toujours **172 caractères** après normalisation. Aucun nouvel audit SEO ou appel IA déclenché pour ce constat.

Les identifiants, e-mail, nom du dépôt client, fichier source privé, cookies et réponses brutes de compte ne sont pas inclus dans les preuves versionnées.

## Points nouveaux et reprise retenue

1. **Revalider #56 avec le lot 23 présent.** Intégration de `main` dans notre seule branche, sans conflit. Les modifications de comportement du lot 22 sont conservées ; son ancien état de revue n’est pas présenté comme un accord.
2. **Actualiser une dépendance signalée depuis le dernier point.** `pyasn1` 0.6.4 est désormais signalé par l’audit ; la version 0.6.5, publiée le 27 octobre, est annoncée corrigée par l’éditeur. Passage minimal à 0.6.5, compatible avec la contrainte `pyasn1>=0.6.1,<0.7.0` de `pyasn1-modules`. [Avis de l’éditeur](https://github.com/pyasn1/pyasn1/security/advisories/GHSA-fm9c-5rcf-2q5p), [version publiée](https://pypi.org/project/pyasn1/0.6.5/).
3. **Aligner l’environnement de validation.** Le `pip` fourni par le nouveau venv était ancien ; il a été actualisé comme dans la CI, sans changement du projet à ce titre. Le premier passage complet a ensuite révélé un chemin de bibliothèque manquant pour Chromium local (`libnspr4.so`), après 3 819 cas réussis. La bibliothèque était déjà disponible dans le dossier temporaire ; son chemin est transmis au passage final. Aucune modification de l’application ou du test n’a été faite pour masquer cet échec d’environnement.
4. **Reprendre l’aperçu après validation et publication du correctif.** La même page reste pertinente et aucun aperçu réussi n’est enregistré. Une proposition ou une modification effective du site témoin devra être attestée distinctement ; la navigation et les tests simulés ne valent pas application d’une correction.

## Validations de reprise

Les preuves publiques et locales sont dans `ux-reprise-20261102/`. Les résultats finaux des tests, de la CI et de la revue seront consignés avant la fusion de la PR. L’audit des dépendances après mise à jour ne signale plus de vulnérabilité connue ; `pip check` ne signale aucune incompatibilité.

Aucun déploiement manuel, changement de formule, réinitialisation de reçu, nouvelle analyse réelle ou écriture sur le dépôt du site témoin ne fait partie de cette vérification préalable.
