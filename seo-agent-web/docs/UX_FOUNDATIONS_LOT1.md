# Lot 1 — Accès mobile et repères de navigation

Premier lot issu de [l’audit UX, PR #22](https://github.com/Tito2912/Agent-SEO-AI-/pull/22), préparé sur main `41ad1b7`, incluant le nouveau guide WordPress. La refonte des parcours et les autres lots restent décrits dans l’audit ; ce changement traite les défauts communs confirmés au navigateur.

## Changements

- Les actions de récupération de compte passent à la ligne et la carte d’authentification peut rétrécir : les liens de réinitialisation et de confirmation ne débordent plus à 360/390 px.
- Les noms de site, adresses email et chaînes longues se replient dans le contenu. L’automatisation projet utilise une seule colonne sur téléphone ; la sauvegarde des réglages laisse le bouton Annuler passer à la ligne.
- Le menu indique **Corrections** pendant la correction guidée, la préparation WordPress, l’aperçu et le lot. La configuration de la plateforme garde son entrée CMS propre. Chaque entrée active dispose d’un `aria-current` rendu côté serveur.
- Les liens de consultation Bilan/Anomalies conservent `crawl` et `compare`, encodés. Le suivi des corrections reste courant : ces paramètres ne sont pas transmis à une action d’écriture.
- Un lien d’évitement mène au contenu au clavier. Le champ de l’assistant, la déconnexion et les filtres des corrections reçoivent des noms explicites ; les deux sélecteurs de période des graphiques ont un label relié.
- L’historique reçoit le nom réel du site depuis la résolution autorisée existante, emploie « analyses » et propose de retrouver le site quand aucun bilan n’existe.
- Les sections « Votre compte » et « Administration Noyaru » donnent des repères de portée. Les contrôles d’accès existants restent responsables de ce qui est affiché.

Le calcul des scores, les quotas, les données des rapports, les états de correction et les confirmations avant modification restent inchangés. La seule modification backend ajoute le nom et l’adresse du projet déjà autorisé au contexte de l’historique. Les références versionnées à la feuille de style commune sont actualisées pour éviter un ancien rendu en cache.

## Validation

[Mesures navigateur](ux-foundations/evidence.json) : **204 affichages**, soit 51 scénarios locaux à 1440, 768, 390 et 360 px, y compris le guide WordPress ajouté sur main. Aucun débordement horizontal de page, aucun 500 et aucune erreur JavaScript dans cette matrice. Les tableaux qui défilent localement ne sont pas assimilés à un débordement de page.

Contrôles supplémentaires au navigateur : premier Tab vers le lien d’évitement, déplacement du focus au contenu, un seul repère actif pendant les étapes de correction, navigation vers un bilan conservant l’analyse/comparaison, retour au suivi courant, filtres nommés, nom du site dans l’historique. Menu mobile et fenêtre d’ajout au clavier, parcours complet application/contrôle/résultat/correction suivante/rétablissement revérifiés avec une **API WordPress simulée**. Aucun site client modifié, aucune PR de contenu fusionnée.

**139 tests existants passent** après modification, sur une nouvelle base temporaire : premier audit, première correction vérifiée, CMS, comptage et droits, navigation mobile, bilan vide et connexions de recherche. Même sélection que celle documentée dans l’audit. Un avertissement de dépréciation Starlette/httpx préexistant.

Les 69 templates sont analysés par Jinja ; compilation Python et Ruff F/E9 sur les cibles complètes de la CI passent. Bandit sur backend et scripts ne relève aucun problème moyen/élevé ; avertissements `nosec` préexistants. `git diff --check` passe. La CI GitHub complète est distincte de ces contrôles locaux ; son résultat est indiqué dans la PR lorsqu’il est disponible.

Les écrans publics ont été examinés en production pendant l’audit, avant ces modifications. Ce lot n’a pas été déployé ; les écrans authentifiés et les connexions réelles de production ne sont pas revendiqués comme vérifiés. Recette lecteur d’écran et tests avec des utilisateurs débutants restent à effectuer.

## Captures après modification

| Récupération de compte à 390 px | Correction guidée |
| --- | --- |
| [Actions adaptées au téléphone](ux-foundations/reset-390.png) | [Repère Corrections conservé](ux-foundations/first-correction-1440.png) |

[Adresse longue dans l’équipe](ux-foundations/team-390.png). Les captures avant sont dans la PR d’audit, avec les mesures de débordement d’origine.
