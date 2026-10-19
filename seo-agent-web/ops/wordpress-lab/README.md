# Laboratoire WordPress / Yoast pour Noyaru

Ce laboratoire démarre un vrai WordPress temporaire avec Yoast grâce à [WordPress Playground](https://developer.wordpress.org/playground/handbook/guides/e2e-testing-with-playwright/). Aucun compte WordPress, domaine, hébergement, Docker, PHP ou serveur MySQL n’est nécessaire.

Il exécute les formulaires Noyaru de connexion, sélection de correction, aperçu, application, vérification et rétablissement sur deux anomalies présentes dans le HTML réel d’une page de démonstration. L’API Yoast et les mots de passe d’application WordPress sont réels. Les tests contrôlent aussi le refus des droits insuffisants, l’absence d’écriture avant validation, l’absence de double envoi, les modifications externes et la déconnexion.

## Exécution

Prérequis : Node.js ≥ 24.18, npm ≥ 11.16 et un Python disposant des dépendances de `seo-agent-web/requirements-dev.txt`.

Depuis ce dossier :

```sh
npm ci --ignore-scripts --no-audit --no-fund
npm test
```

Si le Python du projet n’est pas celui du terminal, sous bash :

```sh
NOYARU_LAB_PYTHON=/chemin/venv/bin/python npm test
```

Sous PowerShell :

```powershell
$env:NOYARU_LAB_PYTHON = "C:\chemin\venv\Scripts\python.exe"
npm test
```

Pour conserver un rapport sans identifiants, définir `NOYARU_LAB_REPORT` avec le chemin du fichier JSON désiré. Le processus supprime le WordPress, les identifiants jetables et la base Noyaru isolée en se terminant normalement ou sur interruption gérée. Un arrêt forcé du système peut laisser le dossier temporaire à nettoyer.

## Périmètre du test

- Playground CLI est fixé à 3.1.58, Yoast à 28.7, PHP à 8.3 ; WordPress utilise la dernière révision de la branche 6.9. Les versions effectives sont inscrites dans le rapport.
- La première exécution télécharge les composants officiels et nécessite Internet. Le PHP de démonstration fonctionne ensuite sans accès réseau externe.
- Le serveur écoute uniquement sur `127.0.0.1`, sur un port temporaire. Une origine fictive `https://wordpress.noyaru.test` sert à produire les permaliens WordPress. L’adaptateur HTTP du **processus de test** dirige uniquement cette origine vers le laboratoire local.
- Le connecteur Noyaru déployé conserve intégralement sa validation DNS, son HTTPS et son refus des adresses privées. Il n’existe pas de réglage de production pour contourner ces protections.
- Yoast n’expose ces fonctions que lorsque ses indexables sont activés. Le WordPress jetable utilise donc son mode interne `production`, tout en restant sur la boucle locale. Il ne doit jamais être copié vers un site client.
- Les rapports d’analyse sont construits à partir du HTML réellement renvoyé par cette page, puis soumis au vérificateur Noyaru existant. Ce laboratoire ne lance pas le crawler complet ni une connexion entre `noyaru.com` et un hébergeur externe. Le transport DNS/TLS est vérifié séparément par les tests habituels du connecteur.

Références : [API Yoast](https://developer.yoast.com/features/yoast-seo-abilities/posts-seo-data/), [Playground CLI](https://developer.wordpress.org/playground/handbook/guides/programmatic-playground-cli/).

## Recette depuis Noyaru en ligne

Un [paquet de démonstration hébergée et son scénario](HOSTED.md) préparent l'essai HTTPS public, avec un Blueprint Render distinct et facultatif. Les contrôles de sa passerelle utilisent `npm run test:gateway` et `npm run test:hosted`. Leur succès local ne déclare pas la liaison entre Noyaru en ligne et l'hébergeur validée.
