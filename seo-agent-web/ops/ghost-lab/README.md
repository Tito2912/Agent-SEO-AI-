# Recette éditoriale native Ghost

Cette recette C082 utilise un **vrai Ghost 6.73.0 jetable**, sans compte externe.
Elle crée sa propre instance SQLite et son compte fictif, puis vérifie le transfert
WebP consenti, le brouillon, la publication séparée, le HTML local et une édition
concurrente rapide. Chaque accord est contrôlé dans la base Noyaru isolée. L'instance
et ses données sont supprimées à la fin ; aucun hôte de production n'est accepté.

Préparer hors dépôt l'archive officielle `ghost-6.73.0.tgz` de la release
[Ghost 6.73.0](https://github.com/TryGhost/Ghost/releases/tag/v6.73.0), vérifier son
SHA-256 indiqué par l'asset GitHub, puis l'extraire. Installer les dépendances avec
le `packageManager` et le `pnpm-lock.yaml` de cette archive, par exemple
`corepack pnpm install --prod --frozen-lockfile`. Node 22.23.5 a été utilisé ici ;
les dépendances natives doivent correspondre à ce Node. Ne pas installer le paquet
npm `ghost` seul : l'archive contient aussi ses composants locaux.

Depuis `seo-agent-web`, avec les dépendances Python du projet :

```sh
python ops/ghost-lab/native_publication.py --ghost-root /chemin/ghost-extrait --node /chemin/node --report /tmp/ghost-native-report.json
```

Le script est prévu pour Linux/WSL (liens symboliques). Il démarre exclusivement
sur `127.0.0.1`, utilise un port disponible, désactive les contrôles de mise à jour
et dirige le courrier vers un port local fermé. Seule la frontière DNS/TLS et
l'origine locale dans les réponses sont adaptées dans le processus de recette ;
le transport Noyaru et le workflow restent réels. Ce n'est **pas** une recette
HTTPS chez un hébergeur ni une preuve de délivrabilité ou de résultat SEO.

Un échec peut conserver `/tmp/noyaru-ghost-native-diagnostic.log` en mode 0600 ;
ce diagnostic privé n'entre ni dans le rapport, ni dans Git et doit être supprimé
après diagnostic. Le rapport JSON ne contient aucun identifiant de connexion.
