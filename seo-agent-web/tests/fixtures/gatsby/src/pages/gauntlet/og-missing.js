import * as React from 'react';

// FAMILLE VISEE : open_graph_tags_missing
// aucune balise Open Graph.

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles</h1>
      <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}

// Gatsby Head API — l'idiome de cette stack. `next/head` n'existe pas ici.
export function Head() {
  return (
    <>
      <meta name="viewport" content="width=device-width" />
      <title>Page de test du parcours d'obstacles Noyaru</title>
      <meta name="description" content="Page du parcours d'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl." />
      <link rel="canonical" href="https://noyaru-stack-gatsby.netlify.app/gauntlet/og-missing" />
      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:title" content="Page de test du parcours d'obstacles Noyaru" />
      <meta name="twitter:description" content="Page du parcours d'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl." />
      <meta name="twitter:image" content="https://noyaru-stack-gatsby.netlify.app/og.png" />
    </>
  );
}
