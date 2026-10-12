import * as React from 'react';

// FAMILLE VISEE : open_graph_tags_missing
// aucune balise Open Graph.

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : og missing</h1>
      <p>Cette page du parcours traite le cas « og missing ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}

// Gatsby Head API — l'idiome de cette stack. `next/head` n'existe pas ici.
export function Head() {
  return (
    <>
      <meta name="viewport" content="width=device-width" />
      <title>Parcours Noyaru : og missing</title>
      <meta name="description" content="Page « og missing » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
      <link rel="canonical" href="https://noyaru-stack-gatsby.netlify.app/gauntlet/og-missing" />
      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:title" content="Parcours Noyaru : og missing" />
      <meta name="twitter:description" content="Page « og missing » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
      <meta name="twitter:image" content="https://noyaru-stack-gatsby.netlify.app/og.png" />
    </>
  );
}
