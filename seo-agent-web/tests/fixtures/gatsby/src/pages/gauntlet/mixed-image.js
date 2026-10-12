import * as React from 'react';

// FAMILLE VISEE : https_page_links_to_http_image + https_http_mixed_content
// image chargee en http sur une page https.

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : mixed image</h1>
      <p>Cette page du parcours traite le cas « mixed image ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <img src="http://noyaru-stack-gatsby.netlify.app/og.png" alt="Illustration de test" />
    </main>
  );
}

// Gatsby Head API — l'idiome de cette stack. `next/head` n'existe pas ici.
export function Head() {
  return (
    <>
      <meta name="viewport" content="width=device-width" />
      <title>Parcours Noyaru : mixed image</title>
      <meta name="description" content="Page « mixed image » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
      <link rel="canonical" href="https://noyaru-stack-gatsby.netlify.app/gauntlet/mixed-image" />
      <meta property="og:type" content="article" />
      <meta property="og:title" content="Parcours Noyaru : mixed image" />
      <meta property="og:description" content="Page « mixed image » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
      <meta property="og:url" content="https://noyaru-stack-gatsby.netlify.app/gauntlet/mixed-image" />
      <meta property="og:image" content="https://noyaru-stack-gatsby.netlify.app/og.png" />
      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:title" content="Parcours Noyaru : mixed image" />
      <meta name="twitter:description" content="Page « mixed image » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
      <meta name="twitter:image" content="https://noyaru-stack-gatsby.netlify.app/og.png" />
    </>
  );
}
