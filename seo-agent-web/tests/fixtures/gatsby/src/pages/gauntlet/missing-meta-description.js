import * as React from 'react';

// FAMILLE VISEE : missing_meta_description
// aucune meta description.

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
      <link rel="canonical" href="https://noyaru-stack-gatsby.netlify.app/gauntlet/missing-meta-description" />
      <meta property="og:type" content="article" />
      <meta property="og:title" content="Page de test du parcours d'obstacles Noyaru" />
      <meta property="og:description" content="" />
      <meta property="og:url" content="https://noyaru-stack-gatsby.netlify.app/gauntlet/missing-meta-description" />
      <meta property="og:image" content="https://noyaru-stack-gatsby.netlify.app/og.png" />
      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:title" content="Page de test du parcours d'obstacles Noyaru" />
      <meta name="twitter:description" content="" />
      <meta name="twitter:image" content="https://noyaru-stack-gatsby.netlify.app/og.png" />
    </>
  );
}
