import Head from 'next/head';

// FAMILLE VISEE : missing_h1
// aucun <h1> dans le corps.

export default function Page() {
  return (
    <main>
      <Head>
        <meta name="viewport" content="width=device-width" />
        <title>Parcours Noyaru : missing h1</title>
        <meta name="description" content="Page « missing h1 » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
        <link rel="canonical" href="https://noyaru-stack-next-pages.netlify.app/gauntlet/missing-h1" />
        <meta property="og:type" content="article" />
        <meta property="og:title" content="Parcours Noyaru : missing h1" />
        <meta property="og:description" content="Page « missing h1 » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
        <meta property="og:url" content="https://noyaru-stack-next-pages.netlify.app/gauntlet/missing-h1" />
        <meta property="og:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content="Parcours Noyaru : missing h1" />
        <meta name="twitter:description" content="Page « missing h1 » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
        <meta name="twitter:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
      </Head>
      <p>Cette page du parcours traite le cas « missing h1 ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
