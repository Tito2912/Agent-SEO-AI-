import Head from 'next/head';

// FAMILLE VISEE : twitter_card_missing
// aucune balise twitter.

export default function Page() {
  return (
    <main>
      <Head>
        <meta name="viewport" content="width=device-width" />
        <title>Page de test du parcours d'obstacles Noyaru</title>
        <meta name="description" content="Page du parcours d'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl." />
        <link rel="canonical" href="https://noyaru-stack-next-pages.netlify.app/gauntlet/twitter-missing" />
        <meta property="og:type" content="article" />
        <meta property="og:title" content="Page de test du parcours d'obstacles Noyaru" />
        <meta property="og:description" content="Page du parcours d'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl." />
        <meta property="og:url" content="https://noyaru-stack-next-pages.netlify.app/gauntlet/twitter-missing" />
        <meta property="og:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
      </Head>
      <h1>Parcours d'obstacles</h1>
      <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
