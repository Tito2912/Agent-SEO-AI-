import Head from 'next/head';

// FAMILLE VISEE : duplicate_pages_without_canonical
// jumelle de no-canonical-a.

export default function Page() {
  return (
    <main>
      <Head>
        <meta name="viewport" content="width=device-width" />
        <title>Deux pages jumelles sans canonical declare</title>
        <meta name="description" content="Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons." />
        <meta property="og:type" content="article" />
        <meta property="og:title" content="Deux pages jumelles sans canonical declare" />
        <meta property="og:description" content="Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons." />
        <meta property="og:url" content="https://noyaru-stack-next-pages.netlify.app/gauntlet/no-canonical-b" />
        <meta property="og:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content="Deux pages jumelles sans canonical declare" />
        <meta name="twitter:description" content="Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons." />
        <meta name="twitter:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
      </Head>
      <h1>Parcours d'obstacles</h1>
      <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
