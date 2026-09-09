import Head from 'next/head';

// FAMILLE VISEE : duplicate_titles + duplicate_meta_descriptions
// jumelle de duplicate-b : meme titre ET meme description.

export default function Page() {
  return (
    <main>
      <Head>
        <meta name="viewport" content="width=device-width" />
        <title>Deux pages qui portent exactement le meme titre pour le test</title>
        <meta name="description" content="Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons." />
        <link rel="canonical" href="https://noyaru-stack-next-pages.netlify.app/gauntlet/duplicate-a" />
        <meta property="og:type" content="article" />
        <meta property="og:title" content="Deux pages qui portent exactement le meme titre pour le test" />
        <meta property="og:description" content="Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons." />
        <meta property="og:url" content="https://noyaru-stack-next-pages.netlify.app/gauntlet/duplicate-a" />
        <meta property="og:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content="Deux pages qui portent exactement le meme titre pour le test" />
        <meta name="twitter:description" content="Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons." />
        <meta name="twitter:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
      </Head>
      <h1>Parcours d'obstacles</h1>
      <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
