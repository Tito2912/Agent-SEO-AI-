import Head from 'next/head';

// FAMILLE VISEE : meta_description_too_short_indexable
// aucune meta description sur une page indexable.

export default function Page() {
  return (
    <main>
      <Head>
        <meta name="viewport" content="width=device-width" />
        <title>Page de test du parcours d'obstacles Noyaru</title>
        <link rel="canonical" href="https://noyaru-stack-next-pages.netlify.app/gauntlet/missing-meta-description" />
        <meta property="og:type" content="article" />
        <meta property="og:title" content="Page de test du parcours d'obstacles Noyaru" />
        <meta property="og:url" content="https://noyaru-stack-next-pages.netlify.app/gauntlet/missing-meta-description" />
        <meta property="og:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content="Page de test du parcours d'obstacles Noyaru" />
        <meta name="twitter:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
      </Head>
      <h1>Parcours d'obstacles</h1>
      <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
