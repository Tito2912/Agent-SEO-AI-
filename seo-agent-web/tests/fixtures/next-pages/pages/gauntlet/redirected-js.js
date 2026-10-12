import Head from 'next/head';

// FAMILLE VISEE : javascript_redirects + page_has_redirected_javascript
// script charge depuis une URL qui redirige.

export default function Page() {
  return (
    <main>
      <Head>
        <meta name="viewport" content="width=device-width" />
        <title>Parcours Noyaru : redirected js</title>
        <meta name="description" content="Page « redirected js » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
        <link rel="canonical" href="https://noyaru-stack-next-pages.netlify.app/gauntlet/redirected-js" />
        <meta property="og:type" content="article" />
        <meta property="og:title" content="Parcours Noyaru : redirected js" />
        <meta property="og:description" content="Page « redirected js » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
        <meta property="og:url" content="https://noyaru-stack-next-pages.netlify.app/gauntlet/redirected-js" />
        <meta property="og:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content="Parcours Noyaru : redirected js" />
        <meta name="twitter:description" content="Page « redirected js » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
        <meta name="twitter:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
      </Head>
      <h1>Parcours d'obstacles : redirected js</h1>
      <p>Cette page du parcours traite le cas « redirected js ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <script src="/ancien.js"></script>
    </main>
  );
}
