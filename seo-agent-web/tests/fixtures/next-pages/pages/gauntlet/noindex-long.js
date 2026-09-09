import Head from 'next/head';

// FAMILLE VISEE : title_too_long_not_indexable + meta_description_too_long_not_indexable
// page noindex portant un titre et une description hors plafond.

export default function Page() {
  return (
    <main>
      <Head>
        <meta name="viewport" content="width=device-width" />
        <title>Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, ecrit pour depasser nettement le plafond</title>
        <meta name="description" content="Une meta description volontairement tres longue, ecrite pour depasser le plafond de cent soixante caracteres retenu par le crawler, afin de declencher la famille des descriptions trop longues sur une page non indexable." />
        <meta name="robots" content="noindex, follow" />
        <link rel="canonical" href="https://noyaru-stack-next-pages.netlify.app/gauntlet/noindex-long" />
        <meta property="og:type" content="article" />
        <meta property="og:title" content="Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, ecrit pour depasser nettement le plafond" />
        <meta property="og:description" content="Une meta description volontairement tres longue, ecrite pour depasser le plafond de cent soixante caracteres retenu par le crawler, afin de declencher la famille des descriptions trop longues sur une page non indexable." />
        <meta property="og:url" content="https://noyaru-stack-next-pages.netlify.app/gauntlet/noindex-long" />
        <meta property="og:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content="Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, ecrit pour depasser nettement le plafond" />
        <meta name="twitter:description" content="Une meta description volontairement tres longue, ecrite pour depasser le plafond de cent soixante caracteres retenu par le crawler, afin de declencher la famille des descriptions trop longues sur une page non indexable." />
        <meta name="twitter:image" content="https://noyaru-stack-next-pages.netlify.app/og.png" />
      </Head>
      <h1>Parcours d'obstacles</h1>
      <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
