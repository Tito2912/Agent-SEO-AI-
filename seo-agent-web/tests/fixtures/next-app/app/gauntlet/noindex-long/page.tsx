// FAMILLE VISEE : title_too_long_not_indexable + meta_description_too_long_not_indexable
// page noindex portant un titre et une description hors plafond.
export const metadata = {
  title: 'Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, ecrit pour depasser nettement le plafond',
  description: 'Une meta description volontairement tres longue, ecrite pour depasser le plafond de cent soixante caracteres retenu par le crawler, afin de declencher la famille des descriptions trop longues sur une page non indexable.',
  robots: 'noindex, follow',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/noindex-long',
  },
  openGraph: {
    type: 'article',
    title: 'Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, ecrit pour depasser nettement le plafond',
    description: 'Une meta description volontairement tres longue, ecrite pour depasser le plafond de cent soixante caracteres retenu par le crawler, afin de declencher la famille des descriptions trop longues sur une page non indexable.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/noindex-long',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, ecrit pour depasser nettement le plafond',
    description: 'Une meta description volontairement tres longue, ecrite pour depasser le plafond de cent soixante caracteres retenu par le crawler, afin de declencher la famille des descriptions trop longues sur une page non indexable.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles</h1>
      <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
