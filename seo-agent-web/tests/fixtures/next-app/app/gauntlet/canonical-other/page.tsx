// FAMILLE VISEE : non_canonical_page_specified_as_canonical_one
// canonical vers une page qui porte elle-meme un autre canonical.
export const metadata = {
  title: 'Parcours Noyaru : canonical other',
  description: 'Page « canonical other » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/canonical-relay',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : canonical other',
    description: 'Page « canonical other » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/canonical-other',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : canonical other',
    description: 'Page « canonical other » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : canonical other</h1>
      <p>Cette page du parcours traite le cas « canonical other ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
