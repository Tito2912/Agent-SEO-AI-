// FAMILLE VISEE : open_graph_tags_incomplete + twitter_card_incomplete
// og:title seul, sans description ni image ni url.
export const metadata = {
  title: 'Parcours Noyaru : og incomplete',
  description: 'Page « og incomplete » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/og-incomplete',
  },
  openGraph: { title: 'Open Graph incomplet' },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : og incomplete',
    description: 'Page « og incomplete » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : og incomplete</h1>
      <p>Cette page du parcours traite le cas « og incomplete ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
