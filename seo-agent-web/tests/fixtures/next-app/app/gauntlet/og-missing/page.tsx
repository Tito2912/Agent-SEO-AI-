// FAMILLE VISEE : open_graph_tags_missing
// aucune balise Open Graph.
export const metadata = {
  title: 'Parcours Noyaru : og missing',
  description: 'Page « og missing » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/og-missing',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : og missing',
    description: 'Page « og missing » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : og missing</h1>
      <p>Cette page du parcours traite le cas « og missing ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
