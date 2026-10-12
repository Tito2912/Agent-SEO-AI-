// FAMILLE VISEE : canonical_points_to_4xx
// canonical vers une page absente, listee au sitemap pour que le crawl constate le 404.
export const metadata = {
  title: 'Parcours Noyaru : canonical 404',
  description: 'Page « canonical 404 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/page-absente',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : canonical 404',
    description: 'Page « canonical 404 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/canonical-404',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : canonical 404',
    description: 'Page « canonical 404 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : canonical 404</h1>
      <p>Cette page du parcours traite le cas « canonical 404 ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
