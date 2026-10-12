// FAMILLE VISEE : https_page_links_to_http_javascript
// script charge en http.
export const metadata = {
  title: 'Parcours Noyaru : mixed js',
  description: 'Page « mixed js » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/mixed-js',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : mixed js',
    description: 'Page « mixed js » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/mixed-js',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : mixed js',
    description: 'Page « mixed js » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : mixed js</h1>
      <p>Cette page du parcours traite le cas « mixed js ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <script src="http://noyaru-stack-next-app.netlify.app/app.js"></script>
    </main>
  );
}
