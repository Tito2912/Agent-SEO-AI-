// FAMILLE VISEE : https_page_has_internal_links_to_http
// lien interne ecrit en http.
export const metadata = {
  title: 'Parcours Noyaru : link http',
  description: 'Page « link http » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/link-http',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : link http',
    description: 'Page « link http » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/link-http',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : link http',
    description: 'Page « link http » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : link http</h1>
      <p>Cette page du parcours traite le cas « link http ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <p><a href="http://noyaru-stack-next-app.netlify.app/a-propos">A propos</a></p>
    </main>
  );
}
