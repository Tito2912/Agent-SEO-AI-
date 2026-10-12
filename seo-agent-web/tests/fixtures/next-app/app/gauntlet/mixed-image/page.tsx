// FAMILLE VISEE : https_page_links_to_http_image + https_http_mixed_content
// image chargee en http sur une page https.
export const metadata = {
  title: 'Parcours Noyaru : mixed image',
  description: 'Page « mixed image » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/mixed-image',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : mixed image',
    description: 'Page « mixed image » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/mixed-image',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : mixed image',
    description: 'Page « mixed image » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : mixed image</h1>
      <p>Cette page du parcours traite le cas « mixed image ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <img src="http://noyaru-stack-next-app.netlify.app/og.png" alt="Illustration de test" />
    </main>
  );
}
