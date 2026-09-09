// FAMILLE VISEE : https_page_links_to_http_image + https_http_mixed_content
// image chargee en http sur une page https.
export const metadata = {
  title: 'Page de test du parcours d\'obstacles Noyaru',
  description: 'Page du parcours d\'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/mixed-image',
  },
  openGraph: {
    type: 'article',
    title: 'Page de test du parcours d\'obstacles Noyaru',
    description: 'Page du parcours d\'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/mixed-image',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Page de test du parcours d\'obstacles Noyaru',
    description: 'Page du parcours d\'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles</h1>
      <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <img src="http://noyaru-stack-next-app.netlify.app/og.png" alt="Illustration de test" />
    </main>
  );
}
