// FAMILLE VISEE : canonical_from_https_to_http
// page servie en https, canonical en http.
export const metadata = {
  title: 'Parcours Noyaru : canonical http',
  description: 'Page « canonical http » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'http://noyaru-stack-next-app.netlify.app/gauntlet/canonical-http',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : canonical http',
    description: 'Page « canonical http » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/canonical-http',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : canonical http',
    description: 'Page « canonical http » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : canonical http</h1>
      <p>Cette page du parcours traite le cas « canonical http ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
