// FAMILLE VISEE : css_redirects + page_has_redirected_css
// feuille de style chargee depuis une URL qui redirige.
export const metadata = {
  title: 'Parcours Noyaru : redirected css',
  description: 'Page « redirected css » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/redirected-css',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : redirected css',
    description: 'Page « redirected css » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/redirected-css',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : redirected css',
    description: 'Page « redirected css » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : redirected css</h1>
      <p>Cette page du parcours traite le cas « redirected css ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <link rel="stylesheet" href="/ancienne.css" />
    </main>
  );
}
