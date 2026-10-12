// FAMILLE VISEE : image_redirects + page_has_redirected_image
// image chargee depuis une URL qui redirige.
export const metadata = {
  title: 'Parcours Noyaru : redirected image',
  description: 'Page « redirected image » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/redirected-image',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : redirected image',
    description: 'Page « redirected image » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/redirected-image',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : redirected image',
    description: 'Page « redirected image » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : redirected image</h1>
      <p>Cette page du parcours traite le cas « redirected image ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <img src="/img/ancienne.png" alt="Illustration de test" />
    </main>
  );
}
