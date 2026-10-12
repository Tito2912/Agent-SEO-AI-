// FAMILLE VISEE : missing_h1
// aucun <h1> dans le corps.
export const metadata = {
  title: 'Parcours Noyaru : missing h1',
  description: 'Page « missing h1 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/missing-h1',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : missing h1',
    description: 'Page « missing h1 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/missing-h1',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : missing h1',
    description: 'Page « missing h1 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <p>Cette page du parcours traite le cas « missing h1 ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
