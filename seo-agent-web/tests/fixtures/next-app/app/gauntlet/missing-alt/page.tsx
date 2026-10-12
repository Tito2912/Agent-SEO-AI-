// FAMILLE VISEE : missing_alt_text
// image sans attribut alt.
export const metadata = {
  title: 'Parcours Noyaru : missing alt',
  description: 'Page « missing alt » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/missing-alt',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : missing alt',
    description: 'Page « missing alt » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/missing-alt',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : missing alt',
    description: 'Page « missing alt » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : missing alt</h1>
      <p>Cette page du parcours traite le cas « missing alt ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <img src="/og.png" />
    </main>
  );
}
