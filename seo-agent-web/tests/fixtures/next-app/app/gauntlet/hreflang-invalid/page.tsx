// FAMILLE VISEE : hreflang_annotation_invalid
// code de langue mal forme (soulignement au lieu du tiret).
export const metadata = {
  title: 'Parcours Noyaru : hreflang invalid',
  description: 'Page « hreflang invalid » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/hreflang-invalid',
    languages: { 'fr_FR': 'https://noyaru-stack-next-app.netlify.app/gauntlet/hreflang-invalid' },
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : hreflang invalid',
    description: 'Page « hreflang invalid » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/hreflang-invalid',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : hreflang invalid',
    description: 'Page « hreflang invalid » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : hreflang invalid</h1>
      <p>Cette page du parcours traite le cas « hreflang invalid ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
