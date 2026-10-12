// FAMILLE VISEE : page_has_links_to_redirect
// lien vers une URL qui redirige (declaree cote hote).
export const metadata = {
  title: 'Parcours Noyaru : link to redirect',
  description: 'Page « link to redirect » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/link-to-redirect',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : link to redirect',
    description: 'Page « link to redirect » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/link-to-redirect',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : link to redirect',
    description: 'Page « link to redirect » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : link to redirect</h1>
      <p>Cette page du parcours traite le cas « link to redirect ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <p><a href="/gauntlet/ancienne-page">Ancienne page</a></p>
    </main>
  );
}
