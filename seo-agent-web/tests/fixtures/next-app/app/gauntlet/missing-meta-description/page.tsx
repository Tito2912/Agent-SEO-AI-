// FAMILLE VISEE : meta_description_too_short_indexable
// aucune meta description sur une page indexable.
export const metadata = {
  title: 'Page de test du parcours d\'obstacles Noyaru',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/missing-meta-description',
  },
  openGraph: {
    type: 'article',
    title: 'Page de test du parcours d\'obstacles Noyaru',
    description: '',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/missing-meta-description',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Page de test du parcours d\'obstacles Noyaru',
    description: '',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles</h1>
      <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
    </main>
  );
}
