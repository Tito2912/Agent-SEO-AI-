// FAMILLE VISEE : duplicate_pages_without_canonical
// jumelle de no-canonical-b, aucune des deux ne declare de canonical.
export const metadata = {
  title: 'Deux pages jumelles sans canonical declare',
  description: 'Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons.',
  openGraph: {
    type: 'article',
    title: 'Deux pages jumelles sans canonical declare',
    description: 'Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/no-canonical-a',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Deux pages jumelles sans canonical declare',
    description: 'Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons.',
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
