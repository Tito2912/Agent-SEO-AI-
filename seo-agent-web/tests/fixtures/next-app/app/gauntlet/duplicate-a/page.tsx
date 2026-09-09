// FAMILLE VISEE : duplicate_titles + duplicate_meta_descriptions
// jumelle de duplicate-b : meme titre ET meme description.
export const metadata = {
  title: 'Deux pages qui portent exactement le meme titre pour le test',
  description: 'Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/duplicate-a',
  },
  openGraph: {
    type: 'article',
    title: 'Deux pages qui portent exactement le meme titre pour le test',
    description: 'Deux pages qui portent exactement la meme meta description, afin de declencher la famille des doublons.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/duplicate-a',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Deux pages qui portent exactement le meme titre pour le test',
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
