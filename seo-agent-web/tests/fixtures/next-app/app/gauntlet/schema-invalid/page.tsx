// FAMILLE VISEE : structured_data_google_rich_results_validation_error
// FAQ dont la question n'a pas de reponse.
export const metadata = {
  title: 'Parcours Noyaru : schema invalid',
  description: 'Page « schema invalid » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/schema-invalid',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : schema invalid',
    description: 'Page « schema invalid » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/schema-invalid',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : schema invalid',
    description: 'Page « schema invalid » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : schema invalid</h1>
      <p>Cette page du parcours traite le cas « schema invalid ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: `{"@context":"https://schema.org","@type":"FAQPage","mainEntity":[{"@type":"Question","name":"Une question sans reponse ?"}]}` }} />
    </main>
  );
}
