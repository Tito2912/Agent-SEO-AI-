// FAMILLE VISEE : structured_data_schema_org_validation_error
// objet schema.org sans @type declare.
export const metadata = {
  title: 'Parcours Noyaru : schema no type',
  description: 'Page « schema no type » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
  alternates: {
    canonical: 'https://noyaru-stack-next-app.netlify.app/gauntlet/schema-no-type',
  },
  openGraph: {
    type: 'article',
    title: 'Parcours Noyaru : schema no type',
    description: 'Page « schema no type » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    url: 'https://noyaru-stack-next-app.netlify.app/gauntlet/schema-no-type',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Parcours Noyaru : schema no type',
    description: 'Page « schema no type » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.',
    images: ['https://noyaru-stack-next-app.netlify.app/og.png'],
  },
};

export default function Page() {
  return (
    <main>
      <h1>Parcours d'obstacles : schema no type</h1>
      <p>Cette page du parcours traite le cas « schema no type ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
      <p><a href="/">Retour a l accueil</a></p>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: `{"@context":"https://schema.org","name":"Objet sans type declare"}` }} />
    </main>
  );
}
