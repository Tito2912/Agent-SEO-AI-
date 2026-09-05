export const metadata = {
  title: "Accueil du site de test Next App Router",
  description: "Site fixture Next.js App Router : les valeurs de tete sont un export metadata, la cinquieme facon d'ecrire la meme chose parmi les neuf stacks.",
  alternates: {
    canonical: "http://127.0.0.1:8749/",
    // DEFAUT INJECTE (famille served_html_lang_mismatch) : le hreflang auto-referent dit
    // anglais, le layout racine sert lang="fr". Sur un export statique sans segment de
    // langue, ce layout ne peut pas connaitre la route : c est le seul stack des neuf ou
    // la correction ne peut pas etre faite a la source.
    languages: {
      en: "http://127.0.0.1:8749/",
      fr: "http://127.0.0.1:8749/a-propos",
      "x-default": "http://127.0.0.1:8749/",
    },
  },
  openGraph: {
    type: "website",
    title: "Accueil du site de test Next App Router",
    description: "Site fixture Next.js App Router.",
    url: "http://127.0.0.1:8749/",
    images: ["http://127.0.0.1:8749/og.png"],
  },
};

export default function Page() {
  return <main><h1>Accueil</h1><p>Site de test Next App Router.</p></main>;
}
