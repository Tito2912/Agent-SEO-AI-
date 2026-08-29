import Head from 'next/head';

const title = 'Accueil du site de test Next Pages';
const description = "Page d'accueil du site fixture Next.js Pages Router, utilise pour prouver la boucle complete de correction SEO.";
const canonical = 'http://127.0.0.1:8745/';

export default function Page() {
  return (
    <main>
      {/* Pages Router idiom: the <head> is declared with next/head, NOT a metadata export
          (that is App Router) and never with a raw <head> tag. */}
      <Head>
        <title>{title}</title>
        <meta name="description" content={description} />
        <link rel="canonical" href={canonical} />
        <meta property="og:type" content="website" />
        <meta property="og:title" content={title} />
        <meta property="og:description" content={description} />
        <meta property="og:url" content={canonical} />
        <meta property="og:image" content="http://127.0.0.1:8745/og.png" />
        <meta name="twitter:card" content="summary_large_image" />
        <meta name="twitter:title" content={title} />
        <meta name="twitter:description" content={description} />
        <meta name="twitter:image" content="http://127.0.0.1:8745/og.png" />
      </Head>
      <h1>{title}</h1>
      <p>Site fixture pour prouver la boucle complete sur la stack Next.js Pages Router.</p>
      <nav><a href="/">Accueil</a> <a href="/blog">Blog</a> <a href="/a-propos">A propos</a></nav>
    </main>
  );
}
