import * as React from 'react';

const title = 'Accueil du site de test Gatsby';
const description = "Page d'accueil du site fixture Gatsby utilise pour prouver la boucle complete de correction SEO, de l'injection au recrawl.";
const canonical = 'http://127.0.0.1:8744/';

export default function Page() {
  return (
    <main>
      <h1>{title}</h1>
      <p>Site fixture pour prouver la boucle complete sur la stack Gatsby.</p>
      <nav><a href="/">Accueil</a> <a href="/blog">Blog</a> <a href="/a-propos">A propos</a></nav>
    </main>
  );
}

// Gatsby Head API — the idiom the corrector must write here. `next/head` does not exist in a
// Gatsby project and importing it breaks the build; that was the real bug this stack revealed.
export function Head() {
  return (
    <>
      <title>{title}</title>
      <meta name="description" content={description} />
      <link rel="canonical" href={canonical} />
      <meta property="og:type" content="website" />
      <meta property="og:title" content={title} />
      <meta property="og:description" content={description} />
      <meta property="og:url" content={canonical} />
      <meta property="og:image" content="http://127.0.0.1:8744/og.png" />
      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:title" content={title} />
      <meta name="twitter:description" content={description} />
      <meta name="twitter:image" content="http://127.0.0.1:8744/og.png" />
    </>
  );
}
