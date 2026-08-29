import * as React from 'react';

const title = 'Blog du site de test Gatsby';
const description = "Index du blog du site fixture Gatsby, servant a verifier que la correction du canonical ne touche ni les autres pages ni la config.";
// THE INJECTED DEFECT: the trailing slash. The host serves /blog and 301s /blog/ to it.
const canonical = 'http://127.0.0.1:8744/blog/';

export default function Page() {
  return (
    <main>
      <h1>{title}</h1>
      <p>Articles.</p>
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
