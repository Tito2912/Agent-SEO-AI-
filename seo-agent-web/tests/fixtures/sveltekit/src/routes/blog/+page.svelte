<script>
  const title = 'Blog du site de test SvelteKit';
  const description = "Index du blog du site fixture SvelteKit, servant a verifier que la correction du canonical ne touche ni le layout partage ni les autres pages.";
  // THE INJECTED DEFECT: the trailing slash. The host serves /blog and 301s /blog/ to it.
  const canonical = 'http://127.0.0.1:8743/blog/';
</script>

<svelte:head>
  <title>{title}</title>
  <meta name="description" content={description} />
  <link rel="canonical" href={canonical} />
  <meta property="og:type" content="website" />
  <meta property="og:title" content={title} />
  <meta property="og:description" content={description} />
  <meta property="og:url" content={canonical} />
  <meta property="og:image" content="http://127.0.0.1:8743/og.png" />
  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:title" content={title} />
  <meta name="twitter:description" content={description} />
  <meta name="twitter:image" content="http://127.0.0.1:8743/og.png" />
</svelte:head>

<h1>{title}</h1>
<p>Articles.</p>
