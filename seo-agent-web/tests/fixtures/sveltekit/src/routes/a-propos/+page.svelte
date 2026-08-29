<script>
  const title = 'A propos du site de test SvelteKit';
  const description = "Page temoin du site fixture SvelteKit : elle est saine et doit rester rigoureusement intacte apres la correction automatique.";
  const canonical = 'http://127.0.0.1:8743/a-propos';
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
<p>Page temoin : elle doit rester intacte apres la correction.</p>
