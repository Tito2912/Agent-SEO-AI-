<script>
  // FAMILLE VISEE : https_page_links_to_http_javascript
  // script charge en http.
</script>

<svelte:head>
  <meta name="viewport" content="width=device-width" />
  <title>Parcours Noyaru : mixed js</title>
  <meta name="description" content="Page « mixed js » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
  <link rel="canonical" href="https://noyaru-stack-sveltekit.netlify.app/gauntlet/mixed-js" />
  <meta property="og:type" content="article" />
  <meta property="og:title" content="Parcours Noyaru : mixed js" />
  <meta property="og:description" content="Page « mixed js » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
  <meta property="og:url" content="https://noyaru-stack-sveltekit.netlify.app/gauntlet/mixed-js" />
  <meta property="og:image" content="https://noyaru-stack-sveltekit.netlify.app/og.png" />
  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:title" content="Parcours Noyaru : mixed js" />
  <meta name="twitter:description" content="Page « mixed js » du parcours d'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche." />
  <meta name="twitter:image" content="https://noyaru-stack-sveltekit.netlify.app/og.png" />
</svelte:head>

<h1>Parcours d'obstacles : mixed js</h1>
<p>Cette page du parcours traite le cas « mixed js ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
<p><a href="/">Retour a l accueil</a></p>
{@html '<script src="http://noyaru-stack-sveltekit.netlify.app/app.js"></script>'}
