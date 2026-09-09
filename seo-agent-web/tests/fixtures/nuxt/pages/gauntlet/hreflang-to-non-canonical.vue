<script setup>
// FAMILLE VISEE : hreflang_to_non_canonical
// hreflang pointant vers une page dont le canonical est ailleurs.
useHead({
    title: 'Page de test du parcours d\'obstacles Noyaru',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page du parcours d\'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Page de test du parcours d\'obstacles Noyaru' },
      { property: 'og:description', content: 'Page du parcours d\'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/hreflang-to-non-canonical' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Page de test du parcours d\'obstacles Noyaru' },
      { name: 'twitter:description', content: 'Page du parcours d\'obstacles : elle est correcte partout sauf sur un point precis, afin que la famille visee soit la seule a se declencher au crawl.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/hreflang-to-non-canonical' },
      { rel: 'alternate', hreflang: 'fr', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/hreflang-to-non-canonical' },
      { rel: 'alternate', hreflang: 'en', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/canonical-other' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles</h1>
    <p>Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
  </main>
</template>
