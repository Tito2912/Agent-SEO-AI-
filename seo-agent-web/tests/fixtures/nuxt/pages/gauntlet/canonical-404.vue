<script setup>
// FAMILLE VISEE : canonical_points_to_4xx
// canonical vers une page absente, listee au sitemap pour que le crawl constate le 404.
useHead({
    title: 'Parcours Noyaru : canonical 404',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « canonical 404 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Parcours Noyaru : canonical 404' },
      { property: 'og:description', content: 'Page « canonical 404 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/canonical-404' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : canonical 404' },
      { name: 'twitter:description', content: 'Page « canonical 404 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/page-absente' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : canonical 404</h1>
    <p>Cette page du parcours traite le cas « canonical 404 ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
  </main>
</template>
