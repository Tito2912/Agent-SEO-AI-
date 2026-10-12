<script setup>
// FAMILLE VISEE : https_page_links_to_http_css
// feuille de style chargee en http.
useHead({
    title: 'Parcours Noyaru : mixed css',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « mixed css » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Parcours Noyaru : mixed css' },
      { property: 'og:description', content: 'Page « mixed css » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/mixed-css' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : mixed css' },
      { name: 'twitter:description', content: 'Page « mixed css » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/mixed-css' },
      { rel: 'stylesheet', href: 'http://noyaru-stack-nuxt.netlify.app/style.css' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : mixed css</h1>
    <p>Cette page du parcours traite le cas « mixed css ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
  </main>
</template>
