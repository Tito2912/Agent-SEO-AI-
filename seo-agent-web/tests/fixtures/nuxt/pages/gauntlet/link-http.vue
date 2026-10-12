<script setup>
// FAMILLE VISEE : https_page_has_internal_links_to_http
// lien interne ecrit en http.
useHead({
    title: 'Parcours Noyaru : link http',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « link http » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Parcours Noyaru : link http' },
      { property: 'og:description', content: 'Page « link http » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/link-http' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : link http' },
      { name: 'twitter:description', content: 'Page « link http » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/link-http' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : link http</h1>
    <p>Cette page du parcours traite le cas « link http ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
    <p><a href="http://noyaru-stack-nuxt.netlify.app/a-propos">A propos</a></p>
  </main>
</template>
