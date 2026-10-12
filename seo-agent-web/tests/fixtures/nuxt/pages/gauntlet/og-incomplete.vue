<script setup>
// FAMILLE VISEE : open_graph_tags_incomplete + twitter_card_incomplete
// og:title seul, sans description ni image ni url.
useHead({
    title: 'Parcours Noyaru : og incomplete',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « og incomplete » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:title', content: 'Open Graph incomplet' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : og incomplete' },
      { name: 'twitter:description', content: 'Page « og incomplete » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/og-incomplete' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : og incomplete</h1>
    <p>Cette page du parcours traite le cas « og incomplete ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
  </main>
</template>
