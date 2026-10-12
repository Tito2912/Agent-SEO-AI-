<script setup>
// FAMILLE VISEE : multiple_h1
// deux <h1> sur la meme page.
useHead({
    title: 'Parcours Noyaru : multiple h1',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « multiple h1 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Parcours Noyaru : multiple h1' },
      { property: 'og:description', content: 'Page « multiple h1 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/multiple-h1' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : multiple h1' },
      { name: 'twitter:description', content: 'Page « multiple h1 » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/multiple-h1' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : multiple h1</h1>
    <h1>Un second titre de niveau 1</h1>
    <p>Cette page du parcours traite le cas « multiple h1 ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
  </main>
</template>
