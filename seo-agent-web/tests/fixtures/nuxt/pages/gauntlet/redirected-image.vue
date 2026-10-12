<script setup>
// FAMILLE VISEE : image_redirects + page_has_redirected_image
// image chargee depuis une URL qui redirige.
useHead({
    title: 'Parcours Noyaru : redirected image',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « redirected image » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Parcours Noyaru : redirected image' },
      { property: 'og:description', content: 'Page « redirected image » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/redirected-image' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : redirected image' },
      { name: 'twitter:description', content: 'Page « redirected image » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/redirected-image' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : redirected image</h1>
    <p>Cette page du parcours traite le cas « redirected image ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
    <img :src="'/img/ancienne.png'" alt="Illustration de test" />
  </main>
</template>
