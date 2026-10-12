<script setup>
// FAMILLE VISEE : html_lang_attribute_missing
// <html> sans attribut lang.
useHead({
    title: 'Parcours Noyaru : html lang missing',
    htmlAttrs: { lang: null },  // langue retiree : l'anomalie visee
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « html lang missing » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Parcours Noyaru : html lang missing' },
      { property: 'og:description', content: 'Page « html lang missing » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/html-lang-missing' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : html lang missing' },
      { name: 'twitter:description', content: 'Page « html lang missing » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/html-lang-missing' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : html lang missing</h1>
    <p>Cette page du parcours traite le cas « html lang missing ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
  </main>
</template>
