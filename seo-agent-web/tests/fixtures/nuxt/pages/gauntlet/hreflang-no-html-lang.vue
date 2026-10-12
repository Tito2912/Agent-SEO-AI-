<script setup>
// FAMILLE VISEE : hreflang_defined_but_html_lang_missing
// hreflang declare mais <html> sans attribut lang.
useHead({
    title: 'Parcours Noyaru : hreflang no html lang',
    htmlAttrs: { lang: null },  // langue retiree : l'anomalie visee
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « hreflang no html lang » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Parcours Noyaru : hreflang no html lang' },
      { property: 'og:description', content: 'Page « hreflang no html lang » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/hreflang-no-html-lang' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : hreflang no html lang' },
      { name: 'twitter:description', content: 'Page « hreflang no html lang » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/hreflang-no-html-lang' },
      { rel: 'alternate', hreflang: 'fr', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/hreflang-no-html-lang' },
      { rel: 'alternate', hreflang: 'en', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/hreflang-to-non-canonical' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : hreflang no html lang</h1>
    <p>Cette page du parcours traite le cas « hreflang no html lang ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
  </main>
</template>
