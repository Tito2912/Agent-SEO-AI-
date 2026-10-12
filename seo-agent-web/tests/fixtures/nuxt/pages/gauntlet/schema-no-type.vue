<script setup>
// FAMILLE VISEE : structured_data_schema_org_validation_error
// objet schema.org sans @type declare.
useHead({
    title: 'Parcours Noyaru : schema no type',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « schema no type » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Parcours Noyaru : schema no type' },
      { property: 'og:description', content: 'Page « schema no type » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/schema-no-type' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : schema no type' },
      { name: 'twitter:description', content: 'Page « schema no type » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/schema-no-type' }
    ],
    script: [
      { type: 'application/ld+json', innerHTML: '{"@context":"https://schema.org","name":"Objet sans type declare"}' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : schema no type</h1>
    <p>Cette page du parcours traite le cas « schema no type ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
  </main>
</template>
