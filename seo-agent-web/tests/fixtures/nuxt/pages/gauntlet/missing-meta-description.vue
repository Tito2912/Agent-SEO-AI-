<script setup>
// FAMILLE VISEE : meta_description_too_short_indexable
// aucune meta description sur une page indexable.
useHead({
    title: 'Page de test du parcours d\'obstacles Noyaru',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Page de test du parcours d\'obstacles Noyaru' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/missing-meta-description' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Page de test du parcours d\'obstacles Noyaru' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/missing-meta-description' }
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
