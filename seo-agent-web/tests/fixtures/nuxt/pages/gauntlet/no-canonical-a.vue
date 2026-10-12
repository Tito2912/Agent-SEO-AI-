<script setup>
// FAMILLE VISEE : duplicate_pages_without_canonical
// jumelle de no-canonical-b, aucune des deux ne declare de canonical.
useHead({
    title: 'Deux pages jumelles sans canonical declare',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Deux pages jumelles au contenu identique et sans canonical declare : le doublon que la famille des pages dupliquees doit trouver.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Deux pages jumelles sans canonical declare' },
      { property: 'og:description', content: 'Deux pages jumelles au contenu identique et sans canonical declare : le doublon que la famille des pages dupliquees doit trouver.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/no-canonical-a' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Deux pages jumelles sans canonical declare' },
      { name: 'twitter:description', content: 'Deux pages jumelles au contenu identique et sans canonical declare : le doublon que la famille des pages dupliquees doit trouver.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : pages jumelles</h1>
    <p>Cette page du parcours traite le cas « pages jumelles ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
  </main>
</template>
