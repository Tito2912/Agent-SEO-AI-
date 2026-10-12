<script setup>
// FAMILLE VISEE : page_has_links_to_redirect
// lien vers une URL qui redirige (declaree cote hote).
useHead({
    title: 'Parcours Noyaru : link to redirect',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Page « link to redirect » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Parcours Noyaru : link to redirect' },
      { property: 'og:description', content: 'Page « link to redirect » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/link-to-redirect' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Parcours Noyaru : link to redirect' },
      { name: 'twitter:description', content: 'Page « link to redirect » du parcours d\'obstacles : correcte partout sauf sur un point precis, pour que seule la famille visee se declenche.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/link-to-redirect' }
    ],
});
</script>

<template>
  <main>
    <h1>Parcours d'obstacles : link to redirect</h1>
    <p>Cette page du parcours traite le cas « link to redirect ». Cette page appartient au parcours d'obstacles de la fixture. Elle sert a provoquer UNE anomalie et une seule.</p>
    <p><a href="/">Retour a l accueil</a></p>
    <p><a href="/gauntlet/ancienne-page">Ancienne page</a></p>
  </main>
</template>
