<script setup>
// FAMILLE VISEE : title_too_long_not_indexable + meta_description_too_long_not_indexable
// page noindex portant un titre et une description hors plafond.
useHead({
    title: 'Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, ecrit pour depasser nettement le plafond',
    htmlAttrs: { lang: 'fr' },
    meta: [
      { name: 'viewport', content: 'width=device-width' },
      { name: 'description', content: 'Une meta description volontairement tres longue, ecrite pour depasser le plafond de cent soixante caracteres retenu par le crawler, afin de declencher la famille des descriptions trop longues sur une page non indexable.' },
      { name: 'robots', content: 'noindex, follow' },
      { property: 'og:type', content: 'article' },
      { property: 'og:title', content: 'Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, ecrit pour depasser nettement le plafond' },
      { property: 'og:description', content: 'Une meta description volontairement tres longue, ecrite pour depasser le plafond de cent soixante caracteres retenu par le crawler, afin de declencher la famille des descriptions trop longues sur une page non indexable.' },
      { property: 'og:url', content: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/noindex-long' },
      { property: 'og:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' },
      { name: 'twitter:card', content: 'summary_large_image' },
      { name: 'twitter:title', content: 'Un titre volontairement beaucoup trop long pour la fenetre visee par le correcteur, ecrit pour depasser nettement le plafond' },
      { name: 'twitter:description', content: 'Une meta description volontairement tres longue, ecrite pour depasser le plafond de cent soixante caracteres retenu par le crawler, afin de declencher la famille des descriptions trop longues sur une page non indexable.' },
      { name: 'twitter:image', content: 'https://noyaru-stack-nuxt.netlify.app/og.png' }
    ],
    link: [
      { rel: 'canonical', href: 'https://noyaru-stack-nuxt.netlify.app/gauntlet/noindex-long' }
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
