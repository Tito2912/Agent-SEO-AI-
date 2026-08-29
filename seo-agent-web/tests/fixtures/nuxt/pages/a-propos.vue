<script setup>
const title = 'A propos du site de test Nuxt';
const description = "Page temoin du site fixture Nuxt : elle est saine et doit rester rigoureusement intacte apres la correction.";
const canonical = 'http://127.0.0.1:8746/a-propos';

// Nuxt idiom: head tags are declared with useHead(), never as raw HTML in the template.
useHead({
  title,
  meta: [
    { name: 'description', content: description },
    { property: 'og:type', content: 'website' },
    { property: 'og:title', content: title },
    { property: 'og:description', content: description },
    { property: 'og:url', content: canonical },
    { property: 'og:image', content: 'http://127.0.0.1:8746/og.png' },
    { name: 'twitter:card', content: 'summary_large_image' },
    { name: 'twitter:title', content: title },
    { name: 'twitter:description', content: description },
    { name: 'twitter:image', content: 'http://127.0.0.1:8746/og.png' },
  ],
  link: [{ rel: 'canonical', href: canonical }],
});
</script>

<template>
  <main>
    <h1>{{ title }}</h1>
    <p>Page temoin : elle doit rester intacte apres la correction.</p>
  </main>
</template>
