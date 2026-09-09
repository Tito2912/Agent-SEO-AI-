import adapter from '@sveltejs/adapter-static';

// Fully prerendered: the crawler must see a built static site, exactly what a customer deploys.
export default {
  kit: {
    adapter: adapter({ pages: 'build', assets: 'build', fallback: null, precompress: false }),
    prerender: {
      // Le parcours d'obstacles lie DELIBEREMENT une URL qui redirige (famille
      // page_has_links_to_redirect) : la regle vit dans `_redirects`, cote hote, et le
      // verificateur de liens du prerendu ne peut pas la connaitre — il voit un 404 et fait
      // echouer le build. On l'abaisse en avertissement, ce qui ne touche a aucune propriete
      // SEO de la fixture : c'est un controle de build, pas un controle de crawl.
      handleHttpError: 'warn',
    },
  },
};
