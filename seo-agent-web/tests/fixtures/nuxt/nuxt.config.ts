import { existsSync, readdirSync } from 'node:fs';

// `nuxt generate` prerenders every route into .output/public — a static site, what a customer
// deploys. trailingSlash is left at Nuxt's default; the host simulated by
// tests/static_site_server.py 301s /x/ to /x, which is what makes the injected canonical a defect.

const gauntletRoutes = existsSync('pages/gauntlet')
  ? readdirSync('pages/gauntlet')
      .filter((f) => f.endsWith('.vue'))
      .map((f) => '/gauntlet/' + f.replace(/\.vue$/, ''))
  : [];
export default defineNuxtConfig({
  compatibilityDate: '2026-08-29',
  ssr: true,
  // Nuxt emits <html> with no lang attribute unless it is set here, which flagged all three
  // pages as html_lang_attribute_missing. The fixture must be clean apart from the ONE injected
  // defect; a real Nuxt site sets it the same way.
  app: { head: { htmlAttrs: { lang: 'fr' } } },
  nitro: {
    prerender: {
      crawlLinks: true,
      // Les pages du parcours sont listees EXPLICITEMENT. Le crawler de prerendu ne decouvre
      // que ce qu'il atteint par des liens, et l'index du parcours est un fichier statique
      // qu'il ne parcourt pas : mesure, 12 pages sur 31 seulement etaient generees. La liste
      // est lue sur le disque pour qu'ajouter une page au parcours suffise.
      routes: ['/', '/blog', '/a-propos', ...gauntletRoutes],
      // Le parcours d'obstacles lie DELIBEREMENT une URL qui redirige (famille
      // page_has_links_to_redirect). La regle vit dans `_redirects`, cote hote ; le crawler de
      // prerendu ne peut pas la connaitre, voit un 404 et fait echouer un build par ailleurs
      // complet — mesure : les 31 pages etaient generees avant qu'il ne sorte en erreur.
      // C'est un controle de build, il ne touche a aucune propriete SEO de la fixture.
      failOnError: false,
    },
  },
});
