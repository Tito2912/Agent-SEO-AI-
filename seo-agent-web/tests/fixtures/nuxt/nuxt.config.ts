// `nuxt generate` prerenders every route into .output/public — a static site, what a customer
// deploys. trailingSlash is left at Nuxt's default; the host simulated by
// tests/static_site_server.py 301s /x/ to /x, which is what makes the injected canonical a defect.
export default defineNuxtConfig({
  compatibilityDate: '2026-08-29',
  ssr: true,
  // Nuxt emits <html> with no lang attribute unless it is set here, which flagged all three
  // pages as html_lang_attribute_missing. The fixture must be clean apart from the ONE injected
  // defect; a real Nuxt site sets it the same way.
  app: { head: { htmlAttrs: { lang: 'fr' } } },
  nitro: { prerender: { crawlLinks: true, routes: ['/', '/blog', '/a-propos'] } },
});
