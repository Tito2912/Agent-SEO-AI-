import { defineConfig } from 'astro/config';

// `format: 'file'` builds `/blog` as `blog.html` instead of `blog/index.html`, and
// `trailingSlash: 'never'` makes `/blog/` the non-canonical spelling. That is the exact shape of
// the real defect this fixture reproduces: a page declaring a canonical the site redirects away
// from. See tests/fixtures/astro/README.md.
export default defineConfig({
  site: 'http://127.0.0.1:8741',
  trailingSlash: 'never',
  build: { format: 'file' },
});
