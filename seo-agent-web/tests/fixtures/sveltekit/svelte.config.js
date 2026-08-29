import adapter from '@sveltejs/adapter-static';

// Fully prerendered: the crawler must see a built static site, exactly what a customer deploys.
export default {
  kit: {
    adapter: adapter({ pages: 'build', assets: 'build', fallback: null, precompress: false }),
  },
};
