<script>
  // The SHARED layout. It renders the chrome every page has; a per-page value such as a
  // canonical must never be written here — it would land on every page of the site.
  let { children } = $props();
</script>

{@render children()}

<nav><a href="/">Accueil</a> <a href="/blog">Blog</a> <a href="/a-propos">A propos</a></nav>
