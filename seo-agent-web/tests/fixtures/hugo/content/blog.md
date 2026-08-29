+++
title = "Blog du site de test Hugo"
# THE INJECTED DEFECT: the trailing slash. The site serves /blog and 301s /blog/ to it, so this
# page declares as canonical a URL that redirects away. TOML front matter on purpose: the value
# is an ASSIGNMENT (`canonical = "..."`), the form the rewriter could not read before 2026-08-29.
description = "Index du blog du site fixture Hugo, servant a verifier que la correction du canonical ne touche ni le template partage ni les autres pages."
canonical = "http://127.0.0.1:8742/blog/"
+++

Articles.
