/** @type {import('next').NextConfig} */
// `output: 'export'` produces a static site in out/, which is what the crawler must see.
// `trailingSlash: false` makes /blog/ the non-canonical spelling, matching the host simulated by
// tests/static_site_server.py.
module.exports = { output: 'export', trailingSlash: false, images: { unoptimized: true } };
