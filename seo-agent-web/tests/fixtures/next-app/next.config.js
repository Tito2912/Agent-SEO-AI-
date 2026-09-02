// App Router, export statique : `next build` ecrit le site dans out/, ce qu'un client deploie.
// trailingSlash reste au defaut (false), donc l'hote sert /blog et redirige /blog/ vers lui :
// c'est ce qui fait du slash final injecte dans app/blog/page.tsx un vrai defaut.
module.exports = { output: 'export', trailingSlash: false, images: { unoptimized: true } };
