// Le sitemap est GENERE : il n'existe aucun <loc> litteral a reecrire, ce qui est la raison
// d'etre du repli IA des familles sitemap. Il declare les URL sans slash final, comme l'hote.
//
// `force-static` est OBLIGATOIRE avec `output: 'export'` : sans elle, `next build` echoue sur
// "Failed to collect page data for /sitemap.xml". Mesure faite en construisant le fixture, pas
// devinee — c'est exactement le genre de detail qu'un fixture non construit laisse passer.
export const dynamic = "force-static";

export default function sitemap() {
  const base = "http://127.0.0.1:8749";
  return [
    { url: base + "/" },
    { url: base + "/blog" },
    { url: base + "/a-propos" },
    // FAMILLE VISEE : sitemap_http_urls_for_https — une entree en clair dans le sitemap d'un
    // site servi en https. Elle est DERIVEE, pas litterale : l'hote est reecrit au moment de
    // la publication, donc un `http://` ecrit en dur ne survivrait pas. C'est aussi le cas
    // reel de cette stack, et la raison d'etre du repli IA des familles de sitemap : il n'y a
    // aucun <loc> a remplacer sur place, seulement du code qui les produit.
    { url: base.replace("https://", "http://") + "/gauntlet/" },
    // FAMILLE VISEE : sitemap_4xx_page — le sitemap propose aux moteurs une page qui n'existe
    // pas. C'est la cible du canonical de `gauntlet/canonical-404`, et la seule stack ou cette
    // entree vit dans du CODE : la reparation y passe donc par le repli modele, pas par la
    // suppression d'un `<loc>` litteral. C'est exactement le chemin qu'il faut eprouver.
    { url: base + "/page-absente" },
  ];
}
