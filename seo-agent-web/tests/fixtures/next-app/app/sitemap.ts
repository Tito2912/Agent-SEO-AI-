// Le sitemap est GENERE : il n'existe aucun <loc> litteral a reecrire, ce qui est la raison
// d'etre du repli IA des familles sitemap. Il declare les URL sans slash final, comme l'hote.
export default function sitemap() {
  const base = "http://127.0.0.1:8749";
  return [
    { url: base + "/" },
    { url: base + "/blog" },
    { url: base + "/a-propos" },
  ];
}
