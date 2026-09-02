export const metadata = {
  title: "A propos du site de test Next App Router",
  description: "Page temoin du fixture Next App Router : elle est saine et doit rester rigoureusement intacte apres l'application de la correction automatique.",
  alternates: { canonical: "http://127.0.0.1:8749/a-propos" },
  openGraph: {
    type: "website",
    title: "A propos du site de test Next App Router",
    description: "Page temoin du fixture Next App Router.",
    url: "http://127.0.0.1:8749/a-propos",
    images: ["http://127.0.0.1:8749/og.png"],
  },
};

export default function Page() {
  return <main><h1>A propos</h1><p>Page temoin : elle doit rester intacte apres la correction.</p></main>;
}
