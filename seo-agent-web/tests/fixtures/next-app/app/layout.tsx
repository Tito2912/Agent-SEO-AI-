// Le gabarit PARTAGE. Il porte <html lang> et rien qui appartienne a une page : un correctif
// per-page qui atterrirait ici donnerait le meme titre a tout le site. C'est exactement ce que
// `_PER_PAGE_ONLY_KEYS` et `repo_index.is_shared_path` empechent, et ce fichier est la pour
// qu'un test puisse le verifier contre un vrai build.
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr">
      <body>
        {children}
        <nav><a href="/">Accueil</a> <a href="/blog">Blog</a> <a href="/a-propos">A propos</a></nav>
      </body>
    </html>
  );
}
