import { Html, Head, Main, NextScript } from 'next/document';

// The SHARED document. Next.js Pages Router emits <html> with no lang attribute unless this
// file sets it, which flagged all three pages as html_lang_attribute_missing. It is also the
// per-stack shared template: a per-page value such as a canonical must never land here.
export default function Document() {
  return (
    <Html lang="fr">
      <Head />
      <body>
        <Main />
        <NextScript />
      </body>
    </Html>
  );
}
