// Gatsby emits <html> with NO lang attribute by default, which the crawler correctly flags as
// html_lang_attribute_missing on every page. A real Gatsby site sets it here; the fixture must
// be clean apart from the ONE injected defect, or "zero at the end" proves nothing.
exports.onRenderBody = ({ setHtmlAttributes }) => {
  setHtmlAttributes({ lang: 'fr' });
};
