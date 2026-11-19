"""Conversion HTML -> Ricos (sous-ensemble) pour l'adaptateur Wix.

Wix Blog v3 Draft Posts attend `richContent` au format Ricos (JSON), pas du HTML arbitraire.
Une conversion complete depasse ce lot. On supporte un sous-ensemble explicite et on REFUSE
toute balise ou tout attribut hors de ce sous-ensemble : jamais de perte silencieuse.

Sous-ensemble supporte :
- paragraphes `<p>`
- titres `<h1>` a `<h6>`
- listes `<ul>`/`<ol>` avec `<li>`
- citations `<blockquote>` (un paragraphe)
- texte riche : `<strong>/<b>`, `<em>/<i>`, `<u>`, `<a href="...">`
- separation `<hr>` et retour `<br>` (dans un paragraphe uniquement)
- image seule `<img src alt>` (l'URL doit deja etre hebergee)

Non supporte, donc refuse (AdapterError code='validation') :
- tableaux, code, iframe, script, style, listes imbriquees, images annotees, media embarque.
"""
from __future__ import annotations

import uuid
from html.parser import HTMLParser
from typing import List, Optional

from .base import AdapterError
from .html_safe import _is_safe_url


RICOS_TAGS = frozenset({
    "p", "br", "hr",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "strong", "b", "em", "i", "u",
    "ul", "ol", "li",
    "blockquote",
    "a", "img",
})

_HEADINGS = {"h1": 1, "h2": 2, "h3": 3, "h4": 4, "h5": 5, "h6": 6}


def _node_id() -> str:
    return uuid.uuid4().hex[:16]


class _RicosBuilder(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.nodes: List[dict] = []
        self._stack: List[dict] = []    # pile des blocs ouverts (p, h, blockquote, li)
        self._list_stack: List[dict] = []  # pile des listes (<ul>, <ol>)
        self._decor_stack: List[dict] = []  # pile des decorations actives
        self._link_stack: List[str] = []
        self._in_paragraph = False
        self._refused: Optional[str] = None

    # ---- navigation ------------------------------------------------------
    def handle_starttag(self, tag, attrs):
        if self._refused is not None:
            return
        attrs_map = {name: value or "" for name, value in attrs}
        if tag not in RICOS_TAGS:
            self._refused = "tag:" + tag
            return
        if tag == "p":
            self._open_text_block("PARAGRAPH")
        elif tag in _HEADINGS:
            self._open_text_block("HEADING", {"level": _HEADINGS[tag]})
        elif tag == "blockquote":
            self._open_text_block("BLOCKQUOTE")
        elif tag == "ul":
            self._open_list("BULLETED_LIST")
        elif tag == "ol":
            self._open_list("ORDERED_LIST")
        elif tag == "li":
            if not self._list_stack:
                self._refused = "stray:li"
                return
            self._open_text_block("LIST_ITEM")
        elif tag in {"strong", "b"}:
            self._decor_stack.append({"type": "BOLD"})
        elif tag in {"em", "i"}:
            self._decor_stack.append({"type": "ITALIC"})
        elif tag == "u":
            self._decor_stack.append({"type": "UNDERLINE"})
        elif tag == "a":
            href = (attrs_map.get("href") or "").strip()
            if not _is_safe_url(href):
                self._refused = "attr:a.href"
                return
            self._link_stack.append(href)
            self._decor_stack.append({"type": "LINK", "linkData": {"link": {"url": href}}})
        elif tag == "br":
            if self._stack:
                self._append_text("\n")
        elif tag == "hr":
            self._flush_pending()
            self.nodes.append({"id": _node_id(), "type": "DIVIDER", "dividerData": {}, "nodes": []})
        elif tag == "img":
            self._handle_image(attrs_map)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        # Les auto-fermantes sont void (br, hr, img) ; aucune fermeture a propager.

    def handle_endtag(self, tag):
        if self._refused is not None:
            return
        if tag in {"p", "blockquote"} or tag in _HEADINGS or tag == "li":
            self._close_text_block()
        elif tag in {"ul", "ol"}:
            if self._list_stack:
                self._list_stack.pop()
        elif tag in {"strong", "b"}:
            self._pop_decor("BOLD")
        elif tag in {"em", "i"}:
            self._pop_decor("ITALIC")
        elif tag == "u":
            self._pop_decor("UNDERLINE")
        elif tag == "a":
            self._pop_decor("LINK")
            if self._link_stack:
                self._link_stack.pop()

    def handle_data(self, data):
        if self._refused is not None or not data:
            return
        self._append_text(data)

    # ---- helpers ---------------------------------------------------------
    def _open_text_block(self, node_type: str, data: Optional[dict] = None):
        block = {"id": _node_id(), "type": node_type, "nodes": []}
        if node_type == "HEADING" and data:
            block["headingData"] = {"level": data["level"]}
        self._stack.append(block)

    def _close_text_block(self):
        if not self._stack:
            return
        block = self._stack.pop()
        self._attach(block)

    def _attach(self, block: dict):
        if self._list_stack and block["type"] == "LIST_ITEM":
            self._list_stack[-1]["nodes"].append(block)
        elif self._stack:
            self._stack[-1]["nodes"].append(block)
        else:
            self.nodes.append(block)

    def _open_list(self, node_type: str):
        self._flush_pending()
        block = {"id": _node_id(), "type": node_type, "nodes": []}
        if self._list_stack or self._stack:
            self._refused = "nested:list"
            return
        self.nodes.append(block)
        self._list_stack.append(block)

    def _append_text(self, data: str):
        if not self._stack:
            if data.strip() == "":
                return
            # texte libre hors paragraphe : on refuse pour forcer un balisage explicite
            self._refused = "stray:text"
            return
        host = self._stack[-1]
        decorations = [dict(d) for d in self._decor_stack]
        host["nodes"].append({
            "id": _node_id(),
            "type": "TEXT",
            "textData": {"text": data, "decorations": decorations},
            "nodes": [],
        })

    def _pop_decor(self, kind: str):
        for i in range(len(self._decor_stack) - 1, -1, -1):
            if self._decor_stack[i]["type"] == kind:
                del self._decor_stack[i]
                return

    def _flush_pending(self):
        # Les blocs texte doivent etre fermes par leur balise ; on ne les ferme pas tacitement.
        return

    def _handle_image(self, attrs_map):
        src = (attrs_map.get("src") or "").strip()
        alt = attrs_map.get("alt") or ""
        if not _is_safe_url(src) or not src:
            self._refused = "attr:img.src"
            return
        image_node = {
            "id": _node_id(),
            "type": "IMAGE",
            "imageData": {"image": {"src": {"url": src}}, "altText": alt},
            "nodes": [],
        }
        if self._stack:
            # Wix attend l'image comme bloc autonome ; on refuse les images inline.
            self._refused = "inline:img"
            return
        self.nodes.append(image_node)


def html_to_ricos(html: str) -> dict:
    """Convertit `html` en document Ricos (JSON serialisable). Leve AdapterError sur un
    element non supporte ; aucune balise n'est silencieusement ignoree."""
    if not isinstance(html, str):
        raise AdapterError("Le contenu HTML doit etre une chaine.", code="validation")
    builder = _RicosBuilder()
    try:
        builder.feed(html)
        builder.close()
    except Exception as error:
        raise AdapterError("Le HTML n'a pas pu etre analyse pour Wix.", code="validation") from error
    if builder._refused:
        raise AdapterError(
            "Element HTML non supporte par la conversion Wix Ricos : %s." % builder._refused,
            code="validation", details={"refused": builder._refused})
    if builder._stack or builder._list_stack:
        raise AdapterError(
            "Balise HTML non fermee avant la fin du document.",
            code="validation")
    return {"nodes": builder.nodes}
