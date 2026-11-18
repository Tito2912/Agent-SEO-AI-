"""Assainissement HTML conservateur partage par les adaptateurs.

But : eviter d'injecter des liens externes non prevus par le brouillon, de reecrire le contenu
du client, ou d'envoyer sur un CMS des balises exotiques qu'il reecrira silencieusement. La
fonction `sanitize_html` ne reecrit PAS le contenu : elle refuse tout element ou attribut qui
sort de la liste autorisee, en levant AdapterError(code='validation'). Le contrat d'entree est
donc explicite : si le brouillon contient quelque chose d'inattendu, l'appelant corrige la
source au lieu de perdre du contenu sans prevenir.

La liste d'elements autorises est volontairement restreinte aux balises editoriales usuelles.
Elle est partagee entre adaptateurs ; `allowed_tags` permet de l'elargir ou de la restreindre
pour une plateforme particuliere.
"""
from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import FrozenSet, Iterable, Optional, Set, Tuple
from urllib.parse import urlsplit

from .base import AdapterError


DEFAULT_TAGS: FrozenSet[str] = frozenset({
    "p", "br", "hr",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "strong", "b", "em", "i", "u", "s",
    "ul", "ol", "li",
    "blockquote", "pre", "code",
    "a", "img",
    "figure", "figcaption",
})

VOID_ELEMENTS: FrozenSet[str] = frozenset({"br", "hr", "img"})

ALLOWED_ATTRS = {
    "a": frozenset({"href", "title", "rel"}),
    "img": frozenset({"src", "alt", "title", "width", "height"}),
}


_SAFE_URL = re.compile(r"\A(?:https?://|mailto:|#|/)", re.IGNORECASE)


class _Collector(HTMLParser):
    def __init__(self, allowed: FrozenSet[str]):
        super().__init__(convert_charrefs=True)
        self.allowed = allowed
        self.used: Set[str] = set()
        self.links: Set[str] = set()
        self.images: Set[str] = set()
        self.refused: Optional[str] = None
        self.refused_attr: Optional[Tuple[str, str]] = None
        self._open: list = []

    def handle_starttag(self, tag, attrs):
        if tag not in self.allowed and self.refused is None:
            self.refused = tag
            return
        self.used.add(tag)
        for name, value in attrs:
            if name not in ALLOWED_ATTRS.get(tag, frozenset()):
                if self.refused_attr is None:
                    self.refused_attr = (tag, name)
                return
            if tag == "a" and name == "href":
                if not _is_safe_url(value):
                    if self.refused_attr is None:
                        self.refused_attr = ("a", "href")
                    return
                self.links.add(value or "")
            if tag == "img" and name == "src":
                if not _is_safe_url(value):
                    if self.refused_attr is None:
                        self.refused_attr = ("img", "src")
                    return
                self.images.add(value or "")
        if tag not in VOID_ELEMENTS:
            self._open.append(tag)

    def handle_endtag(self, tag):
        if tag in VOID_ELEMENTS:
            return
        if self._open and self._open[-1] == tag:
            self._open.pop()

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)

    def handle_data(self, data):
        return  # texte autorise tel quel


def _is_safe_url(value: str) -> bool:
    if value is None:
        return False
    value = value.strip()
    if not value:
        return True  # vide = autorise, la balise sera visible mais inoffensive
    if not _SAFE_URL.match(value):
        return False
    try:
        parts = urlsplit(value)
    except ValueError:
        return False
    if parts.scheme in {"http", "https"} and not parts.hostname:
        return False
    return True


def validate_html(html: str, *, allowed_tags: Optional[Iterable[str]] = None) -> None:
    """Refuse toute balise ou tout attribut hors liste. N'altere pas le contenu."""
    if not isinstance(html, str):
        raise AdapterError("Le contenu HTML doit etre une chaine.", code="validation")
    allowed = frozenset(allowed_tags) if allowed_tags is not None else DEFAULT_TAGS
    collector = _Collector(allowed)
    try:
        collector.feed(html)
        collector.close()
    except Exception as error:
        raise AdapterError("Le contenu HTML n'a pas pu etre analyse.", code="validation") from error
    if collector.refused:
        raise AdapterError(
            "Balise HTML non autorisee pour cette plateforme : <%s>." % collector.refused,
            code="validation", details={"tag": collector.refused})
    if collector.refused_attr:
        tag, attr = collector.refused_attr
        raise AdapterError(
            "Attribut HTML non autorise : <%s %s=...>." % (tag, attr),
            code="validation", details={"tag": tag, "attr": attr})
