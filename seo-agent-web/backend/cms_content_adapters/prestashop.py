"""Adaptateur PrestaShop Webservice pour les PAGES CMS (API officielle du 17/11/2026).

Endpoints utilises (ressource `content_management_system`) :
- POST   /api/content_management_system          cree une page CMS (active=0)
- GET    /api/content_management_system/{id}     relit une page CMS
- PATCH  /api/content_management_system/{id}     met a jour une page CMS (partiel)

La methode PATCH est OFFICIELLEMENT prise en charge par le Webservice depuis PrestaShop 8
(voir la fiche recapitulative des verbes HTTP du Webservice PrestaShop 9). Les boutiques
PrestaShop 1.7 n'exposent que PUT (fiche entiere a chaque ecriture) ; ce cas n'est pas
gere par cet adaptateur : il attend un Webservice 8.x+ compatible PATCH.

Limites EXPLICITES :
- Cet adaptateur ne cree PAS d'article de blog. PrestaShop coeur n'a pas de blog natif
  universel ; les pages CMS (/cms/) sont la SEULE ressource editoriale documentee et
  interoperable. Ne declarer ni emuler un blog.
- Les medias ne sont pas uploades par cet adaptateur (`capabilities.upload_images=False`).
- Format d'echange : XML `<content>` ; chaque valeur est echappee, le corps est borne
  (pas de document geant). Pas d'interpolation brute. Pas de DOCTYPE accepte en reponse.
- Multilingue : `id_lang` fourni par la configuration ; les champs multilingues sont ecrits
  seulement pour cette langue (envoyer une seule langue efface les autres cote PrestaShop
  jusqu'en 8.x — l'appelant qui gere plusieurs langues DOIT envoyer toutes les variantes).

Revision : PrestaShop expose `date_upd` en XML ; on l'utilise comme jeton.
"""
from __future__ import annotations

import re
from typing import Any, Mapping, Optional
from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException
from xml.sax.saxutils import escape as xml_escape

from .base import (
    AdapterError,
    ArticleDraft,
    Capabilities,
    RemoteArticle,
    TransportFn,
    TransportResponse,
    check_revision,
    require_config,
    require_draft_fields,
    wrap_transport_errors,
)
from .html_safe import validate_html


REQUIRED_CONFIG = ("id_lang", "id_shop")
MAX_XML = 2 * 1024 * 1024
_ID_LANG = re.compile(r"^[1-9]\d{0,4}$")
_ID_SHOP = re.compile(r"^[1-9]\d{0,4}$")


class PrestaShopAdapter:
    platform = "prestashop"
    capabilities = Capabilities(
        create_draft=True, read=True, publish=True, update_draft=True,
        upload_images=False,
        requires=REQUIRED_CONFIG,
        notes=("PrestaShop 8.x Webservice. PAGES CMS uniquement, PAS de blog. "
               "XML echappe et borne. Multilingue partiel : une seule langue par appel."),
    )

    def __init__(self, *, transport: TransportFn, config: Mapping[str, Any]):
        require_config(config, REQUIRED_CONFIG)
        self.transport = transport
        self.config = dict(config)
        id_lang = str(self.config["id_lang"]).strip()
        id_shop = str(self.config["id_shop"]).strip()
        if not _ID_LANG.match(id_lang):
            raise AdapterError("id_lang PrestaShop invalide.", code="mapping")
        if not _ID_SHOP.match(id_shop):
            raise AdapterError("id_shop PrestaShop invalide.", code="mapping")
        self.id_lang = id_lang
        self.id_shop = id_shop
        self.id_cms_category = str(self.config.get("id_cms_category") or "1").strip()
        if not _ID_LANG.match(self.id_cms_category):
            raise AdapterError("id_cms_category PrestaShop invalide.", code="mapping")

    # ---- Operations publiques --------------------------------------------
    def create_draft(self, draft: ArticleDraft) -> RemoteArticle:
        require_draft_fields(draft)
        validate_html(draft.html)
        xml_body = self._xml(draft, active=False, with_id=False)
        response = wrap_transport_errors(
            lambda: self.transport(
                "POST", "/api/content_management_system?output_format=XML&id_shop=" + self.id_shop,
                payload=xml_body,
                headers={"Accept": "application/xml", "Content-Type": "application/xml"}),
            write=True, cms="PrestaShop")
        root = self._expect_xml(response, write=True)
        return self._to_remote(root)

    def read(self, remote_id: str) -> RemoteArticle:
        cms_id = self._check_id(remote_id)
        path = ("/api/content_management_system/" + cms_id
                + "?output_format=XML&id_shop=" + self.id_shop)
        response = wrap_transport_errors(
            lambda: self.transport("GET", path, headers={"Accept": "application/xml"}),
            write=False, cms="PrestaShop")
        root = self._expect_xml(response, write=False)
        return self._to_remote(root)

    def publish(self, remote_id: str, expected_revision: Optional[str] = None) -> RemoteArticle:
        cms_id = self._check_id(remote_id)
        current = self.read(cms_id)
        check_revision(expected_revision, current.revision)
        xml_body = self._xml_activation(cms_id, active=True)
        path = ("/api/content_management_system/" + cms_id
                + "?output_format=XML&id_shop=" + self.id_shop)
        response = wrap_transport_errors(
            lambda: self.transport(
                "PATCH", path, payload=xml_body,
                headers={"Accept": "application/xml", "Content-Type": "application/xml"}),
            write=True, cms="PrestaShop")
        root = self._expect_xml(response, write=True)
        return self._to_remote(root)

    def update_draft(self, remote_id: str, draft: ArticleDraft,
                     expected_revision: Optional[str] = None) -> RemoteArticle:
        require_draft_fields(draft)
        validate_html(draft.html)
        cms_id = self._check_id(remote_id)
        current = self.read(cms_id)
        check_revision(expected_revision, current.revision)
        xml_body = self._xml(draft, active=False, with_id=True, cms_id=cms_id)
        path = ("/api/content_management_system/" + cms_id
                + "?output_format=XML&id_shop=" + self.id_shop)
        response = wrap_transport_errors(
            lambda: self.transport(
                "PATCH", path, payload=xml_body,
                headers={"Accept": "application/xml", "Content-Type": "application/xml"}),
            write=True, cms="PrestaShop")
        root = self._expect_xml(response, write=True)
        return self._to_remote(root)

    # ---- Construction XML ------------------------------------------------
    def _xml(self, draft: ArticleDraft, *, active: bool, with_id: bool,
             cms_id: Optional[str] = None) -> bytes:
        parts = ['<?xml version="1.0" encoding="UTF-8"?>', '<prestashop>', '<content>']
        if with_id and cms_id:
            parts.append('<id>' + xml_escape(cms_id) + '</id>')
        parts.append('<id_cms_category>' + xml_escape(self.id_cms_category) + '</id_cms_category>')
        parts.append('<active>' + ("1" if active else "0") + '</active>')
        parts.append('<meta_title>' + self._lang_field(draft.title) + '</meta_title>')
        parts.append('<meta_description>' + self._lang_field(draft.excerpt or "") + '</meta_description>')
        parts.append('<link_rewrite>' + self._lang_field(draft.slug) + '</link_rewrite>')
        parts.append('<content>' + self._lang_field(draft.html) + '</content>')
        parts.append('</content></prestashop>')
        body = ("".join(parts)).encode("utf-8")
        if len(body) > MAX_XML:
            raise AdapterError("Document XML trop volumineux pour PrestaShop.",
                               code="validation")
        return body

    def _xml_activation(self, cms_id: str, *, active: bool) -> bytes:
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<prestashop><content>',
            '<id>' + xml_escape(cms_id) + '</id>',
            '<active>' + ("1" if active else "0") + '</active>',
            '</content></prestashop>',
        ]
        body = "".join(parts).encode("utf-8")
        if len(body) > MAX_XML:
            raise AdapterError("Document XML trop volumineux pour PrestaShop.",
                               code="validation")
        return body

    def _lang_field(self, value: str) -> str:
        return ('<language id="' + xml_escape(self.id_lang) + '">'
                + xml_escape(str(value or "")) + '</language>')

    # ---- Reponse et interpretation --------------------------------------
    @staticmethod
    def _check_id(remote_id: Any) -> str:
        value = str(remote_id or "").strip()
        if not value.isdigit() or len(value) > 10 or int(value) <= 0:
            raise AdapterError("Identifiant PrestaShop attendu : entier positif.",
                               code="validation")
        return value

    @staticmethod
    def _expect_xml(response: TransportResponse, *, write: bool) -> ET.Element:
        status = response.status
        if status in (401, 403):
            raise AdapterError(
                "PrestaShop refuse l'acces. Verifiez la cle Webservice et ses droits.",
                code="auth")
        if status == 404:
            raise AdapterError("PrestaShop : page CMS introuvable.", code="not_found")
        if status == 429:
            raise AdapterError("PrestaShop : trop de requetes.", code="limit")
        if status == 400 or status == 422:
            raise AdapterError(
                "PrestaShop a refuse la valeur ; rien n'a ete enregistre.",
                code="refused")
        if status >= 500:
            raise AdapterError("PrestaShop : erreur serveur.", code="server", uncertain=write)
        if status < 200 or status >= 300:
            raise AdapterError("Reponse PrestaShop inattendue (%s)." % status,
                               code="unreadable", uncertain=write)
        body = response.body
        if len(body) > MAX_XML:
            raise AdapterError("Reponse PrestaShop trop volumineuse.", code="unreadable")
        # ElementTree n'ouvre pas de DTD externe par defaut mais developpe les entites
        # internes : refuser tout DOCTYPE / ENTITY pour eviter les expansions pathologiques
        # (billion laughs) et toute surface XXE residuelle.
        head = body[:4096].lower()
        if b"<!doctype" in head or b"<!entity" in head:
            raise AdapterError(
                "Reponse PrestaShop avec DOCTYPE ou ENTITY refusee.",
                code="unreadable", uncertain=write)
        try:
            root = ET.fromstring(body)
        except (ET.ParseError, DefusedXmlException) as error:
            raise AdapterError("Reponse PrestaShop non lisible en XML.", code="unreadable",
                               uncertain=write) from error
        content = root.find("content")
        if content is None:
            raise AdapterError("PrestaShop n'a pas renvoye d'element <content>.",
                               code="unreadable", uncertain=write)
        return content

    def _to_remote(self, content: ET.Element) -> RemoteArticle:
        id_element = content.find("id")
        if id_element is None or not (id_element.text or "").strip().isdigit():
            raise AdapterError("PrestaShop n'a pas renvoye d'identifiant.", code="unreadable")
        remote_id = (id_element.text or "").strip()
        active_element = content.find("active")
        active = (active_element is not None and (active_element.text or "").strip() == "1")
        status = "published" if active else "draft"
        date_upd = content.find("date_upd")
        revision = ((date_upd.text if date_upd is not None else "") or "").strip()
        if not revision:
            raise AdapterError("PrestaShop n'a pas renvoye date_upd.", code="unreadable")
        return RemoteArticle(id=remote_id, status=status, url="", revision=revision)
