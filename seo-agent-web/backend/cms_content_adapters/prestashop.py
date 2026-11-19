"""Adaptateur PrestaShop Webservice pour les PAGES CMS (ressource `content_management_system`).

Endpoints utilises :
- POST   /api/content_management_system          cree une page CMS (active=0)
- GET    /api/content_management_system/{id}     relit une page CMS
- PATCH  /api/content_management_system/{id}     met a jour une page CMS (partiel)

La methode PATCH est OFFICIELLEMENT prise en charge par le Webservice depuis PrestaShop 8
(fiche des verbes HTTP du Webservice PrestaShop). Les boutiques PrestaShop 1.7 n'exposent
que PUT (fiche entiere a chaque ecriture) ; ce cas n'est pas gere ici : l'adaptateur attend
un Webservice 8.x+ compatible PATCH.

Sources officielles consultees (verifiees le 19/11/2026) :
- https://devdocs.prestashop-project.org/8/webservice/resources/content_management_system/
- https://raw.githubusercontent.com/PrestaShop/PrestaShop/8.2.8/classes/CMS.php

Ces deux sources font autorite sur les champs : la ressource `content_management_system`
expose `id_cms_category`, `position`, `active`, `indexation`, et les champs multilingues
`meta_title`, `head_seo_title`, `meta_description`, `meta_keywords`, `link_rewrite`,
`content`. Aucune colonne `date_add` / `date_upd` n'est declaree sur la classe CMS, et le
schema webservice ne les renvoie pas. L'ancienne implementation en attendait pourtant une
valeur : la revision est maintenant une empreinte SHA-256 des champs reels (voir `_fingerprint`).

Reponse d'ecriture NON autoritative (constate sur boutique 8.2.8 fr/en, 19/11/2026) :
- Le corps renvoye par POST 201 peut echoer `indexation` vide et les variantes langue 2 de
  `meta_title` / `link_rewrite` vides, alors que le GET immediat sur le meme id retourne
  `indexation=0` et le repli langue par defaut dans ces deux champs.
- Un PATCH peut repondre un ORM partiel (champs vides ou absents) qui laisserait croire a
  une mutation, alors que le GET autoritatif montre la ressource preservee.
L'adaptateur relit donc TOUJOURS la ressource via GET apres une ecriture reussie, et c'est
cette relecture qui sert de revision et de base de comparaison. Si la relecture echoue
apres une ecriture reussie, l'erreur est signalee `uncertain=True` et l'identifiant connu
est place dans `details.remote_id` (auth refuse de relecture compris). Aucun reessai
d'ecriture n'est jamais tente.

Limites EXPLICITES :
- Pas d'article de blog : le coeur PrestaShop n'a pas de blog natif universel ; seules les
  pages CMS sont supportees et declarees.
- Pas d'upload de media (`capabilities.upload_images=False`).
- Format d'echange XML, echappe et borne (`MAX_XML`). Pas d'interpolation brute, pas de
  DOCTYPE ni d'ENTITY accepte en reponse.
- Multilingue : les PATCH de l'adaptateur re-envoient TOUTES les langues connues des champs
  touches. Si la relecture GET montre une langue modifiee en dehors de celle visee,
  l'ecriture est signalee comme incertaine. La creation avec un `id_lang` different de la
  langue par defaut de la boutique n'est PAS validee : aucune recopie entre langues n'est
  inventee (voir note PRESTASHOP-CONTRAT-NATIF.md).
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, Mapping, Optional, Tuple
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

# Champs connus de la ressource content_management_system (PrestaShop 8.2).
_SCALAR_FIELDS: Tuple[str, ...] = ("id_cms_category", "position", "active", "indexation")
_LANG_FIELDS: Tuple[str, ...] = (
    "meta_title", "head_seo_title", "meta_description",
    "meta_keywords", "link_rewrite", "content",
)
# Champs multilingues ecrits par l'adaptateur a partir d'un ArticleDraft.
_DRAFT_LANG_FIELDS: Tuple[str, ...] = ("meta_title", "meta_description", "link_rewrite", "content")

# Marqueurs non imprimables utilises comme separateurs dans l'empreinte, pour qu'ils
# n'apparaissent pas naturellement dans le texte d'une page CMS.
_FP_SEP = "\x1f"
_FP_ABSENT = "\x00"


class PrestaShopAdapter:
    platform = "prestashop"
    capabilities = Capabilities(
        create_draft=True, read=True, publish=True, update_draft=True,
        upload_images=False,
        requires=REQUIRED_CONFIG,
        notes=(
            "PrestaShop 8.x Webservice. PAGES CMS uniquement, PAS de blog. "
            "XML echappe et borne. Revision = empreinte SHA-256 des champs CMS "
            "(toutes langues, etat) ; PrestaShop n'expose pas date_upd pour cette ressource. "
            "Reponse d'ecriture non autoritative : l'adaptateur relit la ressource via GET "
            "apres chaque ecriture reussie et c'est ce GET qui decide. "
            "Multilingue : chaque PATCH re-envoie toutes les langues des champs touches. "
            "Creation hors langue par defaut de la boutique non validee."
        ),
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
        xml_body = self._xml_create(draft)
        path = "/api/content_management_system?output_format=XML&id_shop=" + self.id_shop
        response = wrap_transport_errors(
            lambda: self.transport(
                "POST", path, payload=xml_body,
                headers={"Accept": "application/xml", "Content-Type": "application/xml"}),
            write=True, cms="PrestaShop")
        posted = self._expect_xml(response, write=True)
        remote_id = self._extract_id(posted, remote_id_hint=None)
        # La reponse POST n'est pas autoritative (indexation vide, variantes langue 2
        # vides observees sur 8.2.8). On relit l'etat applique et c'est lui qui definit
        # la revision renvoyee.
        snapshot = self._read_after_write(remote_id)
        return self._to_remote(snapshot)

    def read(self, remote_id: str) -> RemoteArticle:
        cms_id = self._check_id(remote_id)
        content = self._read_content(cms_id, write=False)
        return self._to_remote(content)

    def publish(self, remote_id: str, expected_revision: Optional[str] = None) -> RemoteArticle:
        cms_id = self._check_id(remote_id)
        current = self._read_content(cms_id, write=False)
        check_revision(expected_revision, self._fingerprint(current))
        xml_body = self._xml_activation(cms_id, active=True)
        path = ("/api/content_management_system/" + cms_id
                + "?output_format=XML&id_shop=" + self.id_shop)
        response = wrap_transport_errors(
            lambda: self.transport(
                "PATCH", path, payload=xml_body,
                headers={"Accept": "application/xml", "Content-Type": "application/xml"}),
            write=True, cms="PrestaShop")
        patched = self._expect_xml(response, write=True)
        self._extract_id(patched, remote_id_hint=cms_id)
        # Reponse PATCH non autoritative : la verification de preservation et l'etat final
        # sont decides sur un GET autoritatif, pas sur l'ORM partiel echoe par PATCH.
        snapshot = self._read_after_write(cms_id)
        if self._active_text(snapshot) != "1":
            raise AdapterError(
                "PrestaShop n'a pas confirme l'activation de la page CMS.",
                code="unreadable", uncertain=True, details={"remote_id": cms_id})
        # L'activation ne doit toucher que le champ `active` : si le GET montre un texte
        # reecrit (conversion de "\n" literaux signalee par l'ancien labo 8.2.8), on refuse.
        self._verify_fields_preserved(current, snapshot, allowed_changed=("active",),
                                      remote_id=cms_id)
        return self._to_remote(snapshot)

    def update_draft(self, remote_id: str, draft: ArticleDraft,
                     expected_revision: Optional[str] = None) -> RemoteArticle:
        require_draft_fields(draft)
        validate_html(draft.html)
        cms_id = self._check_id(remote_id)
        current = self._read_content(cms_id, write=False)
        check_revision(expected_revision, self._fingerprint(current))
        xml_body = self._xml_update(draft, current, cms_id)
        path = ("/api/content_management_system/" + cms_id
                + "?output_format=XML&id_shop=" + self.id_shop)
        response = wrap_transport_errors(
            lambda: self.transport(
                "PATCH", path, payload=xml_body,
                headers={"Accept": "application/xml", "Content-Type": "application/xml"}),
            write=True, cms="PrestaShop")
        patched = self._expect_xml(response, write=True)
        self._extract_id(patched, remote_id_hint=cms_id)
        snapshot = self._read_after_write(cms_id)
        # update_draft ne publie pas : l'etat `active` doit rester inchange cote GET.
        if self._active_text(snapshot) != self._active_text(current):
            raise AdapterError(
                "PrestaShop a modifie l'etat de publication durant la mise a jour.",
                code="unreadable", uncertain=True, details={"remote_id": cms_id})
        # Toutes les langues NON visees doivent rester byte-a-byte identiques dans le GET.
        self._verify_other_languages_preserved(current, snapshot, remote_id=cms_id)
        return self._to_remote(snapshot)

    # ---- Lecture ---------------------------------------------------------
    def _read_content(self, cms_id: str, *, write: bool) -> ET.Element:
        path = ("/api/content_management_system/" + cms_id
                + "?output_format=XML&id_shop=" + self.id_shop)
        response = wrap_transport_errors(
            lambda: self.transport("GET", path, headers={"Accept": "application/xml"}),
            write=write, cms="PrestaShop")
        return self._expect_xml(response, write=write)

    def _read_after_write(self, remote_id: str) -> ET.Element:
        """Relecture autoritative apres une ecriture reussie. Toute erreur devient
        `uncertain=True` et `details.remote_id` porte l'identifiant connu, y compris
        un refus auth au moment de la relecture : l'ecriture a pu aboutir et l'appelant
        doit verifier cote CMS. Aucun reessai d'ecriture n'est tente."""
        try:
            return self._read_content(remote_id, write=True)
        except AdapterError as error:
            details = dict(error.details or {})
            details.setdefault("remote_id", remote_id)
            raise AdapterError(
                str(error) or "Relecture PrestaShop impossible apres ecriture.",
                code=error.code or "unreadable",
                uncertain=True,
                details=details,
            ) from error

    def _extract_id(self, content: ET.Element, *, remote_id_hint: Optional[str]) -> str:
        """Lit `<id>` dans un corps de reponse d'ecriture et verifie, pour les mises a
        jour, que l'identifiant renvoye correspond a celui envoye. En cas d'echec,
        l'ecriture est signalee `uncertain=True` (le POST/PATCH a pu aboutir malgre une
        reponse malformee)."""
        id_element = content.find("id")
        text = ((id_element.text if id_element is not None else "") or "").strip()
        if not text.isdigit() or int(text) <= 0 or len(text) > 10:
            details = {"remote_id": remote_id_hint} if remote_id_hint else {}
            raise AdapterError(
                "PrestaShop n'a pas renvoye d'identifiant exploitable apres ecriture.",
                code="unreadable", uncertain=True, details=details)
        if remote_id_hint is not None and text != remote_id_hint:
            raise AdapterError(
                "PrestaShop a renvoye un identifiant different de celui ecrit.",
                code="unreadable", uncertain=True,
                details={"remote_id": remote_id_hint, "returned_id": text})
        return text

    # ---- Empreinte deterministe -----------------------------------------
    @staticmethod
    def _fingerprint(content: ET.Element) -> str:
        """Empreinte SHA-256 des champs CMS pertinents, independante de l'ordre XML.

        Champs scalaires et multilingues sont serialises dans un ordre fixe ;
        les langues sont triees par identifiant numerique ; chaque texte est pris verbatim
        (espaces et sauts de ligne conserves). Un champ absent est marque `\\x00` pour
        qu'une disparition n'ait pas le meme hash qu'une valeur vide.
        """
        parts = []
        for name in _SCALAR_FIELDS:
            elem = content.find(name)
            if elem is None:
                parts.append(name + _FP_SEP + _FP_ABSENT)
            else:
                parts.append(name + _FP_SEP + (elem.text or ""))
        for name in _LANG_FIELDS:
            elem = content.find(name)
            if elem is None:
                parts.append(name + _FP_SEP + _FP_ABSENT)
                continue
            entries = []
            for lang in elem.findall("language"):
                lid = (lang.get("id") or "").strip()
                entries.append((lid, lang.text or ""))
            entries.sort(key=lambda item: (
                0 if item[0].isdigit() else 1,
                int(item[0]) if item[0].isdigit() else 0,
                item[0]))
            if not entries:
                parts.append(name + _FP_SEP + "(empty)")
                continue
            for lid, text in entries:
                parts.append(name + _FP_SEP + lid + _FP_SEP + text)
        payload = "\n".join(parts).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    # ---- Construction XML ------------------------------------------------
    def _xml_create(self, draft: ArticleDraft) -> bytes:
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<prestashop><content>',
            '<id_cms_category>' + xml_escape(self.id_cms_category) + '</id_cms_category>',
            '<active>0</active>',
            self._xml_lang_field_new("meta_title", draft.title),
            self._xml_lang_field_new("meta_description", draft.excerpt or ""),
            self._xml_lang_field_new("link_rewrite", draft.slug),
            self._xml_lang_field_new("content", draft.html),
            '</content></prestashop>',
        ]
        return self._finalize_body(parts)

    def _xml_update(self, draft: ArticleDraft, current: ET.Element, cms_id: str) -> bytes:
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<prestashop><content>',
            '<id>' + xml_escape(cms_id) + '</id>',
        ]
        id_cat = (current.findtext("id_cms_category") or self.id_cms_category).strip()
        parts.append('<id_cms_category>' + xml_escape(id_cat) + '</id_cms_category>')
        # Preserver active : update_draft ne publie pas.
        parts.append('<active>' + xml_escape(self._active_text(current) or "0") + '</active>')
        for name in ("position", "indexation"):
            elem = current.find(name)
            if elem is not None and elem.text is not None:
                parts.append('<' + name + '>' + xml_escape(elem.text) + '</' + name + '>')
        drafted = {
            "meta_title": draft.title,
            "meta_description": draft.excerpt or "",
            "link_rewrite": draft.slug,
            "content": draft.html,
        }
        for name in _DRAFT_LANG_FIELDS:
            parts.append(self._xml_lang_field_merged(name, current, drafted[name]))
        # Preserver tels quels les champs multilingues non touches par le brouillon.
        for name in _LANG_FIELDS:
            if name in _DRAFT_LANG_FIELDS:
                continue
            elem = current.find(name)
            if elem is not None:
                parts.append(self._xml_lang_field_copy(name, elem))
        parts.append('</content></prestashop>')
        return self._finalize_body(parts)

    def _xml_activation(self, cms_id: str, *, active: bool) -> bytes:
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<prestashop><content>',
            '<id>' + xml_escape(cms_id) + '</id>',
            '<active>' + ("1" if active else "0") + '</active>',
            '</content></prestashop>',
        ]
        return self._finalize_body(parts)

    def _finalize_body(self, parts) -> bytes:
        body = "".join(parts).encode("utf-8")
        if len(body) > MAX_XML:
            raise AdapterError("Document XML trop volumineux pour PrestaShop.",
                               code="validation")
        return body

    def _xml_lang_field_new(self, name: str, value: str) -> str:
        """Champ multilingue pour la creation : seule la langue configuree est fournie."""
        return ('<' + name + '><language id="' + xml_escape(self.id_lang) + '">'
                + xml_escape(str(value or ""))
                + '</language></' + name + '>')

    def _xml_lang_field_merged(self, name: str, current: ET.Element, new_value: str) -> str:
        """Pour l'id_lang configure : la valeur du brouillon. Pour les autres : la valeur
        exacte retournee par PrestaShop. Envoyer toutes les langues evite qu'un PATCH
        avec une seule langue efface les autres (constat de l'ancien labo 8.2.8)."""
        parts = ['<' + name + '>']
        elem = current.find(name)
        found_target = False
        seen = set()
        if elem is not None:
            for lang in elem.findall("language"):
                lid = (lang.get("id") or "").strip()
                if not _ID_LANG.match(lid) or lid in seen:
                    continue
                seen.add(lid)
                if lid == self.id_lang:
                    text = xml_escape(str(new_value or ""))
                    found_target = True
                else:
                    text = xml_escape(lang.text or "")
                parts.append('<language id="' + xml_escape(lid) + '">' + text + '</language>')
        if not found_target:
            parts.append('<language id="' + xml_escape(self.id_lang) + '">'
                         + xml_escape(str(new_value or "")) + '</language>')
        parts.append('</' + name + '>')
        return "".join(parts)

    def _xml_lang_field_copy(self, name: str, elem: ET.Element) -> str:
        parts = ['<' + name + '>']
        seen = set()
        for lang in elem.findall("language"):
            lid = (lang.get("id") or "").strip()
            if not _ID_LANG.match(lid) or lid in seen:
                continue
            seen.add(lid)
            parts.append('<language id="' + xml_escape(lid) + '">'
                         + xml_escape(lang.text or "") + '</language>')
        parts.append('</' + name + '>')
        return "".join(parts)

    # ---- Verifications apres ecriture -----------------------------------
    @staticmethod
    def _active_text(content: ET.Element) -> str:
        element = content.find("active")
        if element is None:
            return ""
        return (element.text or "").strip()

    @staticmethod
    def _lang_text_map(elem: Optional[ET.Element]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        if elem is None:
            return result
        for lang in elem.findall("language"):
            lid = (lang.get("id") or "").strip()
            if not lid:
                continue
            result.setdefault(lid, lang.text or "")
        return result

    def _verify_fields_preserved(self, before: ET.Element, after: ET.Element,
                                 *, allowed_changed: Tuple[str, ...],
                                 remote_id: Optional[str] = None) -> None:
        for name in _SCALAR_FIELDS:
            if name in allowed_changed:
                continue
            before_text = (before.findtext(name) or "").strip()
            after_text = (after.findtext(name) or "").strip()
            if before_text != after_text:
                raise AdapterError(
                    "PrestaShop a modifie un champ non cible durant l'ecriture.",
                    code="unreadable", uncertain=True,
                    details=self._details(remote_id, field=name))
        for name in _LANG_FIELDS:
            before_map = self._lang_text_map(before.find(name))
            after_map = self._lang_text_map(after.find(name))
            for lid in set(before_map) | set(after_map):
                if before_map.get(lid, "") != after_map.get(lid, ""):
                    raise AdapterError(
                        "PrestaShop a modifie le texte d'un champ multilingue durant l'ecriture.",
                        code="unreadable", uncertain=True,
                        details=self._details(remote_id, field=name, language=lid))

    def _verify_other_languages_preserved(self, before: ET.Element, after: ET.Element,
                                          *, remote_id: Optional[str] = None) -> None:
        """Toutes les langues NON ciblees par l'id_lang configure doivent rester identiques,
        pour chacun des champs multilingues. Les champs non touches par le brouillon doivent
        rester inchanges pour toutes les langues."""
        for name in _LANG_FIELDS:
            before_map = self._lang_text_map(before.find(name))
            after_map = self._lang_text_map(after.find(name))
            for lid in set(before_map) | set(after_map):
                if name in _DRAFT_LANG_FIELDS and lid == self.id_lang:
                    continue
                if before_map.get(lid, "") != after_map.get(lid, ""):
                    raise AdapterError(
                        "PrestaShop a modifie une langue non ciblee durant la mise a jour.",
                        code="unreadable", uncertain=True,
                        details=self._details(remote_id, field=name, language=lid))

    @staticmethod
    def _details(remote_id: Optional[str], **extra) -> Dict[str, str]:
        details: Dict[str, str] = {}
        if remote_id is not None:
            details["remote_id"] = remote_id
        details.update({k: v for k, v in extra.items() if v is not None})
        return details

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
        # defusedxml bloque les entites externes mais developpe les entites internes :
        # refuser tout DOCTYPE / ENTITY en entree evite les expansions pathologiques.
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
        status = "published" if self._active_text(content) == "1" else "draft"
        revision = self._fingerprint(content)
        return RemoteArticle(id=remote_id, status=status, url="", revision=revision)
