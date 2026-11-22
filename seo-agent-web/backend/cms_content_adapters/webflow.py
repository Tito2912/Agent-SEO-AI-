"""Adaptateur Webflow Data API v2 pour une collection editoriale (API officielle du 17/11/2026).

Endpoints utilises :
- POST   /v2/collections/{collection_id}/items/insert   cree un item en BROUILLON (staged)
- GET    /v2/collections/{collection_id}/items/{id}     relit un item
- PATCH  /v2/collections/{collection_id}/items/{id}     met a jour les champs
- POST   /v2/collections/{collection_id}/items/publish  publie des items en masse

Points durs :
- Il n'existe PAS de collection "blog" universelle : la configuration doit fournir
  `collection_id` et `field_mapping` (quel champ CMS porte le titre, le body, le slug,
  l'excerpt, l'auteur). Sans mapping explicite : refus.
- Le nouveau endpoint `/items/insert` attend des `locales` (pluriel) et un `fieldData`.
- La publication est SEPAREE : apres publication, on verifie `erroredItemIds` ; une erreur
  partielle est une erreur, pas un succes.
- Revision : Webflow renvoie `lastUpdated` ; on l'utilise comme jeton.
"""
from __future__ import annotations

from typing import Any, Mapping, Optional
from urllib.parse import quote

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


REQUIRED_CONFIG = ("collection_id", "field_mapping")
REQUIRED_MAPPING = ("title", "slug", "body")


class WebflowAdapter:
    platform = "webflow"
    capabilities = Capabilities(
        create_draft=True, read=True, publish=True, update_draft=True,
        upload_images=False,
        requires=REQUIRED_CONFIG,
        notes=("Webflow Data API v2. collection_id et field_mapping explicites requis. "
               "La publication est un POST separe ; une erreur partielle est signalee."),
    )

    def __init__(self, *, transport: TransportFn, config: Mapping[str, Any]):
        require_config(config, REQUIRED_CONFIG)
        self.transport = transport
        self.config = dict(config)
        self.collection_id = self._check_id(self.config["collection_id"], kind="collection_id")
        mapping = self.config.get("field_mapping") or {}
        if not isinstance(mapping, Mapping):
            raise AdapterError("field_mapping doit etre un dictionnaire.", code="mapping")
        missing = [name for name in REQUIRED_MAPPING if not str(mapping.get(name) or "").strip()]
        if missing:
            raise AdapterError(
                "field_mapping incomplet : " + ", ".join(missing),
                code="mapping", details={"missing": missing})
        self.field_mapping = {k: str(v).strip() for k, v in mapping.items() if str(v).strip()}
        self.locales = tuple(self.config.get("locales") or ())  # peut rester vide (langue principale)

    # ---- Operations publiques --------------------------------------------
    def validate_draft(self, draft: ArticleDraft) -> None:
        """Validate the draft without network access or a media transfer."""
        require_draft_fields(draft)
        validate_html(draft.html)

    def create_draft(self, draft: ArticleDraft) -> RemoteArticle:
        self.validate_draft(draft)
        require_draft_fields(draft)
        validate_html(draft.html)
        payload: dict = {"items": [{"fieldData": self._field_data(draft)}]}
        if self.locales:
            payload["locales"] = list(self.locales)
        path = "/v2/collections/" + quote(self.collection_id, safe="") + "/items/insert"
        response = wrap_transport_errors(
            lambda: self.transport("POST", path, payload=payload,
                                   headers={"Accept": "application/json"}),
            write=True, cms="Webflow")
        data = self._expect_ok(response, write=True)
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
            raise AdapterError("Webflow n'a pas renvoye un unique item.", code="unreadable",
                               uncertain=True)
        return self._to_remote(items[0], status="draft")

    def read(self, remote_id: str) -> RemoteArticle:
        item_id = self._check_id(remote_id, kind="item_id")
        path = "/v2/collections/" + quote(self.collection_id, safe="") + "/items/" + quote(item_id, safe="")
        response = wrap_transport_errors(
            lambda: self.transport("GET", path, headers={"Accept": "application/json"}),
            write=False, cms="Webflow")
        data = self._expect_ok(response, write=False)
        return self._to_remote(data, status=None)

    def publish(self, remote_id: str, expected_revision: Optional[str] = None) -> RemoteArticle:
        item_id = self._check_id(remote_id, kind="item_id")
        current = self.read(item_id)
        check_revision(expected_revision, current.revision)
        path = "/v2/collections/" + quote(self.collection_id, safe="") + "/items/publish"
        payload = {"itemIds": [item_id]}
        response = wrap_transport_errors(
            lambda: self.transport("POST", path, payload=payload,
                                   headers={"Accept": "application/json"}),
            write=True, cms="Webflow")
        data = self._expect_ok(response, write=True)
        errored = data.get("errors") if isinstance(data, dict) else None
        if errored:
            raise AdapterError(
                "Webflow a renvoye des erreurs de publication partielles.",
                code="partial", uncertain=True, details={"errors": errored})
        published = data.get("publishedItemIds") if isinstance(data, dict) else None
        if not isinstance(published, list) or item_id not in [str(x) for x in published]:
            raise AdapterError(
                "Webflow n'a pas confirme la publication de l'item.",
                code="partial", uncertain=True)
        # Webflow renvoie `lastPublished` uniquement en relecture complete ; on relit.
        return self.read(item_id)

    def update_draft(self, remote_id: str, draft: ArticleDraft,
                     expected_revision: Optional[str] = None) -> RemoteArticle:
        require_draft_fields(draft)
        validate_html(draft.html)
        item_id = self._check_id(remote_id, kind="item_id")
        current = self.read(item_id)
        check_revision(expected_revision, current.revision)
        path = "/v2/collections/" + quote(self.collection_id, safe="") + "/items/" + quote(item_id, safe="")
        payload = {"fieldData": self._field_data(draft)}
        response = wrap_transport_errors(
            lambda: self.transport("PATCH", path, payload=payload,
                                   headers={"Accept": "application/json"}),
            write=True, cms="Webflow")
        data = self._expect_ok(response, write=True)
        return self._to_remote(data, status="draft")

    # ---- Helpers ---------------------------------------------------------
    def _field_data(self, draft: ArticleDraft) -> dict:
        mapping = self.field_mapping
        fields = {
            mapping["title"]: draft.title,
            mapping["slug"]: draft.slug,
            mapping["body"]: draft.html,
        }
        if "excerpt" in mapping:
            fields[mapping["excerpt"]] = draft.excerpt or ""
        if "author" in mapping:
            if not draft.author:
                raise AdapterError(
                    "Le champ auteur est declare dans field_mapping mais vide dans le brouillon.",
                    code="author")
            fields[mapping["author"]] = draft.author
        return fields

    @staticmethod
    def _check_id(remote_id: Any, *, kind: str) -> str:
        value = str(remote_id or "").strip()
        # Les identifiants Webflow sont alphanumeriques avec tirets.
        if not value or len(value) > 64 or not all(ch.isalnum() or ch in {"-", "_"} for ch in value):
            raise AdapterError(
                "Identifiant Webflow attendu (%s) : alphanumerique." % kind,
                code="validation")
        return value

    @staticmethod
    def _expect_ok(response: TransportResponse, *, write: bool) -> dict:
        status = response.status
        if status in (401, 403):
            raise AdapterError("Webflow refuse l'acces. Verifiez le jeton et ses scopes.",
                               code="auth")
        if status == 404:
            raise AdapterError("Webflow : item introuvable.", code="not_found")
        if status == 409:
            raise AdapterError("Webflow signale un conflit.", code="conflict")
        if status == 429:
            raise AdapterError("Webflow : trop de requetes.", code="limit")
        if status == 400:
            raise AdapterError("Webflow a refuse la valeur ; rien n'a ete enregistre.",
                               code="refused")
        if status >= 500:
            raise AdapterError("Webflow : erreur serveur.", code="server", uncertain=write)
        if status < 200 or status >= 300:
            raise AdapterError("Reponse Webflow inattendue (%s)." % status,
                               code="unreadable", uncertain=write)
        data = response.json()
        if not isinstance(data, dict):
            raise AdapterError("Reponse Webflow non lisible.", code="unreadable",
                               uncertain=write)
        return data

    @staticmethod
    def _to_remote(item: dict, *, status: Optional[str]) -> RemoteArticle:
        remote_id = str(item.get("id") or "").strip()
        if not remote_id:
            raise AdapterError("Webflow n'a pas renvoye d'identifiant.", code="unreadable")
        last_updated = str(item.get("lastUpdated") or item.get("updatedOn") or "")
        if not last_updated:
            raise AdapterError("Webflow n'a pas renvoye lastUpdated.", code="unreadable")
        if status is None:
            # Webflow distingue `lastPublished` de `lastUpdated` ; sans publication, status=draft.
            status = "published" if item.get("lastPublished") and not item.get("isDraft") else "draft"
        return RemoteArticle(id=remote_id, status=status, url="", revision=last_updated)
