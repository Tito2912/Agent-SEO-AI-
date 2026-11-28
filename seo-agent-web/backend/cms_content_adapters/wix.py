"""Adaptateur Wix Blog v3 Draft Posts (API officielle du 17/11/2026).

Endpoints utilises :
- POST  /blog/v3/draft-posts                    cree un draft (publish=false)
- GET   /blog/v3/draft-posts/{draft_post_id}    relit un draft
- PATCH /blog/v3/draft-posts/{draft_post_id}    met a jour un draft
- POST  /blog/v3/draft-posts/{draft_post_id}/publish  publie le draft

Points durs :
- `richContent` est Ricos (JSON), PAS du HTML arbitraire : la conversion est faite ici via
  `ricos.html_to_ricos`, qui refuse les balises non supportees (pas de perte silencieuse).
- `memberId` (auteur) est OBLIGATOIRE pour une application tierce ; sans auteur -> refus.
- Permission Wix : "Manage Blog".
- Note du 17/11/2026 : l'ancienne note Noyaru affirmait Wix Blog non ecrivable ; cette API
  v3 permet bien l'ecriture de drafts. Un article de blog n'etait pas modifiable via l'API
  SEO Tags (04/11/2026), mais via Draft Posts v3 oui.
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
    require_author,
    wrap_transport_errors,
)
from .ricos import html_to_ricos


REQUIRED_CONFIG = ()


class WixAdapter:
    platform = "wix"
    capabilities = Capabilities(
        create_draft=True, read=True, publish=True, update_draft=True,
        upload_images=False,
        requires=REQUIRED_CONFIG,
        notes=("Wix Blog v3 Draft Posts. Ricos JSON (sous-ensemble explicite). "
               "memberId auteur requis. Permission : Manage Blog."),
    )

    def __init__(self, *, transport: TransportFn, config: Mapping[str, Any]):
        require_config(config, REQUIRED_CONFIG)
        self.transport = transport
        self.config = dict(config or {})

    # ---- Operations publiques --------------------------------------------
    def validate_draft(self, draft: ArticleDraft) -> None:
        """Validate the draft without network access or a media transfer."""
        require_draft_fields(draft)
        require_author(draft, "Wix")
        html_to_ricos(draft.html)

    def create_draft(self, draft: ArticleDraft) -> RemoteArticle:
        self.validate_draft(draft)
        require_draft_fields(draft)
        require_author(draft, "Wix")
        rich_content = html_to_ricos(draft.html)
        payload = {
            "draftPost": {
                "title": draft.title,
                "slug": draft.slug,
                "memberId": draft.author,
                "excerpt": draft.excerpt or "",
                "richContent": rich_content,
                "language": draft.language or "fr",
            },
            # Jamais de publication cachee a la creation.
            "publish": False,
        }
        response = wrap_transport_errors(
            lambda: self.transport("POST", "/blog/v3/draft-posts", payload=payload,
                                   headers={"Accept": "application/json"}),
            write=True, cms="Wix")
        data = self._expect_ok(response, write=True)
        return self._to_remote(self._extract(data), status="draft")

    def read(self, remote_id: str) -> RemoteArticle:
        draft_id = self._check_id(remote_id)
        path = "/blog/v3/draft-posts/" + quote(draft_id, safe="")
        response = wrap_transport_errors(
            lambda: self.transport("GET", path, headers={"Accept": "application/json"}),
            write=False, cms="Wix")
        data = self._expect_ok(response, write=False)
        return self._to_remote(self._extract(data), status=None)

    def publish(self, remote_id: str, expected_revision: Optional[str] = None) -> RemoteArticle:
        draft_id = self._check_id(remote_id)
        current = self.read(draft_id)
        check_revision(expected_revision, current.revision)
        path = "/blog/v3/draft-posts/" + quote(draft_id, safe="") + "/publish"
        response = wrap_transport_errors(
            lambda: self.transport("POST", path, payload={}, headers={"Accept": "application/json"}),
            write=True, cms="Wix")
        data = self._expect_ok(response, write=True)
        post = self._extract(data) or {}
        if not post:
            # L'endpoint publish peut ne renvoyer qu'un identifiant de post publie.
            return self._to_remote({"id": draft_id, "revision": current.revision}, status="published")
        return self._to_remote(post, status="published")

    def read_created_draft(self, remote_id: str, draft: ArticleDraft) -> RemoteArticle:
        path = "/blog/v3/draft-posts/" + quote(self._check_id(remote_id), safe="") + "?fieldsets=RICH_CONTENT"
        response = wrap_transport_errors(
            lambda: self.transport("GET", path, headers={"Accept": "application/json"}),
            write=False, cms="Wix")
        post = self._extract(self._expect_ok(response, write=False)) or {}
        remote = self._to_remote(post, status=None)

        def content(value):
            if isinstance(value, list):
                return [content(item) for item in value]
            if isinstance(value, dict):
                return {k: content(v) for k, v in value.items()
                        if not (k == "id" and "type" in value and "nodes" in value)}
            return value

        if (remote.id != remote_id or remote.status != "draft" or post.get("title") != draft.title
                or post.get("memberId") != draft.author or post.get("language") != (draft.language or "fr")
                or (post.get("excerpt") or "") != (draft.excerpt or "")
                or content((post.get("richContent") or {}).get("nodes")) != content(html_to_ricos(draft.html)["nodes"])):
            raise AdapterError("Le brouillon Wix diffère de l’aperçu.", code="remote_changed")
        return remote

    def update_draft(self, remote_id: str, draft: ArticleDraft,
                     expected_revision: Optional[str] = None) -> RemoteArticle:
        require_draft_fields(draft)
        require_author(draft, "Wix")
        rich_content = html_to_ricos(draft.html)
        draft_id = self._check_id(remote_id)
        current = self.read(draft_id)
        check_revision(expected_revision, current.revision)
        path = "/blog/v3/draft-posts/" + quote(draft_id, safe="")
        payload = {
            "draftPost": {
                "id": draft_id,
                **({} if current.revision.startswith("edited:") else {"revision": current.revision}),
                "title": draft.title,
                "slug": draft.slug,
                "memberId": draft.author,
                "excerpt": draft.excerpt or "",
                "richContent": rich_content,
                "language": draft.language or "fr",
            },
            "fieldMask": {"paths": ["title", "slug", "memberId", "excerpt", "richContent", "language"]},
        }
        response = wrap_transport_errors(
            lambda: self.transport("PATCH", path, payload=payload,
                                   headers={"Accept": "application/json"}),
            write=True, cms="Wix")
        data = self._expect_ok(response, write=True)
        return self._to_remote(self._extract(data), status="draft")

    # ---- Helpers ---------------------------------------------------------
    @staticmethod
    def _check_id(remote_id: Any) -> str:
        value = str(remote_id or "").strip()
        # Les identifiants Wix sont des UUID ou alphanumeriques avec tirets.
        if not value or len(value) > 64 or not all(ch.isalnum() or ch in {"-", "_"} for ch in value):
            raise AdapterError("Identifiant Wix attendu : alphanumerique.", code="validation")
        return value

    @staticmethod
    def _extract(data: Any) -> Optional[dict]:
        if not isinstance(data, dict):
            return None
        # Create : {"draftPost": {...}}  Update : {"draftPost": {...}}  Publish : {"post": {...}}
        for key in ("draftPost", "post"):
            value = data.get(key)
            if isinstance(value, dict):
                return value
        return None

    @staticmethod
    def _expect_ok(response: TransportResponse, *, write: bool) -> dict:
        status = response.status
        if status in (401, 403):
            raise AdapterError(
                "Wix refuse l'acces. Verifiez la cle API et la permission Manage Blog.",
                code="auth")
        if status == 404:
            raise AdapterError("Wix : draft introuvable.", code="not_found")
        if status == 409:
            raise AdapterError("Wix signale un conflit de revision.", code="conflict")
        if status == 429:
            raise AdapterError("Wix : trop de requetes.", code="limit")
        if status == 400:
            raise AdapterError("Wix a refuse la valeur ; rien n'a ete enregistre.",
                               code="refused")
        if status >= 500:
            raise AdapterError("Wix : erreur serveur.", code="server", uncertain=write)
        if status < 200 or status >= 300:
            raise AdapterError("Reponse Wix inattendue (%s)." % status,
                               code="unreadable", uncertain=write)
        data = response.json()
        if not isinstance(data, dict):
            raise AdapterError("Reponse Wix non lisible.", code="unreadable",
                               uncertain=write)
        return data

    @staticmethod
    def _to_remote(post: Optional[dict], *, status: Optional[str]) -> RemoteArticle:
        if not isinstance(post, dict) or not post.get("id"):
            raise AdapterError("Wix n'a pas renvoye de draft exploitable.", code="unreadable")
        remote_id = str(post["id"])
        revision = str(post.get("revision") or "")
        if not revision and post.get("editedDate"):
            revision = "edited:" + str(post["editedDate"])
        if not revision:
            raise AdapterError("Wix n'a pas renvoye de revision.", code="unreadable")
        url = str(post.get("url") or "")
        if post.get("status"):
            status = {"UNPUBLISHED": "draft", "PUBLISHED": "published"}.get(post["status"], "unknown")
        elif status is None:
            status = "published" if post.get("published") else "draft"
        return RemoteArticle(id=remote_id, status=status, url=url, revision=revision)
