"""Adaptateur Ghost Admin API (posts, source=html).

Endpoints utilises (API officielle du 17/11/2026) :
- POST /ghost/api/admin/posts/?source=html        cree un post (status='draft' par defaut)
- GET  /ghost/api/admin/posts/{id}/?formats=html  relit un post
- PUT  /ghost/api/admin/posts/{id}/?source=html   met a jour (updated_at obligatoire)

Points durs :
- `updated_at` lu doit etre renvoye tel quel a la mise a jour (Ghost renvoie 422 sans).
- Le header `Accept-Version` est systematique (>= v5.0).
- JWT HS256 court (quelques minutes) prepare par le transport injecte : rien n'est signe ici.
- On n'envoie PAS la newsletter : `send_email_when_published` reste a `false` ; aucune
  option `email_segment` n'est transmise.
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


ADMIN_API_VERSION = "v5.0"
REQUIRED_CONFIG = ()


class GhostAdapter:
    platform = "ghost"
    capabilities = Capabilities(
        create_draft=True, read=True, publish=True, update_draft=True,
        upload_images=False,
        requires=REQUIRED_CONFIG,
        notes=("Ghost Admin >= 5.0. Le transport injecte le JWT court et Accept-Version. "
               "Aucune newsletter n'est envoyee par cet adaptateur."),
    )

    def __init__(self, *, transport: TransportFn, config: Mapping[str, Any]):
        require_config(config, REQUIRED_CONFIG)
        self.transport = transport
        self.config = dict(config or {})

    # ---- Operations publiques --------------------------------------------
    def create_draft(self, draft: ArticleDraft) -> RemoteArticle:
        require_draft_fields(draft)
        validate_html(draft.html)
        body = {"posts": [self._payload(draft, updated_at=None, status="draft")]}
        response = wrap_transport_errors(
            lambda: self.transport(
                "POST", "/ghost/api/admin/posts/?source=html",
                payload=body,
                headers={"Accept": "application/json", "Accept-Version": ADMIN_API_VERSION}),
            write=True, cms="Ghost")
        return self._first_post(self._expect_ok(response, write=True))

    def read(self, remote_id: str) -> RemoteArticle:
        path = "/ghost/api/admin/posts/" + quote(self._check_id(remote_id), safe="") + "/?formats=html"
        response = wrap_transport_errors(
            lambda: self.transport(
                "GET", path,
                headers={"Accept": "application/json", "Accept-Version": ADMIN_API_VERSION}),
            write=False, cms="Ghost")
        return self._first_post(self._expect_ok(response, write=False))

    def publish(self, remote_id: str, expected_revision: Optional[str] = None) -> RemoteArticle:
        current = self.read(remote_id)
        check_revision(expected_revision, current.revision)
        body = {"posts": [{"status": "published", "updated_at": current.revision}]}
        path = "/ghost/api/admin/posts/" + quote(self._check_id(remote_id), safe="") + "/?source=html"
        response = wrap_transport_errors(
            lambda: self.transport(
                "PUT", path, payload=body,
                headers={"Accept": "application/json", "Accept-Version": ADMIN_API_VERSION}),
            write=True, cms="Ghost")
        return self._first_post(self._expect_ok(response, write=True))

    def update_draft(self, remote_id: str, draft: ArticleDraft,
                     expected_revision: Optional[str] = None) -> RemoteArticle:
        require_draft_fields(draft)
        validate_html(draft.html)
        current = self.read(remote_id)
        check_revision(expected_revision, current.revision)
        body = {"posts": [self._payload(draft, updated_at=current.revision, status=None)]}
        path = "/ghost/api/admin/posts/" + quote(self._check_id(remote_id), safe="") + "/?source=html"
        response = wrap_transport_errors(
            lambda: self.transport(
                "PUT", path, payload=body,
                headers={"Accept": "application/json", "Accept-Version": ADMIN_API_VERSION}),
            write=True, cms="Ghost")
        return self._first_post(self._expect_ok(response, write=True))

    # ---- Helpers ---------------------------------------------------------
    @staticmethod
    def _payload(draft: ArticleDraft, *, updated_at: Optional[str], status: Optional[str]) -> dict:
        payload = {
            "title": draft.title,
            "slug": draft.slug,
            "html": draft.html,
            "custom_excerpt": (draft.excerpt or "")[:300] or None,
            # Jamais de newsletter automatique.
            "send_email_when_published": False,
        }
        if draft.author:
            # L'auteur Ghost est l'email ou l'id du membre du staff ; sans mapping cote
            # Codex, on refuse d'inventer un identifiant.
            if "@" in draft.author:
                payload["authors"] = [{"email": draft.author}]
            else:
                payload["authors"] = [{"id": draft.author}]
        if status is not None:
            payload["status"] = status
        if updated_at is not None:
            payload["updated_at"] = updated_at
        return payload

    @staticmethod
    def _check_id(remote_id: Any) -> str:
        value = str(remote_id or "").strip()
        if not value or len(value) > 64 or not all(ch.isalnum() or ch in {"-", "_"} for ch in value):
            raise AdapterError("Identifiant Ghost attendu : alphanumerique.", code="validation")
        return value

    @staticmethod
    def _expect_ok(response: TransportResponse, *, write: bool) -> dict:
        status = response.status
        if status in (401, 403):
            raise AdapterError("Ghost refuse l'acces. Verifiez la cle Admin et ses droits.",
                               code="auth")
        if status == 404:
            raise AdapterError("Ghost : article introuvable.", code="not_found")
        if status == 422:
            raise AdapterError(
                "Ghost a refuse l'operation (updated_at manquant ou obsolete).",
                code="conflict")
        if status == 429:
            raise AdapterError("Ghost : trop de requetes.", code="limit")
        if status == 400:
            raise AdapterError("Ghost a refuse la valeur ; rien n'a ete enregistre.",
                               code="refused")
        if 300 <= status < 400:
            raise AdapterError("Ghost redirige la requete ; verifiez l'URL de base.",
                               code="refused", uncertain=write)
        if status >= 500:
            raise AdapterError("Ghost : erreur serveur.", code="server", uncertain=write)
        if status < 200 or status >= 300:
            raise AdapterError("Reponse Ghost inattendue (%s)." % status,
                               code="unreadable", uncertain=write)
        data = response.json()
        if not isinstance(data, dict):
            raise AdapterError("Reponse Ghost non lisible.", code="unreadable",
                               uncertain=write)
        return data

    @staticmethod
    def _first_post(data: dict) -> RemoteArticle:
        posts = data.get("posts") if isinstance(data, dict) else None
        if not isinstance(posts, list) or len(posts) != 1 or not isinstance(posts[0], dict):
            raise AdapterError("Ghost n'a pas renvoye un unique article.", code="unreadable")
        post = posts[0]
        remote_id = str(post.get("id") or "").strip()
        if not remote_id:
            raise AdapterError("Ghost n'a pas renvoye d'identifiant.", code="unreadable")
        raw_status = str(post.get("status") or "unknown")
        if raw_status in {"published", "sent"}:
            status = "published"
        elif raw_status == "scheduled":
            status = "scheduled"
        elif raw_status == "draft":
            status = "draft"
        else:
            status = "unknown"
        url = str(post.get("url") or "")
        revision = str(post.get("updated_at") or "")
        if not revision:
            raise AdapterError("Ghost n'a pas renvoye updated_at.", code="unreadable")
        return RemoteArticle(id=remote_id, status=status, url=url, revision=revision)
